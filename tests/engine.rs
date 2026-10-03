mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use _presolv::error::PresolvError;
use _presolv::query::{Protocol, Request};
use _presolv::resolver::{Engine, EngineConfig};
use common::*;
use tokio::time::Instant;

fn req(index: usize, tag: u8, ns: Option<SocketAddr>, protocol: Protocol) -> Request {
    Request { index, wire: query(0x0042, tag), nameserver: ns, protocol }
}

fn fast(ns: Vec<SocketAddr>) -> EngineConfig {
    EngineConfig {
        timeout: Duration::from_millis(150),
        retries: 0,
        blacklist_after: None,
        ..EngineConfig::new(ns)
    }
}

#[tokio::test]
async fn success_returns_response_with_original_id_and_metadata() {
    let m = Mock::spawn(echo_handler()).await;
    let e = Engine::new(fast(vec![m.addr]));
    for proto in [Protocol::Udp, Protocol::Tcp] {
        let o = e.execute(req(7, b'a', None, proto)).await;
        assert_eq!(o.index, 7);
        assert_eq!(o.nameserver, Some(m.addr));
        assert_eq!(o.protocol, proto);
        assert!(o.rtt_ms > 0.0);
        let r = o.result.unwrap();
        assert_eq!(&r[0..2], &[0x00, 0x42]);
    }
}

#[tokio::test]
async fn timeout_is_retried_retries_times_then_reported() {
    let m = Mock::spawn(drop_handler()).await;
    let e = Engine::new(EngineConfig { retries: 2, ..fast(vec![m.addr]) });
    let o = e.execute(req(0, b'a', None, Protocol::Udp)).await;
    assert_eq!(o.result, Err(PresolvError::Timeout));
    assert_eq!(m.received(), 3, "1 attempt + 2 retries");
    // rtt is the FINAL attempt only (~150ms), not the cumulative ~450ms
    assert!(o.rtt_ms >= 100.0 && o.rtt_ms < 300.0, "{}", o.rtt_ms);
}

#[tokio::test]
async fn retry_fails_over_to_next_nameserver() {
    let bad = Mock::spawn(drop_handler()).await;
    let good = Mock::spawn(echo_handler()).await;
    let e = Engine::new(EngineConfig { retries: 1, ..fast(vec![bad.addr, good.addr]) });
    let o = e.execute(req(0, b'a', None, Protocol::Udp)).await; // round-robin base 0 => bad first
    assert!(o.result.is_ok());
    assert_eq!(o.nameserver, Some(good.addr));
    assert_eq!(bad.received(), 1);
    assert_eq!(good.received(), 1);
}

#[tokio::test]
async fn initial_pick_is_round_robin() {
    let a = Mock::spawn(echo_handler()).await;
    let b = Mock::spawn(echo_handler()).await;
    let e = Engine::new(fast(vec![a.addr, b.addr]));
    for i in 0..4 {
        e.execute(req(i, b'a', None, Protocol::Udp)).await.result.unwrap();
    }
    assert_eq!((a.received(), b.received()), (2, 2));
}

#[tokio::test]
async fn pinned_nameserver_is_always_used_even_on_retry() {
    let pinned = Mock::spawn(drop_handler()).await;
    let other = Mock::spawn(echo_handler()).await;
    let e = Engine::new(EngineConfig { retries: 2, ..fast(vec![other.addr]) });
    let o = e.execute(req(0, b'a', Some(pinned.addr), Protocol::Udp)).await;
    assert_eq!(o.result, Err(PresolvError::Timeout));
    assert_eq!(pinned.received(), 3);
    assert_eq!(other.received(), 0);
}

#[tokio::test]
async fn protocol_error_is_not_retried_and_does_not_count_as_lame() {
    let m = Mock::spawn(Arc::new(|q: &[u8]| Reply::Bytes(vec![q[0], q[1], 0x80]))).await;
    let e = Engine::new(EngineConfig {
        retries: 3,
        blacklist_after: Some(1),
        ..fast(vec![m.addr])
    });
    for _ in 0..3 {
        let o = e.execute(req(0, b'a', None, Protocol::Udp)).await;
        assert!(matches!(o.result, Err(PresolvError::Protocol(_))), "{:?}", o.result);
    }
    assert_eq!(m.received(), 3, "one attempt each, never retried, never blacklisted");
}

#[tokio::test]
async fn network_error_is_retried() {
    let dead = closed_tcp_addr();
    let e = Engine::new(EngineConfig { retries: 2, ..fast(vec![dead]) });
    let o = e.execute(req(0, b'a', None, Protocol::Tcp)).await;
    assert!(matches!(o.result, Err(PresolvError::Network(_))));
}

#[tokio::test]
async fn blacklist_then_fail_fast_then_recover() {
    let m = Mock::spawn(drop_handler()).await;
    let e = Engine::new(EngineConfig {
        timeout: Duration::from_millis(100),
        blacklist_after: Some(2),
        blacklist_duration: Duration::from_millis(400),
        ..fast(vec![m.addr])
    });
    for _ in 0..2 {
        let o = e.execute(req(0, b'a', None, Protocol::Udp)).await;
        assert_eq!(o.result, Err(PresolvError::Timeout));
    }
    assert_eq!(m.received(), 2);

    // all defaults blacklisted => fail fast, no network
    let o = e.execute(req(0, b'a', None, Protocol::Udp)).await;
    match o.result {
        Err(PresolvError::Lame { nameservers, retry_in }) => {
            assert_eq!(nameservers, vec![m.addr]);
            assert!(retry_in <= Duration::from_millis(400));
        }
        other => panic!("expected Lame, got {other:?}"),
    }
    assert_eq!(m.received(), 2, "no packet sent while blacklisted");

    // pinned + blacklisted => also fail fast
    let o = e.execute(req(0, b'a', Some(m.addr), Protocol::Udp)).await;
    assert!(matches!(o.result, Err(PresolvError::Lame { .. })));
    assert_eq!(m.received(), 2);

    tokio::time::sleep(Duration::from_millis(450)).await;
    let o = e.execute(req(0, b'a', None, Protocol::Udp)).await;
    assert_eq!(o.result, Err(PresolvError::Timeout), "eligible again after expiry");
    assert_eq!(m.received(), 3);
}

#[tokio::test]
async fn blacklisted_server_is_skipped_when_others_are_healthy() {
    let bad = Mock::spawn(drop_handler()).await;
    let good = Mock::spawn(echo_handler()).await;
    let e = Engine::new(EngineConfig {
        blacklist_after: Some(1),
        blacklist_duration: Duration::from_secs(30),
        ..fast(vec![bad.addr, good.addr])
    });
    // first query goes to `bad` (round-robin base 0), fails, blacklists it
    let _ = e.execute(req(0, b'a', None, Protocol::Udp)).await;
    for i in 1..6 {
        let o = e.execute(req(i, b'a', None, Protocol::Udp)).await;
        assert_eq!(o.nameserver, Some(good.addr));
        assert!(o.result.is_ok());
    }
    assert_eq!(bad.received(), 1);
}

#[tokio::test]
async fn every_attempt_consumes_a_rate_limit_token() {
    let m = Mock::spawn(drop_handler()).await;
    let e = Engine::new(EngineConfig {
        timeout: Duration::from_millis(20),
        retries: 2,
        rate_limit: Some(10.0), // 100ms per token
        burst: Some(1),
        ..fast(vec![m.addr])
    });
    let t = Instant::now();
    let _ = e.execute(req(0, b'a', None, Protocol::Udp)).await;
    assert!(t.elapsed() >= Duration::from_millis(190), "{:?}", t.elapsed());
    assert_eq!(m.received(), 3);
}

#[tokio::test]
async fn rate_limit_wait_does_not_count_against_timeout() {
    let m = Mock::spawn(echo_handler()).await;
    let e = Arc::new(Engine::new(EngineConfig {
        timeout: Duration::from_millis(50),
        rate_limit: Some(10.0),
        burst: Some(1),
        ..fast(vec![m.addr])
    }));
    let hs: Vec<_> = (0..3)
        .map(|i| {
            let e = e.clone();
            tokio::spawn(async move { e.execute(req(i, b'a', None, Protocol::Udp)).await })
        })
        .collect();
    for h in hs {
        assert!(h.await.unwrap().result.is_ok(), "limiter wait must not cause Timeout");
    }
}

#[tokio::test]
async fn no_nameservers_configured_is_network_error() {
    let e = Engine::new(fast(vec![]));
    let o = e.execute(req(0, b'a', None, Protocol::Udp)).await;
    assert!(matches!(o.result, Err(PresolvError::Network(_))));
}

#[tokio::test]
async fn pool_exhaustion_is_reported_and_not_retried() {
    let m1 = Mock::spawn(tagged_slow_handler()).await;
    let m2 = Mock::spawn(echo_handler()).await;
    let e = Arc::new(Engine::new(EngineConfig {
        pool_size: 1,
        timeout: Duration::from_millis(100),
        retries: 3,
        ..fast(vec![m1.addr])
    }));
    let e2 = e.clone();
    let busy = tokio::spawn(async move { e2.execute(req(0, b's', None, Protocol::Udp)).await });
    tokio::time::sleep(Duration::from_millis(30)).await; // slot held, query in flight
    let o = e.execute(req(1, b'a', Some(m2.addr), Protocol::Udp)).await;
    assert_eq!(o.result, Err(PresolvError::PoolExhausted));
    assert_eq!(m2.received(), 0);
    let _ = busy.await;
}
