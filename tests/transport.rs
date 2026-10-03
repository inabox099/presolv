mod common;

use std::sync::Arc;
use std::time::Duration;

use _presolv::error::PresolvError;
use _presolv::query::Protocol;
use _presolv::transport::Conn;
use common::*;
use tokio::time::Instant;

fn deadline(ms: u64) -> Instant {
    Instant::now() + Duration::from_millis(ms)
}

#[tokio::test]
async fn query_roundtrip_restores_original_id() {
    let m = Mock::spawn(echo_handler()).await;
    for proto in [Protocol::Udp, Protocol::Tcp] {
        let c = Conn::connect(m.addr, proto).await.unwrap();
        let r = c.query(&query(0x1234, b'a'), deadline(1000)).await.unwrap();
        assert_eq!(&r[0..2], &[0x12, 0x34], "{proto:?}");
        assert_eq!(*r.last().unwrap(), b'a');
    }
}

#[tokio::test]
async fn concurrent_queries_with_same_id_are_demuxed() {
    // 's' reply is delayed 300ms, so replies arrive out of order; same caller ID 0 for all.
    let m = Mock::spawn(tagged_slow_handler()).await;
    for proto in [Protocol::Udp, Protocol::Tcp] {
        let c = Arc::new(Conn::connect(m.addr, proto).await.unwrap());
        let d = deadline(2000);
        let (qs, qa, qb) = (query(0, b's'), query(0, b'a'), query(0, b'b'));
        let (a, b, x) = tokio::join!(c.query(&qs, d), c.query(&qa, d), c.query(&qb, d),);
        for (r, tag) in [(a, b's'), (b, b'a'), (x, b'b')] {
            let r = r.unwrap();
            assert_eq!(&r[0..2], &[0, 0], "{proto:?}");
            assert_eq!(
                *r.last().unwrap(),
                tag,
                "{proto:?}: response matched wrong query"
            );
        }
    }
}

#[tokio::test]
async fn no_reply_times_out() {
    let m = Mock::spawn(drop_handler()).await;
    for proto in [Protocol::Udp, Protocol::Tcp] {
        let c = Conn::connect(m.addr, proto).await.unwrap();
        let r = c.query(&query(1, b'a'), deadline(100)).await;
        assert_eq!(r, Err(PresolvError::Timeout), "{proto:?}");
        assert_eq!(c.in_flight(), 0, "timed-out query must be deregistered");
    }
}

#[tokio::test]
async fn reply_with_unknown_id_is_discarded_then_times_out() {
    let m = Mock::spawn(Arc::new(|q| {
        let mut r = echo(q);
        r[0] ^= 0xff;
        r[1] ^= 0xff;
        Reply::Bytes(r)
    }))
    .await;
    let c = Conn::connect(m.addr, Protocol::Udp).await.unwrap();
    // (1-in-65536 chance the flipped id collides; ids are random so ignore.)
    assert_eq!(
        c.query(&query(5, b'a'), deadline(150)).await,
        Err(PresolvError::Timeout)
    );
}

#[tokio::test]
async fn reply_shorter_than_header_is_protocol_error() {
    let m = Mock::spawn(Arc::new(|q| Reply::Bytes(vec![q[0], q[1], 0x80]))).await;
    for proto in [Protocol::Udp, Protocol::Tcp] {
        let c = Conn::connect(m.addr, proto).await.unwrap();
        let r = c.query(&query(1, b'a'), deadline(1000)).await;
        assert!(
            matches!(r, Err(PresolvError::Protocol(_))),
            "{proto:?}: {r:?}"
        );
    }
}

#[tokio::test]
async fn tcp_connect_refused_is_network_error() {
    let r = Conn::connect(closed_tcp_addr(), Protocol::Tcp).await;
    assert!(matches!(r, Err(PresolvError::Network(_))));
}

#[tokio::test]
async fn tcp_eof_fails_pending_and_marks_dead() {
    let m = Mock::spawn(Arc::new(|_| Reply::Close)).await;
    let c = Conn::connect(m.addr, Protocol::Tcp).await.unwrap();
    let r = c.query(&query(1, b'a'), deadline(1000)).await;
    assert!(matches!(r, Err(PresolvError::Network(_))), "{r:?}");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(c.is_dead());
    // further queries fail fast instead of hanging
    let r = c.query(&query(2, b'a'), deadline(1000)).await;
    assert!(matches!(r, Err(PresolvError::Network(_))));
}

#[tokio::test]
async fn idle_since_is_none_while_in_flight() {
    let m = Mock::spawn(tagged_slow_handler()).await;
    let c = Arc::new(Conn::connect(m.addr, Protocol::Udp).await.unwrap());
    assert!(c.idle_since().is_some());
    let c2 = c.clone();
    let h = tokio::spawn(async move { c2.query(&query(1, b's'), deadline(2000)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(c.idle_since().is_none());
    h.await.unwrap().unwrap();
    assert!(c.idle_since().is_some());
}
