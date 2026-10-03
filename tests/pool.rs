mod common;

use std::sync::Arc;
use std::time::Duration;

use _presolv::error::PresolvError;
use _presolv::pool::Pool;
use _presolv::query::Protocol::{Tcp, Udp};
use common::*;
use tokio::time::Instant;

fn dl(ms: u64) -> Instant {
    Instant::now() + Duration::from_millis(ms)
}

#[tokio::test]
async fn reuses_connection_for_same_key() {
    let m = Mock::spawn(echo_handler()).await;
    let p = Pool::new(10, Duration::from_secs(60));
    let a = p.get(m.addr, Udp, dl(1000)).await.unwrap();
    let b = p.get(m.addr, Udp, dl(1000)).await.unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(p.len(), 1);
}

#[tokio::test]
async fn different_protocol_is_different_key() {
    let m = Mock::spawn(echo_handler()).await;
    let p = Pool::new(10, Duration::from_secs(60));
    let a = p.get(m.addr, Udp, dl(1000)).await.unwrap();
    let b = p.get(m.addr, Tcp, dl(1000)).await.unwrap();
    assert!(!Arc::ptr_eq(&a, &b));
    assert_eq!(p.len(), 2);
}

#[tokio::test]
async fn concurrent_gets_for_same_key_connect_once() {
    let m = Mock::spawn(echo_handler()).await;
    let p = Arc::new(Pool::new(10, Duration::from_secs(60)));
    let hs: Vec<_> = (0..20)
        .map(|_| {
            let p = p.clone();
            let a = m.addr;
            tokio::spawn(async move { p.get(a, Tcp, dl(1000)).await.unwrap() })
        })
        .collect();
    let conns: Vec<_> = futures_join(hs).await;
    assert!(conns.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])));
    assert_eq!(p.len(), 1);
}

async fn futures_join<T>(hs: Vec<tokio::task::JoinHandle<T>>) -> Vec<T> {
    let mut out = vec![];
    for h in hs {
        out.push(h.await.unwrap());
    }
    out
}

#[tokio::test]
async fn evicts_lru_idle_connection_when_full() {
    let m = Mock::spawn(echo_handler()).await;
    let p = Pool::new(1, Duration::from_secs(60));
    let a = p.get(m.addr, Udp, dl(1000)).await.unwrap();
    drop(a);
    let _b = p.get(m.addr, Tcp, dl(1000)).await.unwrap(); // evicts the idle UDP conn
    assert_eq!(p.len(), 1);
}

#[tokio::test]
async fn full_and_all_busy_gives_pool_exhausted_within_deadline() {
    let m = Mock::spawn(tagged_slow_handler()).await;
    let p = Arc::new(Pool::new(1, Duration::from_secs(60)));
    let c = p.get(m.addr, Udp, dl(1000)).await.unwrap();
    let busy = tokio::spawn(async move { c.query(&query(1, b's'), dl(2000)).await });
    tokio::time::sleep(Duration::from_millis(50)).await; // query now in flight
    let t = Instant::now();
    let r = p.get(m.addr, Tcp, dl(100)).await;
    assert_eq!(r.err(), Some(PresolvError::PoolExhausted));
    assert!(t.elapsed() < Duration::from_millis(400));
    busy.await.unwrap().unwrap();
}

#[tokio::test]
async fn waiter_gets_slot_when_connection_becomes_idle() {
    let m = Mock::spawn(tagged_slow_handler()).await;
    let p = Arc::new(Pool::new(1, Duration::from_secs(60)));
    let c = p.get(m.addr, Udp, dl(1000)).await.unwrap();
    let busy = tokio::spawn(async move { c.query(&query(1, b's'), dl(2000)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    // busy finishes after ~300ms; waiting get (1s budget) must then succeed by evicting it
    let r = p.get(m.addr, Tcp, dl(1000)).await;
    assert!(r.is_ok(), "{:?}", r.err());
    busy.await.unwrap().unwrap();
}

#[tokio::test]
async fn sweep_evicts_connections_idle_past_timeout() {
    let m = Mock::spawn(echo_handler()).await;
    let p = Pool::new(10, Duration::from_millis(50));
    let a = p.get(m.addr, Udp, dl(1000)).await.unwrap();
    drop(a);
    p.sweep();
    assert_eq!(p.len(), 1, "not idle long enough yet");
    tokio::time::sleep(Duration::from_millis(100)).await;
    p.sweep();
    assert_eq!(p.len(), 0);
}

#[tokio::test]
async fn connect_failure_is_not_cached() {
    let p = Pool::new(10, Duration::from_secs(60));
    let dead = closed_tcp_addr();
    let r = p.get(dead, Tcp, dl(1000)).await;
    assert!(matches!(r, Err(PresolvError::Network(_))));
    assert_eq!(p.len(), 0);
}
