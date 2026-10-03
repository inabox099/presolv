use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

#[derive(Default)]
struct Entry {
    failures: u32,
    until: Option<Instant>,
}

/// Per-nameserver lame-server tracking (spec §9). Keyed by `ip:port`.
pub struct Health {
    threshold: u32, // 0 = disabled
    duration: Duration,
    map: Mutex<HashMap<SocketAddr, Entry>>,
}

impl Health {
    pub fn new(blacklist_after: Option<u32>, duration: Duration) -> Self {
        Health {
            threshold: blacklist_after.unwrap_or(0),
            duration,
            map: Mutex::new(HashMap::new()),
        }
    }

    pub fn record_success(&self, a: SocketAddr) {
        if self.threshold == 0 {
            return;
        }
        if let Some(e) = self.map.lock().unwrap().get_mut(&a) {
            e.failures = 0;
        }
    }

    pub fn record_failure(&self, a: SocketAddr) {
        if self.threshold == 0 {
            return;
        }
        let mut map = self.map.lock().unwrap();
        let e = map.entry(a).or_default();
        e.failures += 1;
        if e.failures >= self.threshold {
            e.until = Some(Instant::now() + self.duration);
            e.failures = 0;
        }
    }

    /// Remaining blacklist time, or None if eligible. Expired entries are cleared.
    pub fn blacklisted_for(&self, a: SocketAddr) -> Option<Duration> {
        if self.threshold == 0 {
            return None;
        }
        let mut map = self.map.lock().unwrap();
        let e = map.get_mut(&a)?;
        let until = e.until?;
        let now = Instant::now();
        if now >= until {
            e.until = None;
            e.failures = 0;
            return None;
        }
        Some(until - now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(n: u8) -> SocketAddr {
        SocketAddr::new([10, 0, 0, n].into(), 53)
    }

    #[tokio::test(start_paused = true)]
    async fn blacklists_after_n_consecutive_failures() {
        let h = Health::new(Some(3), Duration::from_secs(30));
        h.record_failure(a(1));
        h.record_failure(a(1));
        assert!(h.blacklisted_for(a(1)).is_none());
        h.record_failure(a(1));
        let left = h.blacklisted_for(a(1)).expect("blacklisted");
        assert!(left <= Duration::from_secs(30) && left > Duration::from_secs(29));
    }

    #[tokio::test(start_paused = true)]
    async fn success_resets_counter() {
        let h = Health::new(Some(2), Duration::from_secs(30));
        h.record_failure(a(1));
        h.record_success(a(1));
        h.record_failure(a(1));
        assert!(h.blacklisted_for(a(1)).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn blacklist_expires_and_counter_restarts() {
        let h = Health::new(Some(2), Duration::from_secs(30));
        h.record_failure(a(1));
        h.record_failure(a(1));
        assert!(h.blacklisted_for(a(1)).is_some());
        tokio::time::sleep(Duration::from_secs(31)).await;
        assert!(h.blacklisted_for(a(1)).is_none());
        h.record_failure(a(1)); // counter was reset: 1 < 2
        assert!(h.blacklisted_for(a(1)).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn servers_are_independent() {
        let h = Health::new(Some(1), Duration::from_secs(30));
        h.record_failure(a(1));
        assert!(h.blacklisted_for(a(1)).is_some());
        assert!(h.blacklisted_for(a(2)).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_when_none_or_zero() {
        for after in [None, Some(0)] {
            let h = Health::new(after, Duration::from_secs(30));
            for _ in 0..100 {
                h.record_failure(a(1));
            }
            assert!(h.blacklisted_for(a(1)).is_none());
        }
    }
}
