use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

struct Bucket {
    tokens: f64,
    last: Instant,
}

/// Global token bucket (spec §7). `rate=None` => unlimited.
pub struct RateLimiter {
    cfg: Option<(f64, f64)>, // (rate per sec, burst)
    bucket: Mutex<Bucket>,
}

impl RateLimiter {
    pub fn new(rate: Option<f64>, burst: Option<u32>) -> Self {
        let cfg = rate.filter(|r| *r > 0.0).map(|r| {
            let b = burst.map(|b| b.max(1) as f64).unwrap_or_else(|| r.ceil().max(1.0));
            (r, b)
        });
        let tokens = cfg.map(|(_, b)| b).unwrap_or(0.0);
        RateLimiter { cfg, bucket: Mutex::new(Bucket { tokens, last: Instant::now() }) }
    }

    pub async fn acquire(&self) {
        let Some((rate, burst)) = self.cfg else { return };
        loop {
            let wait = {
                let mut b = self.bucket.lock().unwrap();
                let now = Instant::now();
                let elapsed = now.duration_since(b.last).as_secs_f64();
                b.tokens = (b.tokens + elapsed * rate).min(burst);
                b.last = now;
                if b.tokens >= 1.0 {
                    b.tokens -= 1.0;
                    return;
                }
                Duration::from_secs_f64((1.0 - b.tokens) / rate)
            }; // guard dropped before awaiting
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[tokio::test(start_paused = true)]
    async fn unlimited_never_waits() {
        let l = RateLimiter::new(None, None);
        let t = Instant::now();
        for _ in 0..1000 {
            l.acquire().await;
        }
        assert_eq!(t.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn burst_then_paced() {
        let l = RateLimiter::new(Some(10.0), Some(2));
        let t = Instant::now();
        l.acquire().await;
        l.acquire().await;
        assert_eq!(t.elapsed(), Duration::ZERO, "burst of 2 is immediate");
        l.acquire().await;
        let e = t.elapsed();
        assert!(e >= Duration::from_millis(100) && e < Duration::from_millis(150), "{e:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn default_burst_is_ceil_of_rate_min_one() {
        let l = RateLimiter::new(Some(3.2), None); // burst = ceil(3.2) = 4
        let t = Instant::now();
        for _ in 0..4 {
            l.acquire().await;
        }
        assert_eq!(t.elapsed(), Duration::ZERO);
        let l = RateLimiter::new(Some(0.2), None); // burst = max(1, ceil(0.2)) = 1
        let t = Instant::now();
        l.acquire().await;
        assert_eq!(t.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn tokens_refill_over_time_up_to_burst() {
        let l = RateLimiter::new(Some(10.0), Some(2));
        l.acquire().await;
        l.acquire().await;
        tokio::time::sleep(Duration::from_secs(10)).await; // would refill 100, capped at 2
        let t = Instant::now();
        l.acquire().await;
        l.acquire().await;
        assert_eq!(t.elapsed(), Duration::ZERO);
        l.acquire().await;
        assert!(t.elapsed() >= Duration::from_millis(100));
    }
}
