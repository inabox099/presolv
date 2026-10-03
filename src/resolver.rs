use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use crate::error::PresolvError;
use crate::health::Health;
use crate::pool::Pool;
use crate::query::{Outcome, Protocol, Request};
use crate::rate_limiter::RateLimiter;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub nameservers: Vec<SocketAddr>,
    pub timeout: Duration,
    pub retries: u32,
    pub rate_limit: Option<f64>,
    pub burst: Option<u32>,
    pub pool_size: usize,
    pub idle_timeout: Duration,
    pub blacklist_after: Option<u32>,
    pub blacklist_duration: Duration,
}

impl EngineConfig {
    /// Spec §5.1 defaults.
    pub fn new(nameservers: Vec<SocketAddr>) -> Self {
        EngineConfig {
            nameservers,
            timeout: Duration::from_secs(5),
            retries: 2,
            rate_limit: None,
            burst: None,
            pool_size: 100,
            idle_timeout: Duration::from_secs(60),
            blacklist_after: Some(5),
            blacklist_duration: Duration::from_secs(30),
        }
    }
}

enum Pick {
    Server(SocketAddr),
    Lame { nameservers: Vec<SocketAddr>, retry_in: Duration },
    NoServers,
}

pub struct Engine {
    cfg: EngineConfig,
    limiter: RateLimiter,
    health: Health,
    pool: Pool,
    rr: AtomicUsize,
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Engine {
        Engine {
            limiter: RateLimiter::new(cfg.rate_limit, cfg.burst),
            health: Health::new(cfg.blacklist_after, cfg.blacklist_duration),
            pool: Pool::new(cfg.pool_size, cfg.idle_timeout),
            rr: AtomicUsize::new(0),
            cfg,
        }
    }

    pub fn sweep(&self) {
        self.pool.sweep();
    }

    fn pick(&self, req: &Request, base: usize, attempt: u32) -> Pick {
        if let Some(a) = req.nameserver {
            return match self.health.blacklisted_for(a) {
                Some(d) => Pick::Lame { nameservers: vec![a], retry_in: d },
                None => Pick::Server(a),
            };
        }
        let ns = &self.cfg.nameservers;
        let n = ns.len();
        if n == 0 {
            return Pick::NoServers;
        }
        let mut earliest: Option<Duration> = None;
        for i in 0..n {
            let a = ns[(base + attempt as usize + i) % n];
            match self.health.blacklisted_for(a) {
                None => return Pick::Server(a),
                Some(d) => earliest = Some(earliest.map_or(d, |e| e.min(d))),
            }
        }
        Pick::Lame { nameservers: ns.clone(), retry_in: earliest.unwrap_or_default() }
    }

    async fn attempt(
        &self,
        addr: SocketAddr,
        protocol: Protocol,
        wire: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, PresolvError> {
        let conn = self.pool.get(addr, protocol, deadline).await?;
        conn.query(wire, deadline).await
    }

    pub async fn execute(&self, req: Request) -> Outcome {
        let base = self.rr.fetch_add(1, Ordering::Relaxed);
        let mut last: Option<Outcome> = None;

        for attempt in 0..=self.cfg.retries {
            let target = match self.pick(&req, base, attempt) {
                Pick::Server(a) => a,
                Pick::Lame { nameservers, retry_in } => {
                    // Later attempts: report the last real failure instead of masking it.
                    return last.unwrap_or_else(|| Outcome {
                        index: req.index,
                        nameserver: nameservers.first().copied(),
                        protocol: req.protocol,
                        rtt_ms: 0.0,
                        result: Err(PresolvError::Lame { nameservers, retry_in }),
                    });
                }
                Pick::NoServers => {
                    return last.unwrap_or_else(|| Outcome {
                        index: req.index,
                        nameserver: None,
                        protocol: req.protocol,
                        rtt_ms: 0.0,
                        result: Err(PresolvError::Network("no nameservers configured".into())),
                    });
                }
            };

            self.limiter.acquire().await; // not counted against `timeout`
            let t0 = Instant::now();
            let deadline = t0 + self.cfg.timeout;
            let result = self.attempt(target, req.protocol, &req.wire, deadline).await;
            let rtt_ms = t0.elapsed().as_secs_f64() * 1000.0;

            match &result {
                Ok(_) => self.health.record_success(target),
                Err(PresolvError::Timeout) | Err(PresolvError::Network(_)) => {
                    self.health.record_failure(target)
                }
                Err(_) => {} // Protocol / PoolExhausted: neither success nor lame evidence
            }
            let retryable =
                matches!(result, Err(PresolvError::Timeout) | Err(PresolvError::Network(_)));
            last = Some(Outcome {
                index: req.index,
                result,
                nameserver: Some(target),
                protocol: req.protocol,
                rtt_ms,
            });
            if !retryable {
                break;
            }
        }
        last.expect("loop runs at least once")
    }
}
