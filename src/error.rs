use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum PresolvError {
    Timeout,
    Network(String),
    Protocol(String),
    PoolExhausted,
    Lame { nameservers: Vec<SocketAddr>, retry_in: Duration },
}

impl PresolvError {
    pub fn kind(&self) -> &'static str {
        match self {
            PresolvError::Timeout => "timeout",
            PresolvError::Network(_) => "network",
            PresolvError::Protocol(_) => "protocol",
            PresolvError::PoolExhausted => "pool_exhausted",
            PresolvError::Lame { .. } => "lame",
        }
    }
}

impl fmt::Display for PresolvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PresolvError::Timeout => write!(f, "no response within timeout"),
            PresolvError::Network(m) => write!(f, "network error: {m}"),
            PresolvError::Protocol(m) => write!(f, "protocol error: {m}"),
            PresolvError::PoolExhausted => write!(f, "connection pool exhausted"),
            PresolvError::Lame { nameservers, retry_in } => write!(
                f,
                "nameserver(s) blacklisted as lame: {nameservers:?} (retry in {:.1}s)",
                retry_in.as_secs_f64()
            ),
        }
    }
}

impl std::error::Error for PresolvError {}
