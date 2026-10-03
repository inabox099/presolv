use std::net::SocketAddr;

use crate::error::PresolvError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Protocol {
    Udp,
    Tcp,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::Udp => "udp",
            Protocol::Tcp => "tcp",
        }
    }
    pub fn parse(s: &str) -> Option<Protocol> {
        match s {
            "udp" => Some(Protocol::Udp),
            "tcp" => Some(Protocol::Tcp),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Request {
    pub index: usize,
    pub wire: Vec<u8>,
    pub nameserver: Option<SocketAddr>,
    pub protocol: Protocol,
}

#[derive(Debug)]
pub struct Outcome {
    pub index: usize,
    pub result: Result<Vec<u8>, PresolvError>,
    pub nameserver: Option<SocketAddr>,
    pub protocol: Protocol,
    pub rtt_ms: f64,
}
