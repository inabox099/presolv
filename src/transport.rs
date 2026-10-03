use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, UdpSocket};
use tokio::task::JoinHandle;
use tokio::time::{timeout_at, Instant};

use crate::demux::Demux;
use crate::error::PresolvError;
use crate::query::Protocol;
use crate::wire::{read_id, write_id, HEADER_LEN};

fn net(e: std::io::Error) -> PresolvError {
    PresolvError::Network(e.to_string())
}

/// Generous UDP socket buffers: presolv is designed for bulk/high-volume resolution (spec §1),
/// where many concurrent queries multiplex over one pooled socket. Default OS buffer sizes
/// (commonly ~208KB) can silently drop datagrams under burst load well within normal
/// operating concurrency; size up front to avoid self-inflicted packet loss.
const UDP_BUF_BYTES: usize = 4 * 1024 * 1024;

fn bind_udp(addr: SocketAddr) -> Result<std::net::UdpSocket, PresolvError> {
    use socket2::{Domain, Protocol as SockProtocol, Socket, Type};
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::DGRAM, Some(SockProtocol::UDP)).map_err(net)?;
    sock.set_nonblocking(true).map_err(net)?;
    let _ = sock.set_recv_buffer_size(UDP_BUF_BYTES);
    let _ = sock.set_send_buffer_size(UDP_BUF_BYTES);
    let bind_addr: SocketAddr = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    }
    .parse()
    .unwrap();
    sock.bind(&bind_addr.into()).map_err(net)?;
    Ok(sock.into())
}

enum Writer {
    Udp(Arc<UdpSocket>),
    Tcp(tokio::sync::Mutex<OwnedWriteHalf>),
}

/// One pooled connection (UDP socket or TCP stream) multiplexing many in-flight queries.
pub struct Conn {
    demux: Arc<Demux>,
    writer: Writer,
    reader: JoinHandle<()>,
    dead: Arc<AtomicBool>,
    last_used: Mutex<Instant>,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

struct CancelGuard<'a> {
    demux: &'a Demux,
    id: u16,
}
impl Drop for CancelGuard<'_> {
    fn drop(&mut self) {
        self.demux.cancel(self.id); // no-op if already completed
    }
}

impl Conn {
    pub async fn connect(addr: SocketAddr, protocol: Protocol) -> Result<Conn, PresolvError> {
        let demux = Arc::new(Demux::new());
        let dead = Arc::new(AtomicBool::new(false));
        let (writer, reader) = match protocol {
            Protocol::Udp => {
                let std_sock = bind_udp(addr)?;
                let sock = UdpSocket::from_std(std_sock).map_err(net)?;
                sock.connect(addr).await.map_err(net)?;
                let sock = Arc::new(sock);
                let task = tokio::spawn(udp_reader(sock.clone(), demux.clone(), dead.clone()));
                (Writer::Udp(sock), task)
            }
            Protocol::Tcp => {
                let stream = TcpStream::connect(addr).await.map_err(net)?;
                let _ = stream.set_nodelay(true);
                let (r, w) = stream.into_split();
                let task = tokio::spawn(tcp_reader(r, demux.clone(), dead.clone()));
                (Writer::Tcp(tokio::sync::Mutex::new(w)), task)
            }
        };
        Ok(Conn {
            demux,
            writer,
            reader,
            dead,
            last_used: Mutex::new(Instant::now()),
        })
    }

    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    pub fn in_flight(&self) -> usize {
        self.demux.in_flight()
    }

    /// Some(last activity) when nothing is in flight.
    pub fn idle_since(&self) -> Option<Instant> {
        if self.in_flight() == 0 {
            Some(*self.last_used.lock().unwrap())
        } else {
            None
        }
    }

    fn touch(&self) {
        *self.last_used.lock().unwrap() = Instant::now();
    }

    pub async fn query(&self, wire: &[u8], deadline: Instant) -> Result<Vec<u8>, PresolvError> {
        if self.is_dead() {
            return Err(PresolvError::Network("connection closed".into()));
        }
        if wire.len() < HEADER_LEN {
            return Err(PresolvError::Protocol(
                "query shorter than DNS header".into(),
            ));
        }
        let original = read_id(wire).expect("len checked");
        let (id, rx) = self.demux.register(original)?;
        let _guard = CancelGuard {
            demux: &self.demux,
            id,
        };
        self.touch();

        let mut out = wire.to_vec();
        write_id(&mut out, id);

        let fut = async {
            match &self.writer {
                Writer::Udp(s) => {
                    s.send(&out).await.map_err(net)?;
                }
                Writer::Tcp(w) => {
                    let mut framed = Vec::with_capacity(out.len() + 2);
                    framed.extend_from_slice(&(out.len() as u16).to_be_bytes());
                    framed.extend_from_slice(&out);
                    w.lock().await.write_all(&framed).await.map_err(net)?;
                }
            }
            match rx.await {
                Ok(r) => r,
                Err(_) => Err(PresolvError::Network("connection closed".into())),
            }
        };
        let res = match timeout_at(deadline, fut).await {
            Ok(r) => r,
            Err(_) => Err(PresolvError::Timeout),
        };
        self.touch();
        res
    }
}

async fn udp_reader(sock: Arc<UdpSocket>, demux: Arc<Demux>, dead: Arc<AtomicBool>) {
    let mut buf = vec![0u8; 65535];
    loop {
        match sock.recv(&mut buf).await {
            Ok(n) => {
                demux.complete(buf[..n].to_vec());
            }
            Err(e) => {
                dead.store(true, Ordering::SeqCst);
                demux.fail_all(PresolvError::Network(e.to_string()));
                return;
            }
        }
    }
}

async fn tcp_reader(mut r: OwnedReadHalf, demux: Arc<Demux>, dead: Arc<AtomicBool>) {
    loop {
        let mut len = [0u8; 2];
        let mut frame = Vec::new();
        let res: std::io::Result<()> = async {
            r.read_exact(&mut len).await?;
            frame = vec![0u8; u16::from_be_bytes(len) as usize];
            r.read_exact(&mut frame).await?;
            Ok(())
        }
        .await;
        match res {
            Ok(()) => {
                demux.complete(frame);
            }
            Err(e) => {
                dead.store(true, Ordering::SeqCst);
                demux.fail_all(PresolvError::Network(e.to_string()));
                return;
            }
        }
    }
}
