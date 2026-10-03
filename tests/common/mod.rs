#![allow(dead_code)]
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

pub enum Reply {
    None,
    Bytes(Vec<u8>),
    After(Duration, Vec<u8>),
    /// TCP: close the connection. UDP: same as None.
    Close,
}
pub type Handler = Arc<dyn Fn(&[u8]) -> Reply + Send + Sync>;

pub struct Mock {
    pub addr: SocketAddr,
    pub count: Arc<AtomicUsize>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// 12-byte header with `id`, QDCOUNT=1, followed by one `tag` byte.
pub fn query(id: u16, tag: u8) -> Vec<u8> {
    let mut v = vec![0u8; 12];
    v[0] = (id >> 8) as u8;
    v[1] = id as u8;
    v[5] = 1;
    v.push(tag);
    v
}

/// Same bytes, QR bit set.
pub fn echo(q: &[u8]) -> Vec<u8> {
    let mut r = q.to_vec();
    r[2] |= 0x80;
    r
}

pub fn echo_handler() -> Handler {
    Arc::new(|q| Reply::Bytes(echo(q)))
}
pub fn drop_handler() -> Handler {
    Arc::new(|_| Reply::None)
}
/// Tag b's' => answer after 300ms; anything else immediately.
pub fn tagged_slow_handler() -> Handler {
    Arc::new(|q| {
        if *q.last().unwrap() == b's' {
            Reply::After(Duration::from_millis(300), echo(q))
        } else {
            Reply::Bytes(echo(q))
        }
    })
}

async fn send_frame(w: Arc<tokio::sync::Mutex<tokio::net::tcp::OwnedWriteHalf>>, b: Vec<u8>) {
    let mut f = (b.len() as u16).to_be_bytes().to_vec();
    f.extend_from_slice(&b);
    let _ = w.lock().await.write_all(&f).await;
}

/// An address nothing listens on (TCP connect => ECONNREFUSED).
pub fn closed_tcp_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

impl Mock {
    pub async fn spawn(handler: Handler) -> Mock {
        for _ in 0..50 {
            let std_udp = {
                use socket2::{Domain, Protocol as SockProtocol, Socket, Type};
                let s = Socket::new(Domain::IPV4, Type::DGRAM, Some(SockProtocol::UDP)).unwrap();
                s.set_nonblocking(true).unwrap();
                let _ = s.set_recv_buffer_size(4 * 1024 * 1024);
                let _ = s.set_send_buffer_size(4 * 1024 * 1024);
                let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
                s.bind(&addr.into()).unwrap();
                std::net::UdpSocket::from(s)
            };
            let udp = UdpSocket::from_std(std_udp).unwrap();
            let addr = udp.local_addr().unwrap();
            let Ok(tcp) = TcpListener::bind(addr).await else { continue };
            let count = Arc::new(AtomicUsize::new(0));
            let udp = Arc::new(udp);

            let (h, c, u) = (handler.clone(), count.clone(), udp.clone());
            let udp_task = tokio::spawn(async move {
                let mut buf = vec![0u8; 65535];
                loop {
                    let Ok((n, peer)) = u.recv_from(&mut buf).await else { return };
                    c.fetch_add(1, Ordering::SeqCst);
                    match h(&buf[..n]) {
                        Reply::None | Reply::Close => {}
                        Reply::Bytes(b) => {
                            let _ = u.send_to(&b, peer).await;
                        }
                        Reply::After(d, b) => {
                            let u2 = u.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(d).await;
                                let _ = u2.send_to(&b, peer).await;
                            });
                        }
                    }
                }
            });

            let (h, c) = (handler.clone(), count.clone());
            let tcp_task = tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = tcp.accept().await else { return };
                    let (h, c) = (h.clone(), c.clone());
                    tokio::spawn(async move {
                        let (mut r, w) = stream.into_split();
                        let w = Arc::new(tokio::sync::Mutex::new(w));
                        loop {
                            let mut len = [0u8; 2];
                            if r.read_exact(&mut len).await.is_err() {
                                return;
                            }
                            let mut buf = vec![0u8; u16::from_be_bytes(len) as usize];
                            if r.read_exact(&mut buf).await.is_err() {
                                return;
                            }
                            c.fetch_add(1, Ordering::SeqCst);
                            match h(&buf) {
                                Reply::None => {}
                                Reply::Close => return,
                                Reply::Bytes(b) => send_frame(w.clone(), b).await,
                                Reply::After(d, b) => {
                                    let w2 = w.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(d).await;
                                        send_frame(w2, b).await;
                                    });
                                }
                            }
                        }
                    });
                }
            });

            return Mock { addr, count, tasks: vec![udp_task, tcp_task] };
        }
        panic!("could not bind UDP+TCP on the same port");
    }

    pub fn received(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}
