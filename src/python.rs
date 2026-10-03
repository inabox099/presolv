use std::borrow::Cow;
use std::net::{IpAddr, SocketAddr};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::runtime::{Builder, Handle, Runtime};
use tokio_util::sync::CancellationToken;

use crate::error::PresolvError;
use crate::query::{Outcome, Protocol, Request};
use crate::resolver::{Engine, EngineConfig};

fn parse_addr(ip: &str, port: u16) -> PyResult<SocketAddr> {
    let ip: IpAddr = ip
        .parse()
        .map_err(|_| PyValueError::new_err(format!("invalid IP literal: {ip:?}")))?;
    Ok(SocketAddr::new(ip, port))
}

/// "1.2.3.4" for port 53, else "1.2.3.4:5353" / "[::1]:5353".
fn fmt_addr(a: SocketAddr) -> String {
    match (a.port(), a.ip()) {
        (53, ip) => ip.to_string(),
        (p, IpAddr::V4(ip)) => format!("{ip}:{p}"),
        (p, IpAddr::V6(ip)) => format!("[{ip}]:{p}"),
    }
}

#[pyclass(frozen)]
pub struct NativeEngine {
    runtime: Mutex<Option<Runtime>>,
    handle: Handle,
    engine: Arc<Engine>,
    token: CancellationToken,
}

#[pymethods]
impl NativeEngine {
    #[new]
    #[pyo3(signature = (nameservers, timeout, retries, rate_limit, burst, pool_size, idle_timeout, blacklist_after, blacklist_duration, worker_threads))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        nameservers: Vec<(String, u16)>,
        timeout: f64,
        retries: u32,
        rate_limit: Option<f64>,
        burst: Option<u32>,
        pool_size: usize,
        idle_timeout: f64,
        blacklist_after: Option<u32>,
        blacklist_duration: f64,
        worker_threads: Option<usize>,
    ) -> PyResult<Self> {
        let mut addrs = Vec::new();
        for (ip, port) in nameservers {
            addrs.push(parse_addr(&ip, port)?);
        }
        let mut b = Builder::new_multi_thread();
        b.enable_all().thread_name("presolv-worker");
        if let Some(n) = worker_threads {
            b.worker_threads(n.max(1));
        }
        let runtime = b
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let engine = Arc::new(Engine::new(EngineConfig {
            nameservers: addrs,
            timeout: Duration::from_secs_f64(timeout.max(0.001)),
            retries,
            rate_limit,
            burst,
            pool_size,
            idle_timeout: Duration::from_secs_f64(idle_timeout.max(0.0)),
            blacklist_after,
            blacklist_duration: Duration::from_secs_f64(blacklist_duration.max(0.0)),
        }));

        let token = CancellationToken::new();
        let (weak, tok) = (Arc::downgrade(&engine), token.clone());
        let period = Duration::from_secs_f64((idle_timeout / 2.0).clamp(1.0, 30.0));
        runtime.spawn(async move {
            loop {
                tokio::select! {
                    _ = tok.cancelled() => break,
                    _ = tokio::time::sleep(period) => {}
                }
                match weak.upgrade() {
                    Some(e) => e.sweep(),
                    None => break,
                }
            }
        });

        let handle = runtime.handle().clone();
        Ok(NativeEngine {
            runtime: Mutex::new(Some(runtime)),
            handle,
            engine,
            token,
        })
    }

    fn session(&self) -> Session {
        let (tx, rx) = mpsc::channel();
        Session {
            handle: self.handle.clone(),
            engine: self.engine.clone(),
            token: self.token.child_token(),
            tx,
            rx: Mutex::new(rx),
        }
    }

    fn close(&self) {
        self.token.cancel();
        if let Some(rt) = self.runtime.lock().unwrap().take() {
            rt.shutdown_background();
        }
    }
}

#[pyclass(frozen)]
pub struct Session {
    handle: Handle,
    engine: Arc<Engine>,
    token: CancellationToken,
    tx: mpsc::Sender<Outcome>,
    rx: Mutex<mpsc::Receiver<Outcome>>,
}

#[pymethods]
impl Session {
    fn submit(
        &self,
        index: usize,
        wire: Cow<'_, [u8]>,
        nameserver: Option<(String, u16)>,
        protocol: &str,
    ) -> PyResult<()> {
        let protocol = Protocol::parse(protocol)
            .ok_or_else(|| PyValueError::new_err(format!("unsupported protocol: {protocol:?}")))?;
        let nameserver = match nameserver {
            Some((ip, port)) => Some(parse_addr(&ip, port)?),
            None => None,
        };
        let req = Request {
            index,
            wire: wire.into_owned(),
            nameserver,
            protocol,
        };
        let (engine, tx, token) = (self.engine.clone(), self.tx.clone(), self.token.clone());
        self.handle.spawn(async move {
            tokio::select! {
                _ = token.cancelled() => {}
                out = engine.execute(req) => { let _ = tx.send(out); }
            }
        });
        Ok(())
    }

    /// Block (GIL released) up to `timeout` seconds. Returns the raw tuple or None.
    fn next<'py>(&self, py: Python<'py>, timeout: f64) -> PyResult<Option<Bound<'py, PyAny>>> {
        let wait = Duration::from_secs_f64(timeout.max(0.0));
        let got = py.detach(|| self.rx.lock().unwrap().recv_timeout(wait));
        let Ok(o) = got else { return Ok(None) };

        let (wire, kind, message, lame_ns, retry_in): (
            Option<Bound<'py, PyBytes>>,
            Option<&'static str>,
            String,
            Vec<String>,
            f64,
        ) = match &o.result {
            Ok(b) => (Some(PyBytes::new(py, b)), None, String::new(), vec![], 0.0),
            Err(e) => {
                let (ns, r) = match e {
                    PresolvError::Lame {
                        nameservers,
                        retry_in,
                    } => (
                        nameservers.iter().map(|a| fmt_addr(*a)).collect(),
                        retry_in.as_secs_f64(),
                    ),
                    _ => (vec![], 0.0),
                };
                (None, Some(e.kind()), e.to_string(), ns, r)
            }
        };
        let tup = (
            o.index,
            wire,
            kind,
            message,
            o.nameserver.map(fmt_addr).unwrap_or_default(),
            o.protocol.as_str(),
            o.rtt_ms,
            lame_ns,
            retry_in,
        );
        Ok(Some(tup.into_pyobject(py)?.into_any()))
    }

    fn close(&self) {
        self.token.cancel();
    }
}

#[pyfunction]
fn native_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Read once by the Python shim at Resolver construction (spec §9).
#[pyfunction]
fn system_nameservers() -> PyResult<Vec<(String, u16)>> {
    crate::system::system_nameservers()
        .map(|v| {
            v.into_iter()
                .map(|a| (a.ip().to_string(), a.port()))
                .collect()
        })
        .map_err(PyRuntimeError::new_err)
}

#[pymodule]
mod _presolv {
    #[pymodule_export]
    use super::{native_version, system_nameservers, NativeEngine, Session};
}
