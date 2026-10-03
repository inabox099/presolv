use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::OnceCell;
use tokio::time::{sleep_until, timeout_at, Instant};

use crate::error::PresolvError;
use crate::query::Protocol;
use crate::transport::Conn;

type Key = (SocketAddr, Protocol);

struct Slot {
    cell: OnceCell<Arc<Conn>>,
}

/// Global pool: at most `max` connections summed over all (nameserver, protocol) keys (spec §7).
/// Each connection multiplexes many queries, so the cap bounds sockets, not queries.
pub struct Pool {
    max: usize,
    idle_timeout: Duration,
    slots: Mutex<HashMap<Key, Arc<Slot>>>,
}

impl Pool {
    pub fn new(max: usize, idle_timeout: Duration) -> Self {
        Pool {
            max: max.max(1),
            idle_timeout,
            slots: Mutex::new(HashMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.slots.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Existing/new slot for `key`, or None when the pool is full and nothing is evictable.
    fn slot_for(&self, key: Key) -> Option<Arc<Slot>> {
        let mut slots = self.slots.lock().unwrap();
        if let Some(s) = slots.get(&key) {
            if s.cell.get().is_some_and(|c| c.is_dead()) {
                slots.remove(&key);
            } else {
                return Some(s.clone());
            }
        }
        if slots.len() >= self.max {
            let victim = slots
                .iter()
                .filter_map(|(k, s)| Some((*k, s.cell.get()?.idle_since()?)))
                .min_by_key(|(_, t)| *t)
                .map(|(k, _)| k);
            let k = victim?;
            slots.remove(&k);
        }
        let s = Arc::new(Slot {
            cell: OnceCell::new(),
        });
        slots.insert(key, s.clone());
        Some(s)
    }

    fn remove_if_same(&self, key: Key, slot: &Arc<Slot>) {
        let mut slots = self.slots.lock().unwrap();
        if slots.get(&key).is_some_and(|s| Arc::ptr_eq(s, slot)) {
            slots.remove(&key);
        }
    }

    /// Get or open a connection. `deadline` is the end of the current attempt:
    /// waiting for capacity past it => `PoolExhausted`; a connect still pending at it => `Timeout`.
    pub async fn get(
        &self,
        addr: SocketAddr,
        protocol: Protocol,
        deadline: Instant,
    ) -> Result<Arc<Conn>, PresolvError> {
        let key = (addr, protocol);
        let slot = loop {
            if let Some(s) = self.slot_for(key) {
                break s;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(PresolvError::PoolExhausted);
            }
            sleep_until((now + Duration::from_millis(10)).min(deadline)).await;
        };
        let init = slot
            .cell
            .get_or_try_init(|| async { Conn::connect(addr, protocol).await.map(Arc::new) });
        match timeout_at(deadline, init).await {
            Ok(Ok(c)) => Ok(c.clone()),
            Ok(Err(e)) => {
                self.remove_if_same(key, &slot);
                Err(e)
            }
            Err(_) => {
                self.remove_if_same(key, &slot);
                Err(PresolvError::Timeout)
            }
        }
    }

    /// Drop dead connections and ones idle for >= `idle_timeout`. Call periodically.
    pub fn sweep(&self) {
        let now = Instant::now();
        let idle = self.idle_timeout;
        self.slots
            .lock()
            .unwrap()
            .retain(|_, s| match s.cell.get() {
                None => true,
                Some(c) => {
                    !c.is_dead()
                        && !c
                            .idle_since()
                            .is_some_and(|t| now.duration_since(t) >= idle)
                }
            });
    }
}
