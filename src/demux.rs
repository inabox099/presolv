use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::error::PresolvError;
use crate::wire::{read_id, write_id, HEADER_LEN};

type Reply = Result<Vec<u8>, PresolvError>;

struct Pending {
    original_id: u16,
    tx: oneshot::Sender<Reply>,
}

/// Registry of in-flight queries on ONE connection. Outgoing queries get an
/// engine-generated ID unique among in-flight entries; responses are matched by
/// that ID and the caller's original ID is restored (spec §6).
#[derive(Default)]
pub struct Demux {
    inner: Mutex<HashMap<u16, Pending>>,
}

impl Demux {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        original_id: u16,
    ) -> Result<(u16, oneshot::Receiver<Reply>), PresolvError> {
        let mut map = self.inner.lock().unwrap();
        if map.len() >= u16::MAX as usize {
            return Err(PresolvError::Network("message-id space exhausted".into()));
        }
        let id = loop {
            let candidate = rand::random::<u16>();
            if !map.contains_key(&candidate) {
                break candidate;
            }
        };
        let (tx, rx) = oneshot::channel();
        map.insert(id, Pending { original_id, tx });
        Ok((id, rx))
    }

    pub fn cancel(&self, id: u16) {
        self.inner.lock().unwrap().remove(&id);
    }

    /// Returns true if the datagram/frame matched an in-flight query.
    pub fn complete(&self, mut resp: Vec<u8>) -> bool {
        let Some(id) = read_id(&resp) else { return false };
        let pending = self.inner.lock().unwrap().remove(&id);
        let Some(p) = pending else { return false };
        if resp.len() < HEADER_LEN {
            let _ = p.tx.send(Err(PresolvError::Protocol(
                "response shorter than DNS header".into(),
            )));
            return true;
        }
        write_id(&mut resp, p.original_id);
        let _ = p.tx.send(Ok(resp));
        true
    }

    pub fn fail_all(&self, err: PresolvError) {
        let drained: Vec<Pending> = self.inner.lock().unwrap().drain().map(|(_, p)| p).collect();
        for p in drained {
            let _ = p.tx.send(Err(err.clone()));
        }
    }

    pub fn in_flight(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(id: u16, extra: &[u8]) -> Vec<u8> {
        let mut v = vec![0u8; HEADER_LEN];
        v[0] = (id >> 8) as u8;
        v[1] = id as u8;
        v.extend_from_slice(extra);
        v
    }

    #[tokio::test]
    async fn complete_restores_original_id() {
        let d = Demux::new();
        let (engine_id, rx) = d.register(0x0000).unwrap();
        assert!(d.complete(resp(engine_id, b"x")));
        let got = rx.await.unwrap().unwrap();
        assert_eq!(&got[0..2], &[0, 0]);
        assert_eq!(*got.last().unwrap(), b'x');
        assert_eq!(d.in_flight(), 0);
    }

    #[tokio::test]
    async fn duplicate_original_ids_get_distinct_engine_ids() {
        let d = Demux::new();
        let mut ids = std::collections::HashSet::new();
        let mut rxs = vec![];
        for _ in 0..200 {
            let (id, rx) = d.register(7).unwrap();
            assert!(ids.insert(id), "engine id reused while in flight");
            rxs.push((id, rx));
        }
        assert_eq!(d.in_flight(), 200);
        for (id, rx) in rxs {
            d.complete(resp(id, &[]));
            assert_eq!(&rx.await.unwrap().unwrap()[0..2], &[0, 7]);
        }
    }

    #[test]
    fn unknown_id_is_discarded() {
        let d = Demux::new();
        let (id, _rx) = d.register(1).unwrap();
        assert!(!d.complete(resp(id.wrapping_add(1), &[])));
        assert_eq!(d.in_flight(), 1);
    }

    #[tokio::test]
    async fn short_response_with_known_id_is_protocol_error() {
        let d = Demux::new();
        let (id, rx) = d.register(1).unwrap();
        assert!(d.complete(vec![(id >> 8) as u8, id as u8, 0x80]));
        assert!(matches!(rx.await.unwrap(), Err(PresolvError::Protocol(_))));
    }

    #[test]
    fn response_shorter_than_two_bytes_is_ignored() {
        let d = Demux::new();
        assert!(!d.complete(vec![1]));
    }

    #[tokio::test]
    async fn cancel_removes_entry() {
        let d = Demux::new();
        let (id, _rx) = d.register(1).unwrap();
        d.cancel(id);
        assert_eq!(d.in_flight(), 0);
    }

    #[tokio::test]
    async fn fail_all_notifies_everyone() {
        let d = Demux::new();
        let (_, a) = d.register(1).unwrap();
        let (_, b) = d.register(2).unwrap();
        d.fail_all(PresolvError::Network("boom".into()));
        assert!(matches!(a.await.unwrap(), Err(PresolvError::Network(_))));
        assert!(matches!(b.await.unwrap(), Err(PresolvError::Network(_))));
        assert_eq!(d.in_flight(), 0);
    }
}
