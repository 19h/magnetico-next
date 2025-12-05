use dashmap::DashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info};

use super::metadata::MetadataLeech;
use super::{IndexingResult, Metadata};

pub struct MetadataSink {
    peer_id: [u8; 20],
    deadline: Duration,
    max_leeches: usize,
    pending: Arc<DashMap<[u8; 20], VecDeque<SocketAddr>>>,
    drain_tx: mpsc::Sender<Metadata>,
    drain_rx: Option<mpsc::Receiver<Metadata>>,
    active_count: Arc<AtomicUsize>,
    deleted_count: Arc<AtomicUsize>,
}

impl MetadataSink {
    pub fn new(deadline: Duration, max_leeches: usize) -> Self {
        let (tx, rx) = mpsc::channel(100);

        Self {
            peer_id: Self::generate_peer_id(),
            deadline,
            max_leeches,
            pending: Arc::new(DashMap::new()),
            drain_tx: tx,
            drain_rx: Some(rx),
            active_count: Arc::new(AtomicUsize::new(0)),
            deleted_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn generate_peer_id() -> [u8; 20] {
        // Generate peer ID: -MC0001- + 12 random digits
        let mut peer_id = *b"-MC0001-            ";
        let suffix: Vec<u8> = (0..12)
            .map(|_| rand::random::<u8>() % 10 + b'0')
            .collect();
        peer_id[8..20].copy_from_slice(&suffix);
        peer_id
    }

    pub fn take_drain(&mut self) -> Option<mpsc::Receiver<Metadata>> {
        self.drain_rx.take()
    }

    pub async fn sink(&self, result: IndexingResult) {
        // Skip if already processing
        if self.pending.contains_key(&result.infohash) {
            return;
        }

        // Skip if at max capacity
        if self.active_count.load(Ordering::Relaxed) >= self.max_leeches {
            return;
        }

        if result.peers.is_empty() {
            return;
        }

        // Take first peer, store rest
        let (first_peer, remaining): (SocketAddr, VecDeque<SocketAddr>) = {
            let mut peers: VecDeque<_> = result.peers.into();
            let first = peers.pop_front().unwrap();
            (first, peers)
        };

        self.pending.insert(result.infohash, remaining);
        self.active_count.fetch_add(1, Ordering::Relaxed);

        // Spawn leech task
        let infohash = result.infohash;
        let peer_id = self.peer_id;
        let deadline_instant = Instant::now() + self.deadline;
        let drain_tx = self.drain_tx.clone();
        let pending = Arc::clone(&self.pending);
        let active_count = Arc::clone(&self.active_count);
        let deleted_count = Arc::clone(&self.deleted_count);

        tokio::spawn(async move {
            let leech = MetadataLeech::new(infohash, first_peer, peer_id, deadline_instant);

            match leech.fetch().await {
                Ok(metadata) => {
                    // Success - send to drain
                    let _ = drain_tx.send(metadata).await;
                    pending.remove(&infohash);
                    active_count.fetch_sub(1, Ordering::Relaxed);
                }
                Err(e) => {
                    // Error - try next peer
                    debug!("Leech error for {:?}: {}", hex::encode(infohash), e);
                    Self::retry_with_next_peer(
                        infohash,
                        peer_id,
                        deadline_instant,
                        drain_tx,
                        pending,
                        active_count,
                        deleted_count,
                    )
                    .await;
                }
            }
        });

        debug!(
            "Sunk infohash {:?}, active={}",
            hex::encode(result.infohash),
            self.active_count.load(Ordering::Relaxed)
        );
    }

    async fn retry_with_next_peer(
        infohash: [u8; 20],
        peer_id: [u8; 20],
        deadline: Instant,
        drain_tx: mpsc::Sender<Metadata>,
        pending: Arc<DashMap<[u8; 20], VecDeque<SocketAddr>>>,
        active_count: Arc<AtomicUsize>,
        deleted_count: Arc<AtomicUsize>,
    ) {
        // Try to get next peer
        let next_peer = {
            if let Some(mut entry) = pending.get_mut(&infohash) {
                entry.pop_front()
            } else {
                None
            }
        };

        if let Some(peer_addr) = next_peer {
            // Retry with next peer
            let leech = MetadataLeech::new(infohash, peer_addr, peer_id, deadline);

            match leech.fetch().await {
                Ok(metadata) => {
                    let _ = drain_tx.send(metadata).await;
                    pending.remove(&infohash);
                    active_count.fetch_sub(1, Ordering::Relaxed);
                }
                Err(e) => {
                    debug!("Retry failed for {:?}: {}", hex::encode(infohash), e);
                    // Recursive retry
                    Box::pin(Self::retry_with_next_peer(
                        infohash,
                        peer_id,
                        deadline,
                        drain_tx,
                        pending,
                        active_count,
                        deleted_count,
                    ))
                    .await;
                }
            }
        } else {
            // No more peers, give up
            pending.remove(&infohash);
            active_count.fetch_sub(1, Ordering::Relaxed);
            deleted_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub async fn run_status_logger(self: Arc<Self>) {
        let mut interval = time::interval(self.deadline);

        loop {
            interval.tick().await;

            let active = self.active_count.load(Ordering::Relaxed);
            let deleted = self.deleted_count.swap(0, Ordering::Relaxed);
            let drain_queue = self.drain_tx.capacity() - self.drain_tx.max_capacity();

            info!(
                "Metadata sink status: active={}, deleted={}, drain_queue={}",
                active, deleted, drain_queue
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_creation() {
        let sink = MetadataSink::new(Duration::from_secs(3), 500);
        assert_eq!(sink.max_leeches, 500);
        assert_eq!(sink.peer_id[0..8], *b"-MC0001-");
    }

    #[test]
    fn test_peer_id_generation() {
        let peer_id = MetadataSink::generate_peer_id();
        assert_eq!(&peer_id[0..8], b"-MC0001-");
        // Check that suffix is digits
        for &b in &peer_id[8..20] {
            assert!(b >= b'0' && b <= b'9');
        }
    }
}
