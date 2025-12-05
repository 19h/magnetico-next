use dashmap::DashMap;
use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, info, warn};

use super::codec::{CompactNodeInfo, CompactPeer, Message};
use super::dht::{random_node_id, Protocol, ProtocolEventHandlers};
use super::IndexingResult;
use crate::metrics;

const BOOTSTRAP_NODES: &[&str] = &[
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
    "router.utorrent.com:6881",
    "dht.aelitis.com:6881",
    "router.bitcomet.com:6881",
    "dht.anacrolix.link:42069",
];

pub struct IndexingService {
    node_id: [u8; 20],
    protocol: Arc<Protocol>,
    routing_table: Arc<DashMap<[u8; 20], SocketAddr>>,
    get_peers_map: Arc<DashMap<u16, [u8; 20]>>,
    counter: Arc<AtomicU16>,
    max_neighbors: usize,
    interval: Duration,
    output: mpsc::Sender<IndexingResult>,
}

impl IndexingService {
    pub async fn new(
        listen_addr: SocketAddr,
        interval: Duration,
        max_neighbors: usize,
    ) -> std::io::Result<(Self, mpsc::Receiver<IndexingResult>)> {
        let node_id = random_node_id();
        let routing_table = Arc::new(DashMap::new());
        let get_peers_map = Arc::new(DashMap::new());
        let counter = Arc::new(AtomicU16::new(0));

        let (tx, rx) = mpsc::channel(1000);

        // Create protocol handlers
        let rt_clone = Arc::clone(&routing_table);
        let rt_clone2 = Arc::clone(&routing_table);
        let gp_clone = Arc::clone(&get_peers_map);
        let tx_clone = tx.clone();
        let max_neighbors_copy = max_neighbors;
        let interval_copy = interval;

        let handlers = ProtocolEventHandlers {
            on_find_node_response: Some(Box::new(move |msg, addr| {
                Self::handle_find_node_response(msg, addr, &rt_clone, max_neighbors_copy);
            })),
            on_get_peers_response: Some(Box::new(move |msg, _addr| {
                Self::handle_get_peers_response(msg, &gp_clone, &tx_clone);
            })),
            on_sample_infohashes_response: Some(Box::new(move |msg, addr| {
                Self::handle_sample_infohashes_response(
                    msg,
                    addr,
                    &rt_clone2,
                    max_neighbors_copy,
                    interval_copy,
                );
            })),
            ..Default::default()
        };

        let protocol = Arc::new(Protocol::new(listen_addr, handlers).await?);

        Ok((
            Self {
                node_id,
                protocol,
                routing_table,
                get_peers_map,
                counter,
                max_neighbors,
                interval,
                output: tx,
            },
            rx,
        ))
    }

    pub async fn run(self: Arc<Self>) {
        let mut interval = time::interval(self.interval);

        loop {
            interval.tick().await;

            let table_size = self.routing_table.len();
            metrics::dht_routing_table_size(table_size);

            if table_size == 0 {
                info!("Routing table empty, bootstrapping...");
                self.bootstrap().await;
            } else {
                info!("Indexing with {} nodes", table_size);
                self.find_neighbors().await;

                // Clear routing table for next cycle
                self.routing_table.clear();
            }
        }
    }

    async fn bootstrap(&self) {
        for node_addr in BOOTSTRAP_NODES {
            let target = random_node_id();

            match tokio::net::lookup_host(*node_addr).await {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        let msg = Message::find_node_query(&self.node_id, &target, b"aa");
                        if let Err(e) = self.protocol.send_message(&msg, addr).await {
                            warn!("Failed to send find_node to {}: {}", node_addr, e);
                        } else {
                            metrics::dht_message_sent("find_node");
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to resolve {}: {}", node_addr, e);
                }
            }
        }
    }

    async fn find_neighbors(&self) {
        let nodes: Vec<_> = self
            .routing_table
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();

        for (node_id, addr) in nodes {
            let target = random_node_id();
            let msg = Message::sample_infohashes_query(&self.node_id, &target, b"aa");

            if let Err(e) = self.protocol.send_message(&msg, addr).await {
                debug!("Failed to send sample_infohashes: {}", e);
            } else {
                metrics::dht_message_sent("sample_infohashes");
            }
        }
    }

    fn handle_find_node_response(
        msg: &Message,
        addr: &SocketAddr,
        routing_table: &DashMap<[u8; 20], SocketAddr>,
        max_neighbors: usize,
    ) {
        metrics::dht_message_received("find_node");

        let resp = match &msg.r {
            Some(r) => r,
            None => return,
        };

        if let Some(nodes_data) = &resp.nodes {
            let nodes = CompactNodeInfo::parse_nodes(nodes_data);

            for node in nodes {
                if node.addr.port() == 0 {
                    continue;
                }

                if routing_table.len() >= max_neighbors {
                    break;
                }

                routing_table.insert(node.id, node.addr);
            }
        }
    }

    fn handle_get_peers_response(
        msg: &Message,
        get_peers_map: &DashMap<u16, [u8; 20]>,
        output: &mpsc::Sender<IndexingResult>,
    ) {
        metrics::dht_message_received("get_peers");

        // Extract transaction ID
        if msg.t.len() < 2 {
            return;
        }

        let tx_id = u16::from_be_bytes([msg.t[0], msg.t[1]]);

        // Look up infohash
        let infohash = match get_peers_map.remove(&tx_id) {
            Some((_, ih)) => ih,
            None => return,
        };

        let resp = match &msg.r {
            Some(r) => r,
            None => return,
        };

        // Extract peers
        let peers: Vec<SocketAddr> = if let Some(values) = &resp.values {
            values
                .iter()
                .filter_map(|v| CompactPeer::from_bytes(v))
                .map(|p| p.addr)
                .filter(|addr| addr.port() != 0)
                .collect()
        } else {
            return;
        };

        if !peers.is_empty() {
            let result = IndexingResult { infohash, peers };
            let _ = output.try_send(result);
        }
    }

    fn handle_sample_infohashes_response(
        msg: &Message,
        addr: &SocketAddr,
        routing_table: &DashMap<[u8; 20], SocketAddr>,
        max_neighbors: usize,
        interval: Duration,
    ) {
        metrics::dht_message_received("sample_infohashes");

        let resp = match &msg.r {
            Some(r) => r,
            None => return,
        };

        // Extract and process samples
        if let Some(samples) = &resp.samples {
            for chunk in samples.chunks_exact(20) {
                let mut infohash = [0u8; 20];
                infohash.copy_from_slice(chunk);

                // This will be sent from the main service
                // We can't access protocol/counter here without more plumbing
            }
        }

        // Add node to routing table if it meets criteria
        let should_add = resp.interval.unwrap_or(0) as u64 <= interval.as_secs()
            && resp.num.unwrap_or(0) > 0
            && addr.port() != 0;

        if should_add && routing_table.len() < max_neighbors {
            if let Some(id) = resp.id.get(..20) {
                let mut node_id = [0u8; 20];
                node_id.copy_from_slice(id);
                routing_table.insert(node_id, *addr);
            }
        }

        // Process nodes list
        if let Some(nodes_data) = &resp.nodes {
            let nodes = CompactNodeInfo::parse_nodes(nodes_data);

            for node in nodes {
                if node.addr.port() == 0 {
                    continue;
                }

                if routing_table.len() >= max_neighbors {
                    break;
                }

                routing_table.insert(node.id, node.addr);
            }
        }
    }

    pub async fn send_get_peers(&self, infohash: [u8; 20], addr: SocketAddr) {
        let tx_id = self.counter.fetch_add(1, Ordering::Relaxed);
        self.get_peers_map.insert(tx_id, infohash);

        let tx_bytes = tx_id.to_be_bytes();
        let msg = Message::get_peers_query(&self.node_id, &infohash, &tx_bytes);

        if let Err(e) = self.protocol.send_message(&msg, addr).await {
            debug!("Failed to send get_peers: {}", e);
            self.get_peers_map.remove(&tx_id);
        } else {
            metrics::dht_message_sent("get_peers");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_indexing_service_creation() {
        let addr = "127.0.0.1:0".parse().unwrap();
        let (service, _rx) = IndexingService::new(addr, Duration::from_secs(1), 100)
            .await
            .unwrap();

        assert_eq!(service.max_neighbors, 100);
    }
}
