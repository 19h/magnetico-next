use rand::Rng;
use sha1::{Digest, Sha1};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use tracing::{debug, warn};

use super::codec::{CompactNodeInfo, CompactPeer, Message};
use super::transport::Transport;

/// Token secret manager for DHT protocol
pub struct TokenManager {
    current: [u8; 20],
    previous: [u8; 20],
}

impl TokenManager {
    pub fn new() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            current: rng.gen(),
            previous: [0u8; 20],
        }
    }

    pub fn calculate_token(&self, ip: &IpAddr) -> [u8; 20] {
        let mut hasher = Sha1::new();
        hasher.update(&self.current);
        match ip {
            IpAddr::V4(ipv4) => hasher.update(ipv4.octets()),
            IpAddr::V6(ipv6) => hasher.update(ipv6.octets()),
        }
        hasher.finalize().into()
    }

    pub fn verify_token(&self, ip: &IpAddr, token: &[u8]) -> bool {
        let current_token = self.calculate_token(ip);
        if token == current_token.as_slice() {
            return true;
        }

        // Try previous token
        let mut hasher = Sha1::new();
        hasher.update(&self.previous);
        match ip {
            IpAddr::V4(ipv4) => hasher.update(ipv4.octets()),
            IpAddr::V6(ipv6) => hasher.update(ipv6.octets()),
        }
        let previous_token: [u8; 20] = hasher.finalize().into();
        token == previous_token.as_slice()
    }

    pub fn rotate(&mut self) {
        self.previous = self.current;
        let mut rng = rand::thread_rng();
        self.current = rng.gen();
    }
}

/// DHT Protocol event handlers
pub struct ProtocolEventHandlers {
    pub on_ping_query: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_find_node_query: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_get_peers_query: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_announce_peer_query: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_sample_infohashes_query: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_find_node_response: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_get_peers_response: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_sample_infohashes_response: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_ping_or_announce_response: Option<Box<dyn Fn(&Message, &SocketAddr) + Send + Sync>>,
    pub on_congestion: Option<Box<dyn Fn() + Send + Sync>>,
}

impl Default for ProtocolEventHandlers {
    fn default() -> Self {
        Self {
            on_ping_query: None,
            on_find_node_query: None,
            on_get_peers_query: None,
            on_announce_peer_query: None,
            on_sample_infohashes_query: None,
            on_find_node_response: None,
            on_get_peers_response: None,
            on_sample_infohashes_response: None,
            on_ping_or_announce_response: None,
            on_congestion: None,
        }
    }
}

/// DHT Protocol handler
pub struct Protocol {
    token_manager: Arc<RwLock<TokenManager>>,
    transport: Arc<Transport>,
    handlers: Arc<ProtocolEventHandlers>,
}

impl Protocol {
    pub async fn new(
        laddr: SocketAddr,
        handlers: ProtocolEventHandlers,
    ) -> std::io::Result<Self> {
        let token_manager = Arc::new(RwLock::new(TokenManager::new()));

        // Start token rotation task
        let tm_clone = Arc::clone(&token_manager);
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(600)); // 10 minutes
            loop {
                interval.tick().await;
                let mut tm = tm_clone.write().await;
                tm.rotate();
                debug!("DHT token rotated");
            }
        });

        // Create transport with message handler
        let handlers_ptr = Arc::new(handlers);
        let handlers_clone = Arc::clone(&handlers_ptr);

        let transport = Transport::new(
            laddr,
            Box::new(move |msg, addr| {
                Self::handle_message(&msg, &addr, &handlers_clone);
            }),
            Some(Box::new(|| {
                // Congestion callback
                warn!("DHT transport congestion detected");
            })),
        )
        .await?;

        Ok(Self {
            token_manager,
            transport: Arc::new(transport),
            handlers: handlers_ptr,
        })
    }

    fn handle_message(msg: &Message, addr: &SocketAddr, handlers: &ProtocolEventHandlers) {
        match msg.y.as_str() {
            "q" => Self::handle_query(msg, addr, handlers),
            "r" => Self::handle_response(msg, addr, handlers),
            "e" => Self::handle_error(msg, addr),
            _ => {
                debug!("Unknown message type from {}: {}", addr, msg.y);
            }
        }
    }

    fn handle_query(msg: &Message, addr: &SocketAddr, handlers: &ProtocolEventHandlers) {
        if !msg.validate_query() {
            debug!("Invalid query from {}", addr);
            return;
        }

        match msg.q.as_deref() {
            Some("ping") => {
                if let Some(ref handler) = handlers.on_ping_query {
                    handler(msg, addr);
                }
            }
            Some("find_node") => {
                if let Some(ref handler) = handlers.on_find_node_query {
                    handler(msg, addr);
                }
            }
            Some("get_peers") => {
                if let Some(ref handler) = handlers.on_get_peers_query {
                    handler(msg, addr);
                }
            }
            Some("announce_peer") => {
                if let Some(ref handler) = handlers.on_announce_peer_query {
                    handler(msg, addr);
                }
            }
            Some("sample_infohashes") => {
                if let Some(ref handler) = handlers.on_sample_infohashes_query {
                    handler(msg, addr);
                }
            }
            Some("vote") => {
                // Ignore vote messages
            }
            _ => {
                debug!("Unknown query method from {}: {:?}", addr, msg.q);
            }
        }
    }

    fn handle_response(msg: &Message, addr: &SocketAddr, handlers: &ProtocolEventHandlers) {
        if !msg.validate_response() {
            debug!("Invalid response from {}", addr);
            return;
        }

        let resp = msg.r.as_ref().unwrap();

        // Ad-hoc response type detection based on field presence
        if resp.samples.is_some() {
            // sample_infohashes response
            if let Some(ref handler) = handlers.on_sample_infohashes_response {
                handler(msg, addr);
            }
        } else if resp.token.is_some() {
            // get_peers response
            if let Some(ref handler) = handlers.on_get_peers_response {
                handler(msg, addr);
            }
        } else if resp.nodes.is_some() {
            // find_node response
            if let Some(ref handler) = handlers.on_find_node_response {
                handler(msg, addr);
            }
        } else {
            // ping or announce_peer response
            if let Some(ref handler) = handlers.on_ping_or_announce_response {
                handler(msg, addr);
            }
        }
    }

    fn handle_error(msg: &Message, addr: &SocketAddr) {
        if let Some(ref err) = msg.e {
            // Ignore common errors
            if err.code == 202 || err.code == 204 {
                return;
            }
            debug!(
                "Protocol error from {}: code={}, msg={}",
                addr,
                err.code,
                String::from_utf8_lossy(&err.message.as_bytes())
            );
        }
    }

    pub async fn send_message(&self, msg: &Message, addr: SocketAddr) -> std::io::Result<()> {
        self.transport.send_message(msg, addr).await
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.transport.local_addr()
    }
}

/// Generate a random node ID
pub fn random_node_id() -> [u8; 20] {
    rand::thread_rng().gen()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_manager() {
        let tm = TokenManager::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let token = tm.calculate_token(&ip);
        assert_eq!(token.len(), 20);
        assert!(tm.verify_token(&ip, &token));
    }

    #[test]
    fn test_token_rotation() {
        let mut tm = TokenManager::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        let token1 = tm.calculate_token(&ip);
        
        tm.rotate();
        
        // Old token should still be valid
        assert!(tm.verify_token(&ip, &token1));
        
        // New token should also be valid
        let token2 = tm.calculate_token(&ip);
        assert!(tm.verify_token(&ip, &token2));
        assert_ne!(token1, token2);
    }
}
