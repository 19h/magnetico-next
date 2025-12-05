use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Main DHT message structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Message type: "q" (query), "r" (response), or "e" (error)
    pub y: String,
    
    /// Transaction ID
    pub t: Vec<u8>,
    
    /// Query method name (for queries only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    
    /// Query arguments (for queries only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a: Option<QueryArguments>,
    
    /// Response values (for responses only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r: Option<ResponseValues>,
    
    /// Error information (for errors only)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub e: Option<ErrorResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryArguments {
    /// Querying node's ID
    pub id: Vec<u8>,
    
    /// InfoHash (for get_peers, announce_peer)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info_hash: Option<Vec<u8>>,
    
    /// Target node ID (for find_node, sample_infohashes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<Vec<u8>>,
    
    /// Token (for announce_peer)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<Vec<u8>>,
    
    /// Port (for announce_peer)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i64>,
    
    /// Implied port (for announce_peer)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub implied_port: Option<i64>,
    
    /// BEP 33: Seed flag
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,
    
    /// BEP 33: No seed flag
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noseed: Option<i64>,
    
    /// BEP 33: Scrape flag
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scrape: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseValues {
    /// Responding node's ID
    pub id: Vec<u8>,
    
    /// Compact node info list
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<Vec<u8>>,
    
    /// Token (for get_peers responses)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<Vec<u8>>,
    
    /// Peer list (for get_peers responses)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<Vec<u8>>>,
    
    /// BEP 51: Refresh interval in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<i64>,
    
    /// BEP 51: Number of infohashes in storage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num: Option<i64>,
    
    /// BEP 51: Sample of infohashes (N × 20 bytes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samples: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub code: i64,
    pub message: String,
}

/// Compact node info: 26 bytes (20 ID + 4 IPv4 + 2 port)
#[derive(Debug, Clone)]
pub struct CompactNodeInfo {
    pub id: [u8; 20],
    pub addr: SocketAddr,
}

/// Compact peer: 6 bytes (4 IPv4 + 2 port) or 18 bytes (16 IPv6 + 2 port)
#[derive(Debug, Clone)]
pub struct CompactPeer {
    pub addr: SocketAddr,
}

impl Message {
    /// Create a find_node query
    pub fn find_node_query(id: &[u8; 20], target: &[u8; 20], transaction_id: &[u8]) -> Self {
        Self {
            y: "q".to_string(),
            t: transaction_id.to_vec(),
            q: Some("find_node".to_string()),
            a: Some(QueryArguments {
                id: id.to_vec(),
                target: Some(target.to_vec()),
                info_hash: None,
                token: None,
                port: None,
                implied_port: None,
                seed: None,
                noseed: None,
                scrape: None,
            }),
            r: None,
            e: None,
        }
    }
    
    /// Create a get_peers query
    pub fn get_peers_query(id: &[u8; 20], infohash: &[u8; 20], transaction_id: &[u8]) -> Self {
        Self {
            y: "q".to_string(),
            t: transaction_id.to_vec(),
            q: Some("get_peers".to_string()),
            a: Some(QueryArguments {
                id: id.to_vec(),
                info_hash: Some(infohash.to_vec()),
                target: None,
                token: None,
                port: None,
                implied_port: None,
                seed: None,
                noseed: None,
                scrape: None,
            }),
            r: None,
            e: None,
        }
    }
    
    /// Create a sample_infohashes query (BEP 51)
    pub fn sample_infohashes_query(
        id: &[u8; 20],
        target: &[u8; 20],
        transaction_id: &[u8],
    ) -> Self {
        Self {
            y: "q".to_string(),
            t: transaction_id.to_vec(),
            q: Some("sample_infohashes".to_string()),
            a: Some(QueryArguments {
                id: id.to_vec(),
                target: Some(target.to_vec()),
                info_hash: None,
                token: None,
                port: None,
                implied_port: None,
                seed: None,
                noseed: None,
                scrape: None,
            }),
            r: None,
            e: None,
        }
    }
    
    /// Validate query message
    pub fn validate_query(&self) -> bool {
        if self.y != "q" || self.a.is_none() {
            return false;
        }
        
        let args = self.a.as_ref().unwrap();
        
        // Node ID must be 20 bytes
        if args.id.len() != 20 {
            return false;
        }
        
        // Validate based on query type
        match self.q.as_deref() {
            Some("ping") => true,
            Some("find_node") => args.target.as_ref().map_or(false, |t| t.len() == 20),
            Some("get_peers") => args.info_hash.as_ref().map_or(false, |ih| ih.len() == 20),
            Some("announce_peer") => {
                args.info_hash.as_ref().map_or(false, |ih| ih.len() == 20)
                    && args.token.as_ref().map_or(false, |t| !t.is_empty())
                    && args.port.is_some()
            }
            Some("sample_infohashes") => args.target.as_ref().map_or(false, |t| t.len() == 20),
            _ => false,
        }
    }
    
    /// Validate response message
    pub fn validate_response(&self) -> bool {
        if self.y != "r" || self.r.is_none() {
            return false;
        }
        
        let resp = self.r.as_ref().unwrap();
        
        // Node ID must be 20 bytes
        if resp.id.len() != 20 {
            return false;
        }
        
        // Validate compact nodes if present
        if let Some(nodes) = &resp.nodes {
            if nodes.len() % 26 != 0 {
                return false;
            }
        }
        
        // Validate samples if present
        if let Some(samples) = &resp.samples {
            if samples.len() % 20 != 0 {
                return false;
            }
        }
        
        true
    }
}

impl CompactNodeInfo {
    /// Parse from 26-byte compact format
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() != 26 {
            return None;
        }
        
        let mut id = [0u8; 20];
        id.copy_from_slice(&data[0..20]);
        
        let ip = Ipv4Addr::new(data[20], data[21], data[22], data[23]);
        let port = u16::from_be_bytes([data[24], data[25]]);
        
        Some(Self {
            id,
            addr: SocketAddr::new(IpAddr::V4(ip), port),
        })
    }
    
    /// Parse multiple nodes from compact format
    pub fn parse_nodes(data: &[u8]) -> Vec<Self> {
        data.chunks_exact(26)
            .filter_map(Self::from_bytes)
            .collect()
    }
    
    /// Encode to 26-byte compact format
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(26);
        result.extend_from_slice(&self.id);
        
        match self.addr.ip() {
            IpAddr::V4(ip) => result.extend_from_slice(&ip.octets()),
            IpAddr::V6(_) => {
                // IPv6 not supported in compact format, use zeros
                result.extend_from_slice(&[0, 0, 0, 0]);
            }
        }
        
        result.extend_from_slice(&self.addr.port().to_be_bytes());
        result
    }
}

impl CompactPeer {
    /// Parse from 6-byte (IPv4) or 18-byte (IPv6) compact format
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        match data.len() {
            6 => {
                let ip = Ipv4Addr::new(data[0], data[1], data[2], data[3]);
                let port = u16::from_be_bytes([data[4], data[5]]);
                Some(Self {
                    addr: SocketAddr::new(IpAddr::V4(ip), port),
                })
            }
            18 => {
                // IPv6 support
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&data[0..16]);
                let ip = std::net::Ipv6Addr::from(octets);
                let port = u16::from_be_bytes([data[16], data[17]]);
                Some(Self {
                    addr: SocketAddr::new(IpAddr::V6(ip), port),
                })
            }
            _ => None,
        }
    }
    
    /// Parse multiple peers from compact format
    pub fn parse_peers(data: &[u8]) -> Vec<Self> {
        if data.len() % 6 == 0 {
            // IPv4 peers
            data.chunks_exact(6)
                .filter_map(Self::from_bytes)
                .collect()
        } else if data.len() % 18 == 0 {
            // IPv6 peers
            data.chunks_exact(18)
                .filter_map(Self::from_bytes)
                .collect()
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_compact_node_info() {
        let id = [1u8; 20];
        let addr = "192.168.1.1:6881".parse().unwrap();
        let node = CompactNodeInfo { id, addr };
        
        let bytes = node.to_bytes();
        assert_eq!(bytes.len(), 26);
        
        let parsed = CompactNodeInfo::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.addr, addr);
    }
    
    #[test]
    fn test_compact_peer() {
        let data = vec![192, 168, 1, 1, 0x1A, 0xE1]; // 192.168.1.1:6881
        let peer = CompactPeer::from_bytes(&data).unwrap();
        assert_eq!(peer.addr, "192.168.1.1:6881".parse().unwrap());
    }
}

