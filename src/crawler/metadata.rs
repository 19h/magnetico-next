use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, warn};

use super::{File, Metadata};
use crate::metrics;

const MAX_METADATA_SIZE: usize = 10 * 1024 * 1024; // 10 MB
const PIECE_SIZE: usize = 16384; // 16 KiB

#[derive(Debug, Serialize, Deserialize)]
struct ExtensionHandshake {
    m: MDict,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MDict {
    ut_metadata: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetadataMessage {
    msg_type: i64,
    piece: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_size: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TorrentInfo {
    name: String,
    #[serde(rename = "piece length")]
    piece_length: i64,
    pieces: Vec<u8>,
    #[serde(default)]
    length: i64,
    #[serde(default)]
    files: Vec<TorrentFile>,
}

#[derive(Debug, Deserialize)]
struct TorrentFile {
    length: i64,
    path: Vec<String>,
}

pub struct MetadataLeech {
    infohash: [u8; 20],
    peer_addr: SocketAddr,
    peer_id: [u8; 20],
    deadline: Instant,
}

impl MetadataLeech {
    pub fn new(
        infohash: [u8; 20],
        peer_addr: SocketAddr,
        peer_id: [u8; 20],
        deadline: Instant,
    ) -> Self {
        Self {
            infohash,
            peer_addr,
            peer_id,
            deadline,
        }
    }

    pub async fn fetch(self) -> Result<Metadata, Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        let result = self.fetch_internal().await;

        let duration = start.elapsed().as_secs_f64();
        metrics::metadata_fetch_duration(duration);

        match result {
            Ok(metadata) => {
                metrics::metadata_fetched();
                Ok(metadata)
            }
            Err(e) => {
                let error_str = format!("{:?}", e);
                let error_type = error_str.split(':').next().unwrap_or("unknown");
                metrics::metadata_fetch_error(error_type);
                Err(e)
            }
        }
    }

    async fn fetch_internal(self) -> Result<Metadata, Box<dyn std::error::Error + Send + Sync>> {
        // Connect with timeout
        let time_left = self.deadline.saturating_duration_since(Instant::now());
        let mut stream = timeout(time_left, TcpStream::connect(self.peer_addr)).await??;

        // Set socket options
        stream.set_nodelay(true)?;

        // BitTorrent handshake
        self.do_bt_handshake(&mut stream).await?;

        // Extension handshake
        let (ut_metadata_id, metadata_size) = self.do_extension_handshake(&mut stream).await?;

        if metadata_size == 0 || metadata_size > MAX_METADATA_SIZE {
            return Err("Invalid metadata size".into());
        }

        // Request all pieces
        let num_pieces = (metadata_size + PIECE_SIZE - 1) / PIECE_SIZE;
        for piece in 0..num_pieces {
            self.request_piece(&mut stream, ut_metadata_id, piece as i64)
                .await?;
        }

        // Receive all pieces
        let metadata = self
            .receive_pieces(&mut stream, ut_metadata_id, metadata_size, num_pieces)
            .await?;

        // Verify SHA-1
        let hash = Sha1::digest(&metadata);
        if hash.as_slice() != self.infohash {
            return Err("Infohash mismatch".into());
        }

        // Parse and validate info dictionary
        let info: TorrentInfo = serde_bencode::from_bytes(&metadata)?;
        self.validate_info(&info)?;

        // Extract files
        let files = if info.files.is_empty() {
            vec![File {
                path: info.name.clone(),
                size: info.length,
            }]
        } else {
            info.files
                .iter()
                .map(|f| File {
                    path: format!("{}/{}", info.name, f.path.join("/")),
                    size: f.length,
                })
                .collect()
        };

        let total_size: u64 = files.iter().map(|f| f.size as u64).sum();

        let discovered_on = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        Ok(Metadata {
            infohash: self.infohash,
            name: info.name,
            total_size,
            discovered_on,
            files,
        })
    }

    async fn do_bt_handshake(&self, stream: &mut TcpStream) -> io::Result<()> {
        // Build handshake: 68 bytes
        let mut handshake = Vec::with_capacity(68);
        handshake.push(19); // Protocol name length
        handshake.extend_from_slice(b"BitTorrent protocol");
        handshake.extend_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0x01]); // Reserved (extension bit set)
        handshake.extend_from_slice(&self.infohash);
        handshake.extend_from_slice(&self.peer_id);

        stream.write_all(&handshake).await?;

        // Read response
        let mut response = vec![0u8; 68];
        stream.read_exact(&mut response).await?;

        // Validate
        if &response[0..20] != b"\x13BitTorrent protocol" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid handshake",
            ));
        }

        // Check extension protocol support
        if response[25] & 0x10 == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Peer does not support extension protocol",
            ));
        }

        Ok(())
    }

    async fn do_extension_handshake(
        &self,
        stream: &mut TcpStream,
    ) -> Result<(u8, usize), Box<dyn std::error::Error + Send + Sync>> {
        // Send extension handshake
        let handshake = ExtensionHandshake {
            m: MDict { ut_metadata: 1 },
            metadata_size: None,
        };

        let payload = serde_bencode::to_bytes(&handshake)?;
        let mut msg = Vec::new();
        msg.extend_from_slice(&(payload.len() as u32 + 2).to_be_bytes());
        msg.push(20); // Extension message
        msg.push(0); // Extension handshake
        msg.extend_from_slice(&payload);

        stream.write_all(&msg).await?;

        // Read response
        let response = self.read_extension_message(stream).await?;

        if response.len() < 2 || response[0] != 20 || response[1] != 0 {
            return Err("Invalid extension handshake response".into());
        }

        let handshake: ExtensionHandshake = serde_bencode::from_bytes(&response[2..])?;

        let ut_metadata_id = handshake.m.ut_metadata as u8;
        let metadata_size = handshake
            .metadata_size
            .ok_or("No metadata_size in handshake")?;

        Ok((ut_metadata_id, metadata_size))
    }

    async fn request_piece(
        &self,
        stream: &mut TcpStream,
        ut_metadata_id: u8,
        piece: i64,
    ) -> io::Result<()> {
        let msg_dict = MetadataMessage {
            msg_type: 0, // REQUEST
            piece,
            total_size: None,
        };

        let payload = serde_bencode::to_bytes(&msg_dict)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut msg = Vec::new();
        msg.extend_from_slice(&(payload.len() as u32 + 2).to_be_bytes());
        msg.push(20); // Extension message
        msg.push(ut_metadata_id);
        msg.extend_from_slice(&payload);

        stream.write_all(&msg).await
    }

    async fn receive_pieces(
        &self,
        stream: &mut TcpStream,
        ut_metadata_id: u8,
        metadata_size: usize,
        num_pieces: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let mut metadata = vec![0u8; metadata_size];
        let mut received = 0;

        while received < metadata_size {
            let msg = self.read_extension_message(stream).await?;

            if msg.len() < 2 || msg[0] != 20 {
                continue;
            }

            if msg[1] != ut_metadata_id {
                continue;
            }

            // Parse bencode dict to find where data starts
            let mut decoder = serde_bencode::Deserializer::new(&msg[2..]);
            let msg_dict: MetadataMessage = serde::Deserialize::deserialize(&mut decoder)?;

            if msg_dict.msg_type == 2 {
                // REJECT
                return Err("Peer rejected metadata request".into());
            }

            if msg_dict.msg_type == 1 {
                // DATA
                // Calculate the bencode dictionary length by re-encoding
                let dict_bytes = serde_bencode::to_bytes(&msg_dict)?;
                let dict_len = dict_bytes.len();
                let piece_data = &msg[2 + dict_len..];

                if piece_data.len() > PIECE_SIZE {
                    return Err("Piece too large".into());
                }

                let offset = msg_dict.piece as usize * PIECE_SIZE;
                metadata[offset..offset + piece_data.len()].copy_from_slice(piece_data);
                received += piece_data.len();
            }
        }

        Ok(metadata)
    }

    async fn read_extension_message(
        &self,
        stream: &mut TcpStream,
    ) -> io::Result<Vec<u8>> {
        loop {
            // Read message length
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let msg_len = u32::from_be_bytes(len_buf) as usize;

            if msg_len == 0 || msg_len > MAX_METADATA_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid message length",
                ));
            }

            // Read message
            let mut msg = vec![0u8; msg_len];
            stream.read_exact(&mut msg).await?;

            // Return only extension messages
            if !msg.is_empty() && msg[0] == 20 {
                return Ok(msg);
            }
        }
    }

    fn validate_info(&self, info: &TorrentInfo) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if info.pieces.len() % 20 != 0 {
            return Err("Invalid pieces length".into());
        }

        if info.piece_length == 0 && self.total_length(info) != 0 {
            return Err("Zero piece length".into());
        }

        let expected_pieces = if info.piece_length > 0 {
            ((self.total_length(info) + info.piece_length - 1) / info.piece_length) as usize
        } else {
            0
        };

        if expected_pieces != info.pieces.len() / 20 {
            return Err("Piece count mismatch".into());
        }

        Ok(())
    }

    fn total_length(&self, info: &TorrentInfo) -> i64 {
        if info.files.is_empty() {
            info.length
        } else {
            info.files.iter().map(|f| f.length).sum()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_leech_creation() {
        let infohash = [1u8; 20];
        let peer_addr = "127.0.0.1:6881".parse().unwrap();
        let peer_id = [2u8; 20];
        let deadline = Instant::now() + Duration::from_secs(3);

        let leech = MetadataLeech::new(infohash, peer_addr, peer_id, deadline);
        assert_eq!(leech.infohash, infohash);
    }
}
