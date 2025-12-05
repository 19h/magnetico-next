use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentData {
    pub info_hash: [u8; 20],
    pub name: String,
    pub total_size: u64,
    pub discovered_on: i64,
    pub modified_on: i64,
    pub n_files: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileData {
    pub path: String,
    pub size: i64,
}

impl TorrentData {
    pub fn new(
        info_hash: [u8; 20],
        name: String,
        total_size: u64,
        discovered_on: i64,
        n_files: u32,
    ) -> Self {
        Self {
            info_hash,
            name,
            total_size,
            discovered_on,
            modified_on: discovered_on,
            n_files,
        }
    }
}

impl FileData {
    pub fn new(path: String, size: i64) -> Self {
        Self { path, size }
    }
}

