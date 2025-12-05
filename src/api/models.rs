use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentResponse {
    pub id: u64,
    pub info_hash: String,
    pub name: String,
    pub size: u64,
    pub discovered_on: i64,
    pub n_files: u32,
    pub relevance: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileResponse {
    pub path: String,
    pub size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentsQuery {
    pub q: Option<String>,
    pub order_by: Option<OrderBy>,
    pub order: Option<Order>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderBy {
    Relevance,
    Size,
    Discovered,
    Files,
    Updated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentsResponse {
    pub torrents: Vec<TorrentResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

