use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;

use super::error::ApiError;
use super::models::{
    FileResponse, HealthResponse, Order, OrderBy, TorrentResponse, TorrentsQuery,
    TorrentsResponse,
};
use crate::storage::{rocksdb::RocksDbStorage, search::SearchEngine};

pub struct AppState {
    pub storage: Arc<RocksDbStorage>,
    pub search: Arc<SearchEngine>,
}

pub async fn health_handler() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

pub async fn torrents_handler(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TorrentsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let q = query.q.as_deref().unwrap_or("");
    let limit = query.limit.unwrap_or(20).min(100) as usize;

    // Search torrents
    let infohashes = if q.is_empty() {
        // TODO: Return recent torrents
        Vec::new()
    } else {
        state
            .search
            .combined_search(q, limit)
            .map_err(|e| ApiError::Internal(format!("Search error: {}", e)))?
    };

    // Get torrent details
    let mut torrents = Vec::new();
    for infohash in infohashes {
        if let Some(torrent) = state
            .storage
            .get_torrent(&infohash)
            .map_err(|e| ApiError::Internal(format!("Storage error: {}", e)))?
        {
            torrents.push(TorrentResponse {
                id: 0, // TODO: Generate proper ID
                info_hash: hex::encode(torrent.info_hash),
                name: torrent.name,
                size: torrent.total_size,
                discovered_on: torrent.discovered_on,
                n_files: torrent.n_files,
                relevance: Some(1.0), // TODO: Calculate relevance
            });
        }
    }

    Ok(Json(TorrentsResponse {
        torrents,
        next_cursor: None,
    }))
}

pub async fn torrent_handler(
    State(state): State<Arc<AppState>>,
    Path(infohash_hex): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let infohash = hex::decode(&infohash_hex)
        .map_err(|_| ApiError::BadRequest("Invalid infohash".to_string()))?;

    if infohash.len() != 20 {
        return Err(ApiError::BadRequest("Infohash must be 40 hex chars".to_string()));
    }

    let mut infohash_arr = [0u8; 20];
    infohash_arr.copy_from_slice(&infohash);

    let torrent = state
        .storage
        .get_torrent(&infohash_arr)
        .map_err(|e| ApiError::Internal(format!("Storage error: {}", e)))?
        .ok_or_else(|| ApiError::NotFound("Torrent not found".to_string()))?;

    Ok(Json(TorrentResponse {
        id: 0,
        info_hash: infohash_hex,
        name: torrent.name,
        size: torrent.total_size,
        discovered_on: torrent.discovered_on,
        n_files: torrent.n_files,
        relevance: None,
    }))
}

pub async fn files_handler(
    State(state): State<Arc<AppState>>,
    Path(infohash_hex): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let infohash = hex::decode(&infohash_hex)
        .map_err(|_| ApiError::BadRequest("Invalid infohash".to_string()))?;

    if infohash.len() != 20 {
        return Err(ApiError::BadRequest("Infohash must be 40 hex chars".to_string()));
    }

    let mut infohash_arr = [0u8; 20];
    infohash_arr.copy_from_slice(&infohash);

    let files = state
        .storage
        .get_files(&infohash_arr)
        .map_err(|e| ApiError::Internal(format!("Storage error: {}", e)))?;

    let response: Vec<FileResponse> = files
        .into_iter()
        .map(|f| FileResponse {
            path: f.path,
            size: f.size,
        })
        .collect();

    Ok(Json(response))
}

pub async fn statistics_handler() -> Result<impl IntoResponse, ApiError> {
    // TODO: Implement statistics
    Err::<Json<()>, _>(ApiError::Internal("Not yet implemented".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_handler() {
        let response = health_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
