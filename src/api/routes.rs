use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use super::handlers::{
    files_handler, health_handler, statistics_handler, torrent_handler, torrents_handler,
    AppState,
};

pub fn create_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // Health endpoint
        .route("/health", get(health_handler))
        // API v2 endpoints
        .route("/api/v2/torrents", get(torrents_handler))
        .route("/api/v2/torrents/:infohash", get(torrent_handler))
        .route("/api/v2/torrents/:infohash/files", get(files_handler))
        .route("/api/v2/statistics", get(statistics_handler))
        // Metrics endpoint (TODO: implement actual metrics)
        .route("/metrics", get(|| async { "# Metrics\n" }))
        .layer(CompressionLayer::new())
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{rocksdb::RocksDbStorage, search::SearchEngine};
    use tempfile::TempDir;

    #[test]
    fn test_create_router() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDbStorage::new(temp_dir.path().join("db"), 100).unwrap());
        let search = Arc::new(SearchEngine::new(temp_dir.path().join("search"), 64).unwrap());

        let state = Arc::new(AppState { storage, search });
        let _router = create_router(state);
    }
}
