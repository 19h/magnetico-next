// Web UI handlers - serving HTML pages

use axum::{
    extract::State,
    response::{Html, IntoResponse},
};
use std::sync::Arc;

use super::handlers::AppState;

// TODO: Implement Askama templates
// For now, serve basic HTML

pub async fn homepage_handler(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    Html(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>Magnetico - DHT Search Engine</title>
    <meta charset="utf-8">
</head>
<body>
    <h1>Magnetico</h1>
    <p>Autonomous BitTorrent DHT search engine</p>
    <form action="/torrents" method="get">
        <input type="text" name="q" placeholder="Search torrents...">
        <button type="submit">Search</button>
    </form>
    <p><em>Web UI under development. Use API at /api/v2/torrents</em></p>
</body>
</html>"#,
    )
}

pub async fn torrents_page_handler(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    Html("<h1>Torrents</h1><p>Search results will appear here</p>")
}

pub async fn torrent_page_handler(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    Html("<h1>Torrent Details</h1><p>Torrent information will appear here</p>")
}

pub async fn statistics_page_handler(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    Html("<h1>Statistics</h1><p>Statistics will appear here</p>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_homepage() {
        // Test basic handler response
        let response = homepage_handler(State(Arc::new(AppState {
            storage: Arc::new(
                crate::storage::rocksdb::RocksDbStorage::new(
                    tempfile::tempdir().unwrap().path(),
                    100
                )
                .unwrap(),
            ),
            search: Arc::new(
                crate::storage::search::SearchEngine::new(
                    tempfile::tempdir().unwrap().path(),
                    64
                )
                .unwrap(),
            ),
        })))
        .await
        .into_response();

        assert_eq!(response.status(), 200);
    }
}
