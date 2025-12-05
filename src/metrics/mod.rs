use metrics::{counter, gauge, histogram};

// Crawler metrics
pub fn torrents_discovered() {
    counter!("magnetico_torrents_discovered_total").increment(1);
}

pub fn metadata_fetched() {
    counter!("magnetico_metadata_fetched_total").increment(1);
}

pub fn metadata_fetch_error(error_type: &str) {
    counter!("magnetico_metadata_fetch_errors_total", "error_type" => error_type.to_string())
        .increment(1);
}

pub fn dht_message_sent(msg_type: &str) {
    counter!("magnetico_dht_messages_sent_total", "msg_type" => msg_type.to_string())
        .increment(1);
}

pub fn dht_message_received(msg_type: &str) {
    counter!("magnetico_dht_messages_received_total", "msg_type" => msg_type.to_string())
        .increment(1);
}

pub fn dht_routing_table_size(size: usize) {
    gauge!("magnetico_dht_routing_table_size").set(size as f64);
}

pub fn metadata_fetch_duration(duration_secs: f64) {
    histogram!("magnetico_metadata_fetch_duration_seconds").record(duration_secs);
}

// API metrics
pub fn http_request(endpoint: &str, status: u16) {
    counter!(
        "magnetico_http_requests_total",
        "endpoint" => endpoint.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub fn http_request_duration(endpoint: &str, duration_secs: f64) {
    histogram!(
        "magnetico_http_request_duration_seconds",
        "endpoint" => endpoint.to_string()
    )
    .record(duration_secs);
}

pub fn search_duration(duration_secs: f64) {
    histogram!("magnetico_search_duration_seconds").record(duration_secs);
}

pub fn active_connections(count: usize) {
    gauge!("magnetico_active_connections").set(count as f64);
}

// Storage metrics
pub fn db_size_bytes(size: u64) {
    gauge!("magnetico_db_size_bytes").set(size as f64);
}

pub fn index_size_bytes(size: u64) {
    gauge!("magnetico_index_size_bytes").set(size as f64);
}

pub fn total_torrents(count: u64) {
    gauge!("magnetico_total_torrents").set(count as f64);
}

pub fn total_files(count: u64) {
    gauge!("magnetico_total_files").set(count as f64);
}

