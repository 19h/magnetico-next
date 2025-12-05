use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

pub mod codec;
pub mod dht;
pub mod indexing;
pub mod metadata;
pub mod sink;
pub mod transport;

pub use codec::Message;
pub use indexing::IndexingService;
pub use metadata::MetadataLeech;
pub use sink::MetadataSink;

use crate::config::CrawlerConfig;
use crate::storage::rocksdb::RocksDbStorage;
use crate::storage::search::SearchEngine;

#[derive(Debug, Clone)]
pub struct IndexingResult {
    pub infohash: [u8; 20],
    pub peers: Vec<SocketAddr>,
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub infohash: [u8; 20],
    pub name: String,
    pub total_size: u64,
    pub discovered_on: i64,
    pub files: Vec<File>,
}

#[derive(Debug, Clone)]
pub struct File {
    pub path: String,
    pub size: i64,
}

pub struct Crawler {
    indexing_service: Arc<IndexingService>,
    metadata_sink: Arc<MetadataSink>,
    storage: Arc<RocksDbStorage>,
    search: Arc<SearchEngine>,
}

impl Crawler {
    pub async fn new(
        config: &CrawlerConfig,
        storage: Arc<RocksDbStorage>,
        search: Arc<SearchEngine>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listen_addr: SocketAddr = config.listen_addr.parse()?;
        let interval = Duration::from_secs(config.indexing_interval_secs);

        // Create indexing service
        let (indexing_service, mut indexing_rx) =
            IndexingService::new(listen_addr, interval, config.max_neighbors).await?;

        let indexing_service = Arc::new(indexing_service);

        // Create metadata sink
        let deadline = Duration::from_secs(3);
        let mut metadata_sink = MetadataSink::new(deadline, config.max_metadata_fetchers);
        let mut metadata_rx = metadata_sink.take_drain().unwrap();

        let metadata_sink = Arc::new(metadata_sink);

        // Connect indexing to sink
        let sink_clone = Arc::clone(&metadata_sink);
        tokio::spawn(async move {
            while let Some(result) = indexing_rx.recv().await {
                sink_clone.sink(result).await;
            }
        });

        // Connect sink to storage
        let storage_clone = Arc::clone(&storage);
        let search_clone = Arc::clone(&search);
        tokio::spawn(async move {
            while let Some(metadata) = metadata_rx.recv().await {
                if let Err(e) = Self::store_metadata(&storage_clone, &search_clone, metadata).await
                {
                    error!("Failed to store metadata: {}", e);
                }
            }
        });

        Ok(Self {
            indexing_service,
            metadata_sink,
            storage,
            search,
        })
    }

    async fn store_metadata(
        storage: &RocksDbStorage,
        search: &SearchEngine,
        metadata: Metadata,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check if exists
        if storage.torrent_exists(&metadata.infohash)? {
            return Ok(());
        }

        // Store torrent
        let torrent_data = crate::storage::schema::TorrentData::new(
            metadata.infohash,
            metadata.name.clone(),
            metadata.total_size,
            metadata.discovered_on,
            metadata.files.len() as u32,
        );

        storage.add_torrent(&metadata.infohash, &torrent_data)?;

        // Index torrent name
        search.index_torrent(&metadata.infohash, &metadata.name)?;

        // Store and index files
        for (idx, file) in metadata.files.iter().enumerate() {
            let file_data = crate::storage::schema::FileData::new(file.path.clone(), file.size);
            storage.add_file(&metadata.infohash, idx as u32, &file_data)?;
            search.index_file(&metadata.infohash, &file.path)?;
        }

        info!(
            "Stored torrent: {} ({})",
            metadata.name,
            hex::encode(metadata.infohash)
        );

        Ok(())
    }

    pub async fn run(self: Arc<Self>) {
        info!("Starting crawler...");

        // Start indexing service
        let indexing_clone = Arc::clone(&self.indexing_service);
        tokio::spawn(async move {
            indexing_clone.run().await;
        });

        // Start metadata sink status logger
        let sink_clone = Arc::clone(&self.metadata_sink);
        tokio::spawn(async move {
            sink_clone.run_status_logger().await;
        });

        info!("Crawler is running");
    }
}

