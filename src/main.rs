use clap::{Parser, Subcommand};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod api;
mod config;
mod crawler;
mod metrics;
mod storage;
mod web;

#[derive(Parser)]
#[command(name = "magnetico")]
#[command(version, about = "Autonomous BitTorrent DHT crawler and search engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the DHT crawler and metadata fetcher
    Crawl {
        /// Configuration file path
        #[arg(short, long)]
        config: Option<String>,
        
        /// Data directory
        #[arg(short, long)]
        data_dir: Option<String>,
        
        /// Listen address for DHT
        #[arg(short, long)]
        listen: Option<String>,
        
        /// Log level
        #[arg(short = 'v', long, default_value = "info")]
        log_level: String,
    },
    
    /// Run the web API and UI server
    Serve {
        /// Configuration file path
        #[arg(short, long)]
        config: Option<String>,
        
        /// Data directory
        #[arg(short, long)]
        data_dir: Option<String>,
        
        /// Listen address for API
        #[arg(short, long)]
        listen: Option<String>,
        
        /// Disable authentication
        #[arg(long)]
        no_auth: bool,
        
        /// Log level
        #[arg(short = 'v', long, default_value = "info")]
        log_level: String,
    },
    
    /// Run both crawler and server
    All {
        /// Configuration file path
        #[arg(short, long)]
        config: Option<String>,
        
        /// Data directory
        #[arg(short, long)]
        data_dir: Option<String>,
        
        /// Disable authentication
        #[arg(long)]
        no_auth: bool,
        
        /// Log level
        #[arg(short = 'v', long, default_value = "info")]
        log_level: String,
    },
    
    #[cfg(feature = "migration")]
    /// Migrate from SQLite to RocksDB
    Migrate {
        /// Source SQLite database URL (e.g., sqlite:///path/to/db.sqlite3)
        #[arg(long)]
        from: String,
        
        /// Destination RocksDB directory
        #[arg(long)]
        to: String,
        
        /// Verify migration after completion
        #[arg(long)]
        verify: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    
    // Initialize tracing based on log level
    let log_level = match &cli.command {
        Commands::Crawl { log_level, .. } => log_level,
        Commands::Serve { log_level, .. } => log_level,
        Commands::All { log_level, .. } => log_level,
        #[cfg(feature = "migration")]
        Commands::Migrate { .. } => "magnetico=info,tantivy=warn",  // Suppress tantivy spam during migration
    };
    
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    match cli.command {
        Commands::Crawl { config, data_dir, listen, .. } => {
            tracing::info!("Starting magnetico crawler");
            run_crawler(config, data_dir, listen).await?;
        }
        Commands::Serve { config, data_dir, listen, no_auth, .. } => {
            tracing::info!("Starting magnetico server");
            run_server(config, data_dir, listen, no_auth).await?;
        }
        Commands::All { config, data_dir, no_auth, .. } => {
            tracing::info!("Starting magnetico (crawler + server)");
            run_all(config, data_dir, no_auth).await?;
        }
        #[cfg(feature = "migration")]
        Commands::Migrate { from, to, verify } => {
            tracing::info!("Starting database migration");
            run_migration(&from, &to, verify).await?;
        }
    }
    
    Ok(())
}

async fn run_crawler(
    config: Option<String>,
    data_dir: Option<String>,
    listen: Option<String>,
) -> anyhow::Result<()> {
    let mut cfg = config::Config::load(config.as_deref())?;

    if let Some(dir) = data_dir {
        cfg.storage.data_dir = dir.into();
    }
    if let Some(addr) = listen {
        cfg.crawler.listen_addr = addr;
    }

    // Initialize storage
    let storage = Arc::new(storage::rocksdb::RocksDbStorage::new(
        &cfg.storage.data_dir.join("db"),
        cfg.storage.rocksdb_max_open_files,
    )?);

    let search = Arc::new(storage::search::SearchEngine::new(
        &cfg.storage.data_dir.join("search"),
        cfg.storage.tantivy_index_memory_mb,
    ).map_err(|e| anyhow::anyhow!("{}", e))?);

    // Create and run crawler
    let crawler = Arc::new(crawler::Crawler::new(&cfg.crawler, storage, search).await.map_err(|e| anyhow::anyhow!("{}", e))?);
    crawler.run().await;

    // Keep running
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down crawler");

    Ok(())
}

async fn run_server(
    config: Option<String>,
    data_dir: Option<String>,
    listen: Option<String>,
    no_auth: bool,
) -> anyhow::Result<()> {
    let mut cfg = config::Config::load(config.as_deref())?;

    if let Some(dir) = data_dir {
        cfg.storage.data_dir = dir.into();
    }
    if let Some(addr) = listen {
        cfg.server.listen_addr = addr;
    }
    if no_auth {
        cfg.auth.enabled = false;
    }

    // Initialize storage
    let storage = Arc::new(storage::rocksdb::RocksDbStorage::new(
        &cfg.storage.data_dir.join("db"),
        cfg.storage.rocksdb_max_open_files,
    )?);

    let search = Arc::new(storage::search::SearchEngine::new(
        &cfg.storage.data_dir.join("search"),
        cfg.storage.tantivy_index_memory_mb,
    ).map_err(|e| anyhow::anyhow!("{}", e))?);

    // Create API state
    let state = Arc::new(api::handlers::AppState { storage, search });

    // Create router
    let app = api::routes::create_router(state);

    // Start server
    let listener = tokio::net::TcpListener::bind(&cfg.server.listen_addr).await?;
    tracing::info!("Server listening on {}", cfg.server.listen_addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn run_all(
    config: Option<String>,
    data_dir: Option<String>,
    no_auth: bool,
) -> anyhow::Result<()> {
    let mut cfg = config::Config::load(config.as_deref())?;

    if let Some(dir) = data_dir {
        cfg.storage.data_dir = dir.into();
    }
    if no_auth {
        cfg.auth.enabled = false;
    }

    // Initialize storage (shared)
    let storage = Arc::new(storage::rocksdb::RocksDbStorage::new(
        &cfg.storage.data_dir.join("db"),
        cfg.storage.rocksdb_max_open_files,
    )?);

    let search = Arc::new(storage::search::SearchEngine::new(
        &cfg.storage.data_dir.join("search"),
        cfg.storage.tantivy_index_memory_mb,
    ).map_err(|e| anyhow::anyhow!("{}", e))?);

    // Start crawler
    let crawler_storage = Arc::clone(&storage);
    let crawler_search = Arc::clone(&search);
    let crawler_cfg = cfg.crawler.clone();
    tokio::spawn(async move {
        match crawler::Crawler::new(&crawler_cfg, crawler_storage, crawler_search).await {
            Ok(crawler) => {
                let crawler = Arc::new(crawler);
                crawler.run().await;
            }
            Err(e) => {
                tracing::error!("Failed to start crawler: {}", e);
            }
        }
    });

    // Start server
    let state = Arc::new(api::handlers::AppState { storage, search });
    let app = api::routes::create_router(state);

    let listener = tokio::net::TcpListener::bind(&cfg.server.listen_addr).await?;
    tracing::info!("Server listening on {}", cfg.server.listen_addr);

    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(feature = "migration")]
async fn run_migration(
    from: &str,
    to: &str,
    verify: bool,
) -> anyhow::Result<()> {
    use crate::storage::migration::Migrator;

    tracing::info!("Starting database migration");
    tracing::info!("Source: {}", from);
    tracing::info!("Target: {}", to);
    tracing::info!("Verify: {}", verify);

    let migrator = Migrator::new(from, to, verify).map_err(|e| anyhow::anyhow!("{}", e))?;
    let stats = migrator.migrate().await.map_err(|e| anyhow::anyhow!("{}", e))?;

    tracing::info!("Migration completed successfully!");
    tracing::info!("Torrents migrated: {}/{}", stats.migrated_torrents, stats.total_torrents);
    tracing::info!("Files migrated: {}/{}", stats.migrated_files, stats.total_files);
    
    if stats.errors > 0 {
        tracing::warn!("Migration completed with {} errors", stats.errors);
    }

    Ok(())
}

