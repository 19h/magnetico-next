use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub crawler: CrawlerConfig,
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub auth: AuthConfig,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrawlerConfig {
    pub enabled: bool,
    pub listen_addr: String,
    pub max_neighbors: usize,
    pub indexing_interval_secs: u64,
    pub max_metadata_fetchers: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub enabled: bool,
    pub listen_addr: String,
    pub cors_origins: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub rocksdb_max_open_files: i32,
    pub tantivy_index_memory_mb: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub enabled: bool,
    pub credentials_file: Option<PathBuf>,
    pub jwt_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub listen_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Pretty,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            crawler: CrawlerConfig {
                enabled: true,
                listen_addr: "0.0.0.0:6881".to_string(),
                max_neighbors: 10000,
                indexing_interval_secs: 1,
                max_metadata_fetchers: 500,
            },
            server: ServerConfig {
                enabled: true,
                listen_addr: "0.0.0.0:8080".to_string(),
                cors_origins: vec!["*".to_string()],
            },
            storage: StorageConfig {
                data_dir: dirs::data_local_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join("magnetico"),
                rocksdb_max_open_files: 1024,
                tantivy_index_memory_mb: 256,
            },
            auth: AuthConfig {
                enabled: true,
                credentials_file: dirs::config_dir()
                    .map(|p| p.join("magnetico").join("credentials")),
                jwt_secret: None,
            },
            metrics: MetricsConfig {
                enabled: true,
                listen_addr: "127.0.0.1:9090".to_string(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: LogFormat::Pretty,
            },
        }
    }
}

impl Config {
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        let mut config = Config::default();
        
        if let Some(path) = path {
            let contents = std::fs::read_to_string(path)?;
            config = toml::from_str(&contents)?;
        }
        
        Ok(config)
    }
}

