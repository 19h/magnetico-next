use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch, DB};
use std::path::Path;
use std::sync::Arc;

use super::schema::{FileData, TorrentData};

const CF_TORRENTS: &str = "torrents";
const CF_FILES: &str = "files";
const CF_METADATA: &str = "metadata";

pub struct RocksDbStorage {
    db: Arc<DB>,
}

pub struct BatchWriter {
    batch: WriteBatch,
    torrents_cf: *const rocksdb::ColumnFamily,
    files_cf: *const rocksdb::ColumnFamily,
}

impl BatchWriter {
    pub fn add_torrent(
        &mut self,
        infohash: &[u8; 20],
        data: &TorrentData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let encoded = bincode::serialize(data)?;
        unsafe {
            self.batch.put_cf(&*self.torrents_cf, infohash, encoded);
        }
        Ok(())
    }

    pub fn add_file(
        &mut self,
        infohash: &[u8; 20],
        file_idx: u32,
        data: &FileData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(infohash);
        key.extend_from_slice(&file_idx.to_be_bytes());

        let encoded = bincode::serialize(data)?;
        unsafe {
            self.batch.put_cf(&*self.files_cf, &key, encoded);
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.batch.len()
    }
}

impl RocksDbStorage {
    pub fn new<P: AsRef<Path>>(path: P, max_open_files: i32) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_max_open_files(max_open_files);
        opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        
        // Optimize for bulk loading during migration
        opts.set_write_buffer_size(256 * 1024 * 1024); // 256 MB
        opts.set_max_write_buffer_number(6);
        opts.set_min_write_buffer_number_to_merge(2);
        
        // Increase parallelism for better write performance (use reasonable default)
        opts.increase_parallelism(std::cmp::min(16, std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4) as i32));
        
        // Set level compaction for better write throughput
        opts.set_level_compaction_dynamic_level_bytes(true);

        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_TORRENTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_FILES, Options::default()),
            ColumnFamilyDescriptor::new(CF_METADATA, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors)?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn create_batch(&self) -> BatchWriter {
        let torrents_cf = self.db.cf_handle(CF_TORRENTS).expect("Torrents CF should exist");
        let files_cf = self.db.cf_handle(CF_FILES).expect("Files CF should exist");
        
        BatchWriter {
            batch: WriteBatch::default(),
            torrents_cf: torrents_cf as *const _,
            files_cf: files_cf as *const _,
        }
    }

    pub fn commit_batch(&self, batch: BatchWriter) -> Result<(), rocksdb::Error> {
        self.db.write(batch.batch)
    }

    pub fn add_torrent(
        &self,
        infohash: &[u8; 20],
        data: &TorrentData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cf = self
            .db
            .cf_handle(CF_TORRENTS)
            .ok_or("Torrents CF not found")?;

        let encoded = bincode::serialize(data)?;
        self.db.put_cf(&cf, infohash, encoded)?;

        Ok(())
    }

    pub fn get_torrent(
        &self,
        infohash: &[u8; 20],
    ) -> Result<Option<TorrentData>, Box<dyn std::error::Error + Send + Sync>> {
        let cf = self
            .db
            .cf_handle(CF_TORRENTS)
            .ok_or("Torrents CF not found")?;

        match self.db.get_cf(&cf, infohash)? {
            Some(data) => {
                let torrent: TorrentData = bincode::deserialize(&data)?;
                Ok(Some(torrent))
            }
            None => Ok(None),
        }
    }

    pub fn torrent_exists(&self, infohash: &[u8; 20]) -> Result<bool, rocksdb::Error> {
        let cf = self
            .db
            .cf_handle(CF_TORRENTS)
            .expect("Torrents CF should exist");

        Ok(self.db.get_cf(&cf, infohash)?.is_some())
    }

    pub fn add_file(
        &self,
        infohash: &[u8; 20],
        file_idx: u32,
        data: &FileData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cf = self.db.cf_handle(CF_FILES).ok_or("Files CF not found")?;

        // Key: infohash + file_idx
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(infohash);
        key.extend_from_slice(&file_idx.to_be_bytes());

        let encoded = bincode::serialize(data)?;
        self.db.put_cf(&cf, &key, encoded)?;

        Ok(())
    }

    pub fn get_files(
        &self,
        infohash: &[u8; 20],
    ) -> Result<Vec<FileData>, Box<dyn std::error::Error + Send + Sync>> {
        let cf = self.db.cf_handle(CF_FILES).ok_or("Files CF not found")?;

        let mut files = Vec::new();
        let prefix = infohash;

        let iter = self.db.prefix_iterator_cf(&cf, prefix);
        for item in iter {
            let (key, value) = item?;

            // Check if key starts with our infohash
            if key.len() >= 20 && &key[..20] == infohash {
                let file: FileData = bincode::deserialize(&value)?;
                files.push(file);
            } else {
                break;
            }
        }

        Ok(files)
    }

    pub fn get_total_torrents(&self) -> Result<u64, rocksdb::Error> {
        let cf = self
            .db
            .cf_handle(CF_TORRENTS)
            .expect("Torrents CF should exist");

        let mut count = 0u64;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        for _item in iter {
            count += 1;
        }

        Ok(count)
    }

    pub fn get_db_size(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let path = self.db.path();
        let mut total_size = 0u64;

        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    total_size += metadata.len();
                }
            }
        }

        Ok(total_size)
    }

    pub fn set_metadata(&self, key: &str, value: &[u8]) -> Result<(), rocksdb::Error> {
        let cf = self
            .db
            .cf_handle(CF_METADATA)
            .expect("Metadata CF should exist");

        self.db.put_cf(&cf, key.as_bytes(), value)
    }

    pub fn get_metadata(&self, key: &str) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        let cf = self
            .db
            .cf_handle(CF_METADATA)
            .expect("Metadata CF should exist");

        self.db.get_cf(&cf, key.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_rocksdb_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::new(temp_dir.path(), 100).unwrap();

        let infohash = [1u8; 20];
        let torrent = TorrentData::new(infohash, "Test".to_string(), 1000, 12345, 1);

        storage.add_torrent(&infohash, &torrent).unwrap();
        let retrieved = storage.get_torrent(&infohash).unwrap().unwrap();

        assert_eq!(retrieved.name, "Test");
        assert_eq!(retrieved.total_size, 1000);
    }

    #[test]
    fn test_file_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDbStorage::new(temp_dir.path(), 100).unwrap();

        let infohash = [2u8; 20];
        let file = FileData::new("test.txt".to_string(), 500);

        storage.add_file(&infohash, 0, &file).unwrap();

        let files = storage.get_files(&infohash).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "test.txt");
    }
}
