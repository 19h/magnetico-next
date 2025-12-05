use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy};
use std::path::Path;
use std::sync::Arc;

const TORRENT_INDEX_DIR: &str = "torrents_idx";
const FILE_INDEX_DIR: &str = "files_idx";

pub struct SearchEngine {
    torrent_index: Arc<Index>,
    torrent_reader: IndexReader,
    torrent_writer: Arc<std::sync::Mutex<IndexWriter>>,
    torrent_schema: Schema,
    
    file_index: Arc<Index>,
    file_reader: IndexReader,
    file_writer: Arc<std::sync::Mutex<IndexWriter>>,
    file_schema: Schema,
}

impl SearchEngine {
    pub fn new<P: AsRef<Path>>(
        base_path: P,
        memory_budget_mb: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let base_path = base_path.as_ref();

        // Create torrent index
        let mut torrent_schema_builder = Schema::builder();
        torrent_schema_builder.add_bytes_field("infohash", STORED);
        torrent_schema_builder.add_text_field("name", TEXT | STORED);
        let torrent_schema = torrent_schema_builder.build();

        let torrent_index_path = base_path.join(TORRENT_INDEX_DIR);
        std::fs::create_dir_all(&torrent_index_path)?;

        let torrent_index = if torrent_index_path.join("meta.json").exists() {
            Index::open_in_dir(&torrent_index_path)?
        } else {
            Index::create_in_dir(&torrent_index_path, torrent_schema.clone())?
        };

        let torrent_writer = torrent_index.writer(memory_budget_mb * 1024 * 1024)?;
        let torrent_reader = torrent_index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        // Create file index
        let mut file_schema_builder = Schema::builder();
        file_schema_builder.add_bytes_field("infohash", STORED);
        file_schema_builder.add_text_field("path", TEXT | STORED);
        let file_schema = file_schema_builder.build();

        let file_index_path = base_path.join(FILE_INDEX_DIR);
        std::fs::create_dir_all(&file_index_path)?;

        let file_index = if file_index_path.join("meta.json").exists() {
            Index::open_in_dir(&file_index_path)?
        } else {
            Index::create_in_dir(&file_index_path, file_schema.clone())?
        };

        let file_writer = file_index.writer(memory_budget_mb * 1024 * 1024)?;
        let file_reader = file_index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        Ok(Self {
            torrent_index: Arc::new(torrent_index),
            torrent_reader,
            torrent_writer: Arc::new(std::sync::Mutex::new(torrent_writer)),
            torrent_schema,
            file_index: Arc::new(file_index),
            file_reader,
            file_writer: Arc::new(std::sync::Mutex::new(file_writer)),
            file_schema,
        })
    }

    pub fn index_torrent(
        &self,
        infohash: &[u8; 20],
        name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let infohash_field = self.torrent_schema.get_field("infohash").unwrap();
        let name_field = self.torrent_schema.get_field("name").unwrap();

        let mut writer = self.torrent_writer.lock().unwrap();
        writer.add_document(doc!(
            infohash_field => infohash.to_vec(),
            name_field => name
        ))?;
        writer.commit()?;

        Ok(())
    }

    pub fn index_file(
        &self,
        infohash: &[u8; 20],
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let infohash_field = self.file_schema.get_field("infohash").unwrap();
        let path_field = self.file_schema.get_field("path").unwrap();

        let mut writer = self.file_writer.lock().unwrap();
        writer.add_document(doc!(
            infohash_field => infohash.to_vec(),
            path_field => path
        ))?;
        writer.commit()?;

        Ok(())
    }

    pub fn search_torrents(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<[u8; 20]>, Box<dyn std::error::Error + Send + Sync>> {
        let searcher = self.torrent_reader.searcher();
        let name_field = self.torrent_schema.get_field("name").unwrap();

        let query_parser = QueryParser::for_index(&self.torrent_index, vec![name_field]);
        let query = query_parser.parse_query(query)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let infohash_field = self.torrent_schema.get_field("infohash").unwrap();
        let mut results = Vec::new();

        for (_score, doc_address) in top_docs {
            let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            if let Some(bytes_value) = doc.get_first(infohash_field) {
                if let Some(bytes) = bytes_value.as_bytes() {
                    if bytes.len() == 20 {
                        let mut infohash = [0u8; 20];
                        infohash.copy_from_slice(bytes);
                        results.push(infohash);
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn search_files(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<[u8; 20]>, Box<dyn std::error::Error + Send + Sync>> {
        let searcher = self.file_reader.searcher();
        let path_field = self.file_schema.get_field("path").unwrap();

        let query_parser = QueryParser::for_index(&self.file_index, vec![path_field]);
        let query = query_parser.parse_query(query)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let infohash_field = self.file_schema.get_field("infohash").unwrap();
        let mut results = Vec::new();

        for (_score, doc_address) in top_docs {
            let doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            if let Some(bytes_value) = doc.get_first(infohash_field) {
                if let Some(bytes) = bytes_value.as_bytes() {
                    if bytes.len() == 20 {
                        let mut infohash = [0u8; 20];
                        infohash.copy_from_slice(bytes);
                        results.push(infohash);
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn combined_search(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<[u8; 20]>, Box<dyn std::error::Error + Send + Sync>> {
        let mut torrent_results = self.search_torrents(query, limit)?;
        let file_results = self.search_files(query, limit)?;

        // Combine and deduplicate
        for infohash in file_results {
            if !torrent_results.contains(&infohash) {
                torrent_results.push(infohash);
                if torrent_results.len() >= limit {
                    break;
                }
            }
        }

        Ok(torrent_results)
    }

    pub fn get_index_size(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Note: In tantivy 0.22, we can't easily get the directory path from ManagedDirectory
        // For now, return 0 or estimate based on other metrics
        // TODO: Store the base paths in the struct if we need accurate size reporting
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_search_engine() {
        let temp_dir = TempDir::new().unwrap();
        let engine = SearchEngine::new(temp_dir.path(), 64).unwrap();

        let infohash = [1u8; 20];
        engine.index_torrent(&infohash, "Test Torrent").unwrap();

        let results = engine.search_torrents("test", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], infohash);
    }

    #[test]
    fn test_file_search() {
        let temp_dir = TempDir::new().unwrap();
        let engine = SearchEngine::new(temp_dir.path(), 64).unwrap();

        let infohash = [2u8; 20];
        engine
            .index_file(&infohash, "document.pdf")
            .unwrap();

        let results = engine.search_files("document", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], infohash);
    }
}
