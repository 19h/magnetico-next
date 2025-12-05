#[cfg(feature = "migration")]
use indicatif::{ProgressBar, ProgressStyle};
#[cfg(feature = "migration")]
use rusqlite::{Connection, OpenFlags};
#[cfg(feature = "migration")]
use std::path::Path;
#[cfg(feature = "migration")]
use tempfile::TempDir;
#[cfg(feature = "migration")]
use tracing::{info, warn};

#[cfg(feature = "migration")]
use super::rocksdb::RocksDbStorage;
#[cfg(feature = "migration")]
use super::schema::{FileData, TorrentData};
#[cfg(feature = "migration")]
use super::search::SearchEngine;

#[cfg(feature = "migration")]
pub struct MigrationStats {
    pub total_torrents: u64,
    pub migrated_torrents: u64,
    pub total_files: u64,
    pub migrated_files: u64,
    pub errors: u64,
}

#[cfg(feature = "migration")]
impl MigrationStats {
    fn new() -> Self {
        Self {
            total_torrents: 0,
            migrated_torrents: 0,
            total_files: 0,
            migrated_files: 0,
            errors: 0,
        }
    }
}

#[cfg(feature = "migration")]
pub struct Migrator {
    sqlite_path: String,
    target_path: String,
    verify: bool,
    batch_size: usize,
    commit_every: usize, // Commit batch every N items
}

#[cfg(feature = "migration")]
impl Migrator {
    pub fn new(sqlite_url: &str, target_path: &str, verify: bool) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Parse SQLite URL (format: sqlite:///path/to/db.sqlite3)
        let sqlite_path = if let Some(path) = sqlite_url.strip_prefix("sqlite://") {
            path.to_string()
        } else {
            return Err("Invalid SQLite URL format. Expected: sqlite:///path/to/db.sqlite3".into());
        };

        Ok(Self {
            sqlite_path,
            target_path: target_path.to_string(),
            verify,
            batch_size: 1000,      // Fetch 1000 torrents at a time from SQLite
            commit_every: 5000,    // Commit RocksDB batch every 5000 items
        })
    }

    pub async fn migrate(&self) -> Result<MigrationStats, Box<dyn std::error::Error + Send + Sync>> {
        info!("Starting migration from {} to {}", self.sqlite_path, self.target_path);

        // Step 1: Validate source database
        info!("Step 1/5: Validating source database...");
        let conn = self.open_sqlite()?;
        let stats = self.get_source_stats(&conn)?;
        info!(
            "Source database contains {} torrents and {} files",
            stats.total_torrents, stats.total_files
        );

        // Step 2: Create temporary directory for migration
        info!("Step 2/5: Creating temporary migration directory...");
        let target_path = Path::new(&self.target_path);

        // Create all directories leading up to the target (but not the target itself)
        // This handles deep directory structures like /a/b/c/d/e/f/
        if let Some(parent) = target_path.parent() {
            info!("Creating all parent directories: {}", parent.display());
            std::fs::create_dir_all(parent)?;
            info!("All parent directories created successfully");
        } else {
            info!("Target is at filesystem root, no parent directories to create");
        }

        // Create temp directory in the same location as target for atomic rename
        let temp_dir = if let Some(parent) = target_path.parent() {
            info!("Creating temp directory in: {}", parent.display());
            TempDir::new_in(parent)?
        } else {
            info!("Creating temp directory in current directory (no parent)");
            TempDir::new_in(".")?
        };
        let temp_path = temp_dir.path();
        info!("Using temporary directory: {}", temp_path.display());

        // Verify temp directory exists and show its contents
        if temp_path.exists() {
            info!("Temp directory created successfully");
        } else {
            return Err("Temp directory was not created".into());
        }

        // Step 3: Migrate data
        info!("Step 3/5: Migrating data...");
        let mut migration_stats = self.migrate_data(&conn, temp_path).await?;
        migration_stats.total_torrents = stats.total_torrents;
        migration_stats.total_files = stats.total_files;

        // Step 4: Verify if requested
        if self.verify {
            info!("Step 4/5: Verifying migration...");
            self.verify_migration(&conn, temp_path, &migration_stats)?;
        } else {
            info!("Step 4/5: Skipping verification (not requested)");
        }

        // Step 5: Atomic switchover
        info!("Step 5/5: Performing atomic switchover...");
        self.atomic_switchover(temp_path)?;

        info!("Migration completed successfully!");
        info!(
            "Migrated {} torrents and {} files ({} errors)",
            migration_stats.migrated_torrents, migration_stats.migrated_files, migration_stats.errors
        );

        Ok(migration_stats)
    }

    fn open_sqlite(&self) -> Result<Connection, Box<dyn std::error::Error + Send + Sync>> {
        let conn = Connection::open_with_flags(
            &self.sqlite_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;

        // Optimize SQLite for faster reads
        conn.execute_batch(
            "PRAGMA cache_size = -262144;  -- 256 MB cache
             PRAGMA temp_store = MEMORY;    -- Use memory for temp tables
             PRAGMA mmap_size = 536870912;  -- 512 MB mmap
             PRAGMA page_size = 4096;       -- 4KB pages"
        )?;

        // Check schema version
        let user_version: i64 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        info!("SQLite schema version: {}", user_version);

        if user_version < 3 {
            warn!("Old schema version detected. Migration may be incomplete.");
        }

        Ok(conn)
    }

    fn get_source_stats(&self, conn: &Connection) -> Result<MigrationStats, Box<dyn std::error::Error + Send + Sync>> {
        let total_torrents: i64 = conn.query_row(
            "SELECT COUNT(*) FROM torrents",
            [],
            |row| row.get(0),
        )?;

        let total_files: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files",
            [],
            |row| row.get(0),
        )?;

        Ok(MigrationStats {
            total_torrents: total_torrents as u64,
            total_files: total_files as u64,
            migrated_torrents: 0,
            migrated_files: 0,
            errors: 0,
        })
    }

    async fn migrate_data(
        &self,
        conn: &Connection,
        temp_path: &Path,
    ) -> Result<MigrationStats, Box<dyn std::error::Error + Send + Sync>> {
        let mut stats = MigrationStats::new();

        // Create storage and search in temp directory
        let storage = RocksDbStorage::new(temp_path.join("db"), 1024)?;
        let search = SearchEngine::new(temp_path.join("search"), 256)?;

        // Get total counts for progress bar
        let total_torrents: i64 = conn.query_row(
            "SELECT COUNT(*) FROM torrents",
            [],
            |row| row.get(0),
        )?;
        
        let total_files: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files",
            [],
            |row| row.get(0),
        )?;

        // Create progress bar showing torrent progress (like original)
        // Files are processed per-torrent so this gives accurate completion %
        let pb = ProgressBar::new(total_torrents as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} torrents ({percent}%) | {per_sec} | ETA: {eta} | {msg}")
                .unwrap()
                .progress_chars("#>-"),
        );

        // Enable steady tick to keep progress bar visible above logs
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        // Pre-count files per torrent (this is MUCH faster than doing it one-by-one)
        info!("Pre-computing file counts...");
        let mut file_counts = std::collections::HashMap::new();
        {
            let mut stmt = conn.prepare(
                "SELECT torrent_id, COUNT(*) FROM files GROUP BY torrent_id"
            )?;
            let counts = stmt.query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })?;
            for count_result in counts {
                if let Ok((torrent_id, count)) = count_result {
                    file_counts.insert(torrent_id, count as u32);
                }
            }
        }
        info!("Found file counts for {} torrents", file_counts.len());

        // Create batch writer
        let mut batch = storage.create_batch();
        let mut items_in_batch = 0;
        let mut last_commit_time = std::time::Instant::now();

        // Migrate torrents in batches
        let mut offset = 0i64;

        loop {
            let mut stmt = conn.prepare(
                "SELECT id, info_hash, name, total_size, discovered_on, modified_on 
                 FROM torrents 
                 ORDER BY id 
                 LIMIT ? OFFSET ?",
            )?;

            let torrents = stmt.query_map([self.batch_size as i64, offset], |row| {
                let id: i64 = row.get(0)?;
                let info_hash: Vec<u8> = row.get(1)?;

                // Get raw bytes from TEXT column to handle invalid UTF-8
                let raw_value = row.get_ref(2)?;
                let name_bytes = raw_value.as_bytes().unwrap_or(&[]);
                let name = String::from_utf8_lossy(name_bytes).to_string();

                let total_size: i64 = row.get(3)?;
                let discovered_on: i64 = row.get(4)?;
                let modified_on: i64 = row.get(5)?;

                Ok((id, info_hash, name, total_size, discovered_on, modified_on))
            })?;

            let mut batch_count = 0;

            for torrent_result in torrents {
                match torrent_result {
                    Ok((torrent_id, info_hash, name, total_size, discovered_on, modified_on)) => {
                        // Validate infohash
                        if info_hash.len() != 20 {
                            warn!("Skipping torrent with invalid infohash length: {}", name);
                            stats.errors += 1;
                            continue;
                        }

                        let mut infohash_arr = [0u8; 20];
                        infohash_arr.copy_from_slice(&info_hash);

                        // Get file count from pre-computed map
                        let n_files = file_counts.get(&torrent_id).copied().unwrap_or(0);

                        // Create torrent data
                        let torrent_data = TorrentData {
                            info_hash: infohash_arr,
                            name: name.clone(),
                            total_size: total_size as u64,
                            discovered_on,
                            modified_on,
                            n_files,
                        };

                        // Add to batch
                        if let Err(e) = batch.add_torrent(&infohash_arr, &torrent_data) {
                            warn!("Failed to batch torrent {}: {}", name, e);
                            stats.errors += 1;
                            continue;
                        }
                        items_in_batch += 1;

                        // Index in Tantivy silently (log errors only occasionally)
                        if let Err(e) = search.index_torrent(&infohash_arr, &name) {
                            if stats.errors % 100 == 0 {  // Log every 100th error only
                                warn!("Failed to index torrent {}: {} (suppressing similar errors)", name, e);
                            }
                            stats.errors += 1;
                        }

                        stats.migrated_torrents += 1;
                        pb.inc(1);

                        // Update status message with file count
                        pb.set_message(format!("{} files", stats.migrated_files));

                        // Migrate files for this torrent (adds to batch)
                        let file_stats = self.migrate_files_batched(
                            conn, 
                            torrent_id, 
                            &infohash_arr, 
                            &mut batch,
                            &search,
                            &mut items_in_batch,
                            &pb
                        )?;
                        stats.migrated_files += file_stats;

                        batch_count += 1;

                        // Commit batch periodically
                        if items_in_batch >= self.commit_every {
                            let commit_start = std::time::Instant::now();
                            storage.commit_batch(batch)?;
                            let commit_duration = commit_start.elapsed();
                            
                            // Show commit stats every 30 seconds
                            if last_commit_time.elapsed().as_secs() >= 30 {
                                info!(
                                    "Progress: {}/{} torrents, {}/{} files, commit took {:?}",
                                    stats.migrated_torrents, total_torrents,
                                    stats.migrated_files, total_files,
                                    commit_duration
                                );
                                last_commit_time = std::time::Instant::now();
                            }
                            
                            batch = storage.create_batch();
                            items_in_batch = 0;
                        }
                    }
                    Err(e) => {
                        warn!("Error reading torrent: {}", e);
                        stats.errors += 1;
                    }
                }
            }

            if batch_count == 0 {
                break; // No more torrents
            }

            offset += self.batch_size as i64;
        }

        // Commit any remaining items
        if items_in_batch > 0 {
            info!("Committing final batch of {} items", items_in_batch);
            storage.commit_batch(batch)?;
        }

        pb.finish_with_message("Migration complete");

        Ok(stats)
    }

    fn migrate_files_batched(
        &self,
        conn: &Connection,
        torrent_id: i64,
        infohash: &[u8; 20],
        batch: &mut super::rocksdb::BatchWriter,
        search: &SearchEngine,
        items_in_batch: &mut usize,
        pb: &ProgressBar,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut stmt = conn.prepare(
            "SELECT size, path FROM files WHERE torrent_id = ? ORDER BY id",
        )?;

        let files = stmt.query_map([torrent_id], |row| {
            let size: i64 = row.get(0)?;

            // Get raw bytes from TEXT column to handle invalid UTF-8
            let raw_value = row.get_ref(1)?;
            let path_bytes = raw_value.as_bytes().unwrap_or(&[]);
            let path = String::from_utf8_lossy(path_bytes).to_string();

            Ok((size, path))
        })?;

        let mut file_count = 0u64;
        let mut file_idx = 0u32;

        for file_result in files {
            match file_result {
                Ok((size, path)) => {
                    let file_data = FileData { path: path.clone(), size };

                    // Add to batch
                    if let Err(e) = batch.add_file(infohash, file_idx, &file_data) {
                        warn!("Failed to batch file {}: {}", path, e);
                        continue;
                    }
                    *items_in_batch += 1;

                    // Index in Tantivy silently (errors are not critical during migration)
                    let _ = search.index_file(infohash, &path);

                    file_count += 1;
                    file_idx += 1;
                }
                Err(e) => {
                    warn!("Error reading file: {}", e);
                }
            }
        }

        Ok(file_count)
    }

    fn verify_migration(
        &self,
        conn: &Connection,
        temp_path: &Path,
        stats: &MigrationStats,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Verifying migration...");

        // Open migrated storage
        let storage = RocksDbStorage::new(temp_path.join("db"), 1024)?;

        // Verify torrent count
        let migrated_count = storage.get_total_torrents()?;
        info!(
            "Torrent count: source={}, migrated={}",
            stats.total_torrents, migrated_count
        );

        if migrated_count != stats.migrated_torrents {
            return Err(format!(
                "Torrent count mismatch: expected {}, got {}",
                stats.migrated_torrents, migrated_count
            )
            .into());
        }

        // Spot check: verify 10 random torrents
        let mut stmt = conn.prepare(
            "SELECT info_hash, name FROM torrents ORDER BY RANDOM() LIMIT 10",
        )?;

        let sample_torrents = stmt.query_map([], |row| {
            let info_hash: Vec<u8> = row.get(0)?;

            // Get raw bytes from TEXT column to handle invalid UTF-8
            let raw_value = row.get_ref(1)?;
            let name_bytes = raw_value.as_bytes().unwrap_or(&[]);
            let name = String::from_utf8_lossy(name_bytes).to_string();

            Ok((info_hash, name))
        })?;

        for torrent_result in sample_torrents {
            match torrent_result {
                Ok((info_hash, name)) => {
                    if info_hash.len() != 20 {
                        continue;
                    }

                    let mut infohash_arr = [0u8; 20];
                    infohash_arr.copy_from_slice(&info_hash);

                    match storage.get_torrent(&infohash_arr)? {
                        Some(torrent) => {
                            if torrent.name != name {
                                return Err(format!(
                                    "Name mismatch for {}: expected '{}', got '{}'",
                                    hex::encode(info_hash),
                                    name,
                                    torrent.name
                                )
                                .into());
                            }
                        }
                        None => {
                            return Err(format!(
                                "Torrent not found in migrated DB: {}",
                                hex::encode(info_hash)
                            )
                            .into());
                        }
                    }
                }
                Err(_) => continue,
            }
        }

        info!("Verification passed!");
        Ok(())
    }

    fn atomic_switchover(&self, temp_path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let target = Path::new(&self.target_path);
        info!("Starting atomic switchover from {} to {}", temp_path.display(), target.display());

        // Create parent directory if needed
        if let Some(parent) = target.parent() {
            info!("Ensuring parent directory exists: {}", parent.display());
            std::fs::create_dir_all(parent)?;
        }

        // If target exists, back it up
        if target.exists() {
            let backup_path = format!("{}.backup", self.target_path);
            info!("Backing up existing data to {}", backup_path);
            std::fs::rename(target, &backup_path)?;
        }

        // Move temp to target
        info!("Moving {} to {}", temp_path.display(), target.display());
        std::fs::rename(temp_path, target)?;
        info!("Successfully moved temp directory to target");

        // Verify the target exists
        if target.exists() {
            info!("Target directory created successfully: {}", target.display());
            let entries = std::fs::read_dir(target)?;
            let entry_count = entries.count();
            info!("Target contains {} entries", entry_count);
        } else {
            return Err(format!("Target directory was not created: {}", target.display()).into());
        }

        info!("Switchover complete");
        Ok(())
    }
}

#[cfg(all(test, feature = "migration"))]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_migrator_creation() {
        let result = Migrator::new("sqlite:///test.db", "/tmp/target", false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_sqlite_url() {
        let result = Migrator::new("invalid://test.db", "/tmp/target", false);
        assert!(result.is_err());
    }
}
