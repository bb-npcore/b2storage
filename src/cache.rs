use anyhow::{Context, Result};
use rusqlite::{params, Connection};

use crate::client::FileInfo;

pub struct FileCache {
    conn: Connection,
}

impl FileCache {
    pub fn open() -> Result<Self> {
        let dir = dirs::home_dir()
            .context("No home dir")?
            .join(".b2storage");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("cache_v2.db");

        let conn = Connection::open(&path)?;

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA page_size=4096;
             PRAGMA cache_size=-8000;",  // 8MB cache
        )?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY,
                bucket_id TEXT NOT NULL,
                path TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_folder_bp ON folders(bucket_id, path);

            CREATE TABLE IF NOT EXISTS file_entries (
                id INTEGER PRIMARY KEY,
                folder_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                file_id TEXT,
                content_length INTEGER NOT NULL DEFAULT 0,
                upload_timestamp INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_fe_folder ON file_entries(folder_id);

            CREATE TABLE IF NOT EXISTS cache_meta (
                bucket_id TEXT PRIMARY KEY,
                last_sync INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0
            );",
        )?;

        // FTS5 for instant search (ignore error if already exists)
        let _ = conn.execute_batch(
            "CREATE VIRTUAL TABLE IF NOT EXISTS search_fts USING fts5(full_path, content='');"
        );

        Ok(Self { conn })
    }

    pub fn is_fresh(&self, bucket_id: &str, max_age_secs: u64) -> bool {
        let now = chrono::Utc::now().timestamp() as u64;
        let last: u64 = self.conn
            .query_row(
                "SELECT last_sync FROM cache_meta WHERE bucket_id=?1",
                params![bucket_id], |r| r.get(0),
            )
            .unwrap_or(0);
        last > 0 && (now - last) < max_age_secs
    }

    pub fn cache_age(&self, bucket_id: &str) -> u64 {
        let now = chrono::Utc::now().timestamp() as u64;
        let last: u64 = self.conn
            .query_row(
                "SELECT last_sync FROM cache_meta WHERE bucket_id=?1",
                params![bucket_id], |r| r.get(0),
            )
            .unwrap_or(0);
        if last == 0 { 0 } else { now - last }
    }

    pub fn file_count(&self, bucket_id: &str) -> u64 {
        self.conn
            .query_row(
                "SELECT file_count FROM cache_meta WHERE bucket_id=?1",
                params![bucket_id], |r| r.get(0),
            )
            .unwrap_or(0)
    }

    /// Store all files: normalized folders + FTS5 index + VACUUM
    pub fn store_all(&self, bucket_id: &str, files: &[FileInfo]) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;

        // Clear old data for this bucket
        tx.execute_batch(&format!(
            "DELETE FROM file_entries WHERE folder_id IN (SELECT id FROM folders WHERE bucket_id='{}');",
            bucket_id.replace('\'', "''")
        ))?;
        tx.execute("DELETE FROM folders WHERE bucket_id=?1", params![bucket_id])?;
        // Contentless FTS5 tables can't be DELETEd from; drop and recreate
        let _ = tx.execute_batch("DROP TABLE IF EXISTS search_fts;
            CREATE VIRTUAL TABLE search_fts USING fts5(full_path, content='');");

        // Prepare statements
        let mut insert_folder = tx.prepare(
            "INSERT OR IGNORE INTO folders(bucket_id, path) VALUES(?1, ?2)"
        )?;
        let mut get_folder_id = tx.prepare(
            "SELECT id FROM folders WHERE bucket_id=?1 AND path=?2"
        )?;
        let mut insert_file = tx.prepare(
            "INSERT INTO file_entries(folder_id, name, file_id, content_length, upload_timestamp)
             VALUES(?1, ?2, ?3, ?4, ?5)"
        )?;
        let mut insert_fts = tx.prepare(
            "INSERT INTO search_fts(rowid, full_path) VALUES(?1, ?2)"
        )?;

        for f in files {
            // Split path into folder + filename
            let (folder_path, file_name) = if let Some(pos) = f.file_name.rfind('/') {
                (&f.file_name[..=pos], &f.file_name[pos + 1..])
            } else {
                ("", f.file_name.as_str())
            };

            // Insert folder (deduped by UNIQUE index)
            insert_folder.execute(params![bucket_id, folder_path])?;
            let folder_id: i64 = get_folder_id.query_row(
                params![bucket_id, folder_path],
                |r| r.get(0),
            )?;

            // Insert file entry
            insert_file.execute(params![
                folder_id,
                file_name,
                f.file_id,
                f.content_length,
                f.upload_timestamp,
            ])?;

            // Get the rowid for FTS
            let entry_id = tx.last_insert_rowid();

            // Insert into FTS5 search index
            let _ = insert_fts.execute(params![entry_id, f.file_name]);
        }

        drop(insert_folder);
        drop(get_folder_id);
        drop(insert_file);
        drop(insert_fts);

        let now = chrono::Utc::now().timestamp();
        tx.execute(
            "INSERT OR REPLACE INTO cache_meta(bucket_id, last_sync, file_count)
             VALUES(?1, ?2, ?3)",
            params![bucket_id, now, files.len() as u64],
        )?;

        tx.commit()?;

        // Compact database
        let _ = self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
        let _ = self.conn.execute("VACUUM", []);

        Ok(())
    }

    /// List folder contents from normalized cache
    pub fn list_folder(&self, bucket_id: &str, prefix: &str) -> Result<Vec<FileInfo>> {
        let mut results = Vec::new();

        // Get files at this exact folder level
        let folder_id: Option<i64> = self.conn
            .query_row(
                "SELECT id FROM folders WHERE bucket_id=?1 AND path=?2",
                params![bucket_id, prefix],
                |r| r.get(0),
            )
            .ok();

        if let Some(fid) = folder_id {
            let mut stmt = self.conn.prepare(
                "SELECT name, file_id, content_length, upload_timestamp
                 FROM file_entries WHERE folder_id=?1 ORDER BY name"
            )?;
            let rows = stmt.query_map(params![fid], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, u64>(2)?,
                    r.get::<_, u64>(3)?,
                ))
            })?;
            for row in rows {
                let (name, file_id, size, ts) = row?;
                let full = format!("{}{}", prefix, name);
                results.push(FileInfo {
                    file_id,
                    file_name: full,
                    content_length: size,
                    content_type: None,
                    action: "upload".into(),
                    upload_timestamp: ts,
                });
            }
        }

        // Get subfolders: folders whose path starts with prefix
        // and has exactly one more segment
        let like = format!("{}%", prefix);
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT path FROM folders
             WHERE bucket_id=?1 AND path LIKE ?2 AND path != ?3
             ORDER BY path"
        )?;
        let rows = stmt.query_map(params![bucket_id, like, prefix], |r| {
            r.get::<_, String>(0)
        })?;

        let mut seen = std::collections::HashSet::new();
        for row in rows {
            let path = row?;
            let rest = &path[prefix.len()..];
            // Get first segment only (direct child folder)
            if let Some(slash) = rest.find('/') {
                let child_folder = format!("{}{}/", prefix, &rest[..slash]);
                if seen.insert(child_folder.clone()) {
                    results.push(FileInfo {
                        file_id: None,
                        file_name: child_folder,
                        content_length: 0,
                        content_type: None,
                        action: "folder".into(),
                        upload_timestamp: 0,
                    });
                }
            }
        }

        // Sort: folders first, then by name
        results.sort_by(|a, b| {
            let af = a.action == "folder";
            let bf = b.action == "folder";
            bf.cmp(&af).then(a.file_name.cmp(&b.file_name))
        });

        Ok(results)
    }

    /// Smart search: matching folders + files whose NAME matches (not path)
    pub fn search(&self, bucket_id: &str, query: &str) -> Result<Vec<FileInfo>> {
        let mut results = Vec::new();

        // 1. Find matching folders
        results.extend(self.search_folders(bucket_id, query)?);

        // 2. Find files whose FILENAME matches (not folder path)
        results.extend(self.search_files(bucket_id, query)?);

        Ok(results)
    }

    /// Search folders — finds ALL unique folders whose name segment matches query
    fn search_folders(&self, bucket_id: &str, query: &str) -> Result<Vec<FileInfo>> {
        let pattern = format!("%{}%", query);
        let query_lower = query.to_lowercase();

        // Get ALL folder paths containing the query (no limit on raw query)
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT path FROM folders
             WHERE bucket_id = ?1 AND path LIKE ?2 COLLATE NOCASE"
        )?;

        let rows = stmt.query_map(params![bucket_id, pattern], |r| {
            r.get::<_, String>(0)
        })?;

        // Extract the matching folder segment and deduplicate
        let mut seen = std::collections::HashSet::new();

        for row in rows {
            let path = row?;
            let parts: Vec<&str> = path.trim_end_matches('/').split('/').collect();
            for (i, part) in parts.iter().enumerate() {
                if part.to_lowercase().contains(&query_lower) {
                    let folder = format!("{}/", parts[..=i].join("/"));
                    seen.insert(folder);
                    break;
                }
            }
        }

        // Now get file count + size for each unique folder (limit final results)
        let mut results = Vec::new();
        let mut folders: Vec<String> = seen.into_iter().collect();
        folders.sort();

        for folder in folders.iter().take(200) {
            let like = format!("{}%", folder);
            let (count, total_size): (u64, u64) = self.conn.query_row(
                "SELECT COUNT(*), COALESCE(SUM(fe.content_length), 0)
                 FROM file_entries fe
                 JOIN folders f ON f.id = fe.folder_id
                 WHERE f.bucket_id = ?1 AND f.path LIKE ?2",
                params![bucket_id, like],
                |r| Ok((r.get(0)?, r.get(1)?)),
            ).unwrap_or((0, 0));

            results.push(FileInfo {
                file_id: None,
                file_name: folder.clone(),
                content_length: total_size,
                content_type: Some(format!("{} files", count)),
                action: "folder".into(),
                upload_timestamp: 0,
            });
        }

        Ok(results)
    }

    /// Search files only — returns files whose NAME matches (not folder path)
    fn search_files(&self, bucket_id: &str, query: &str) -> Result<Vec<FileInfo>> {
        let pattern = format!("%{}%", query);
        let mut stmt = self.conn.prepare(
            "SELECT f.path, fe.name, fe.file_id, fe.content_length, fe.upload_timestamp
             FROM file_entries fe
             JOIN folders f ON f.id = fe.folder_id
             WHERE f.bucket_id = ?1
             AND fe.name LIKE ?2 COLLATE NOCASE
             ORDER BY fe.name
             LIMIT 500"
        )?;

        let rows = stmt.query_map(params![bucket_id, pattern], |r| {
            let path: String = r.get(0)?;
            let name: String = r.get(1)?;
            Ok(FileInfo {
                file_name: format!("{}{}", path, name),
                file_id: r.get(2)?,
                content_length: r.get(3)?,
                upload_timestamp: r.get(4)?,
                content_type: None,
                action: "upload".into(),
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Add a single file to cache (after upload)
    pub fn add_file(&self, bucket_id: &str, file: &FileInfo) {
        let (folder_path, file_name) = if let Some(pos) = file.file_name.rfind('/') {
            (&file.file_name[..=pos], &file.file_name[pos + 1..])
        } else {
            ("", file.file_name.as_str())
        };

        let _ = self.conn.execute(
            "INSERT OR IGNORE INTO folders(bucket_id, path) VALUES(?1, ?2)",
            params![bucket_id, folder_path],
        );

        if let Ok(folder_id) = self.conn.query_row(
            "SELECT id FROM folders WHERE bucket_id=?1 AND path=?2",
            params![bucket_id, folder_path],
            |r| r.get::<_, i64>(0),
        ) {
            // Remove old entry if exists
            let _ = self.conn.execute(
                "DELETE FROM file_entries WHERE folder_id=?1 AND name=?2",
                params![folder_id, file_name],
            );
            let _ = self.conn.execute(
                "INSERT INTO file_entries(folder_id, name, file_id, content_length, upload_timestamp)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                params![folder_id, file_name, file.file_id, file.content_length, file.upload_timestamp],
            );
            // Update FTS
            let entry_id = self.conn.last_insert_rowid();
            let _ = self.conn.execute(
                "INSERT INTO search_fts(rowid, full_path) VALUES(?1, ?2)",
                params![entry_id, file.file_name],
            );
        }
    }

    /// Remove a file from cache
    pub fn remove_file(&self, bucket_id: &str, file_name: &str) {
        let (folder_path, name) = if let Some(pos) = file_name.rfind('/') {
            (&file_name[..=pos], &file_name[pos + 1..])
        } else {
            ("", file_name)
        };

        if let Ok(folder_id) = self.conn.query_row(
            "SELECT id FROM folders WHERE bucket_id=?1 AND path=?2",
            params![bucket_id, folder_path],
            |r| r.get::<_, i64>(0),
        ) {
            let _ = self.conn.execute(
                "DELETE FROM file_entries WHERE folder_id=?1 AND name=?2",
                params![folder_id, name],
            );
        }
    }

    /// Remove all files with prefix
    pub fn remove_prefix(&self, bucket_id: &str, prefix: &str) {
        let like = format!("{}%", prefix);
        let _ = self.conn.execute(
            "DELETE FROM file_entries WHERE folder_id IN
             (SELECT id FROM folders WHERE bucket_id=?1 AND path LIKE ?2)",
            params![bucket_id, like],
        );
    }

    /// Get existing file sizes for smart sync
    pub fn get_existing_sizes(&self, bucket_id: &str, prefix: &str) -> std::collections::HashMap<String, u64> {
        let like = format!("{}%", prefix);
        let mut map = std::collections::HashMap::new();
        if let Ok(mut stmt) = self.conn.prepare(
            "SELECT f.path || fe.name, fe.content_length
             FROM file_entries fe JOIN folders f ON f.id = fe.folder_id
             WHERE f.bucket_id=?1 AND f.path LIKE ?2"
        ) {
            if let Ok(rows) = stmt.query_map(params![bucket_id, like], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?))
            }) {
                for row in rows.flatten() {
                    map.insert(row.0, row.1);
                }
            }
        }
        map
    }
}
