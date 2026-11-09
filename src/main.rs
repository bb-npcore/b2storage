#![windows_subsystem = "windows"]

mod cache;
mod client;
mod config;

use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use client::{B2Client, Bucket, FileInfo, ProgressFn};
use eframe::egui;
use futures::StreamExt;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;

// ── Messages from background tasks ──

enum Msg {
    Authorized(config::AuthCache),
    BucketsLoaded(Vec<Bucket>),
    FilesLoaded(Vec<FileInfo>),
    Progress { id: usize, done: u64, total: u64 },
    TransferDone { id: usize, name: String },
    Deleted(String),
    FolderCreated(String),
    BucketCreated,
    BucketDeleted,
    Error(String),
    SearchResults { files: Vec<FileInfo>, scanned: usize },
    SearchDone { count: usize, all_files: Vec<FileInfo> },
    FolderProgress { id: usize, done_files: usize, total_files: usize, done_bytes: u64, total_bytes: u64, current: String },
    FolderDone { id: usize, name: String, done_files: usize, total_files: usize, failed: usize, failed_files: Vec<String> },
    CacheSynced { bucket_id: String, count: usize },
    DestFolders(Vec<String>),
    DownloadFolder(String, PathBuf), // (remote_prefix, local_dest_dir)
    FilesPicked(Vec<PathBuf>),
    FolderPicked(PathBuf),
    SavePicked(PathBuf, String),
}

// ── Transfer tracking ──

struct Transfer {
    id: usize,
    name: String,
    is_upload: bool,
    done: u64,
    total: u64,
    complete: bool,
    started_at: std::time::Instant,
    done_files: usize,
    total_files: usize,
    current_file: String,
}

// ── Log system ──

struct LogEntry {
    time: chrono::DateTime<chrono::Local>,
    message: String,
    level: LogLevel,
}

#[derive(Clone, Copy, PartialEq)]
enum LogLevel {
    Info,
    Success,
    Error,
    Progress,
}

// ── App ──

struct App {
    rt: tokio::runtime::Runtime,
    tx: mpsc::Sender<Msg>,
    rx: mpsc::Receiver<Msg>,

    // Auth
    key_id: String,
    app_key: String,
    client: Option<B2Client>,
    authenticating: bool,
    /// False until credentials exist, which shows the setup screen instead of the app.
    setup_done: bool,

    // Data
    buckets: Vec<Bucket>,
    buckets_loaded: bool,
    files: Vec<FileInfo>,
    selected_bucket: Option<usize>,
    current_prefix: String,
    nav_stack: Vec<String>,
    selected_files: HashSet<String>,

    // SQLite cache
    db: Option<cache::FileCache>,
    cache_syncing: bool,
    needs_refresh: bool,
    last_search: String, // saved search query for "back to results"

    // Transfers
    transfers: Vec<Transfer>,
    next_id: usize,

    // Logs
    logs: Vec<LogEntry>,

    // UI state
    show_create_bucket: bool,
    new_bucket_name: String,
    new_bucket_public: bool,
    show_create_folder: bool,
    new_folder_name: String,
    loading: bool,
    searching: bool,
    search: String,
    status: String,
    error: Option<String>,
    show_delete_confirm: bool,
    pending_upload: Option<PathBuf>,
    show_upload_picker: bool,
    context_menu_installed: bool,
    // Right-click destination picker
    queued_uploads: Vec<PathBuf>,
    show_dest_picker: bool,
    dest_folders: Vec<String>,  // folder list for destination picker
    dest_prefix: String,        // currently browsed prefix in picker
    bring_to_front: bool,
    minimize_after: bool,
}

impl App {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let (tx, rx) = mpsc::channel();
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

        let mut app = Self {
            rt,
            tx,
            rx,
            key_id: String::new(),
            app_key: String::new(),
            client: None,
            authenticating: false,
            setup_done: false,
            buckets: Vec::new(),
            buckets_loaded: false,
            files: Vec::new(),
            selected_bucket: None,
            current_prefix: String::new(),
            nav_stack: Vec::new(),
            selected_files: HashSet::new(),
            db: cache::FileCache::open().ok(),
            cache_syncing: false,
            needs_refresh: false,
            last_search: String::new(),
            transfers: Vec::new(),
            next_id: 0,
            logs: Vec::new(),
            show_create_bucket: false,
            new_bucket_name: String::new(),
            new_bucket_public: false,
            show_create_folder: false,
            new_folder_name: String::new(),
            loading: false,
            searching: false,
            search: String::new(),
            status: "Not connected".into(),
            error: None,
            show_delete_confirm: false,
            pending_upload: None,
            show_upload_picker: false,
            context_menu_installed: is_context_menu_installed(),
            queued_uploads: Vec::new(),
            show_dest_picker: false,
            dest_folders: Vec::new(),
            dest_prefix: String::new(),
            bring_to_front: false,
            minimize_after: false,
        };

        // Credentials live in ~/.b2storage/config.json, never in the binary. `strip = true`
        // removes symbols but not string literals, so anything baked in here would ship
        // readable to everyone who gets the exe.
        if let Ok(cfg) = config::load_config() {
            if !cfg.key_id.is_empty() && !cfg.app_key.is_empty() {
                app.key_id = cfg.key_id;
                app.app_key = cfg.app_key;
                app.setup_done = true;
                app.spawn_auth();
            }
        }

        app
    }

    fn poll_upload_queue(&mut self) {
        if self.client.is_none() || self.selected_bucket.is_none() {
            return;
        }
        let queue = upload_queue_path();
        if let Ok(content) = std::fs::read_to_string(&queue) {
            let _ = std::fs::remove_file(&queue);
            for line in content.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                // Format: path|destination
                let (path_str, dest) = if let Some(pos) = line.rfind('|') {
                    (&line[..pos], &line[pos + 1..])
                } else {
                    (line, "")
                };
                let path = PathBuf::from(path_str);
                if !path.exists() {
                    continue;
                }
                self.log(
                    LogLevel::Info,
                    format!("Queued: {} -> /{}", path.display(), dest),
                );
                let saved_prefix = self.current_prefix.clone();
                self.current_prefix = dest.to_string();
                if path.is_dir() {
                    self.upload_folder(path);
                } else {
                    self.upload_paths(vec![path]);
                }
                self.current_prefix = saved_prefix;
            }
        }
    }

    fn load_dest_folders(&self) {
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();
        let prefix = self.dest_prefix.clone();
        let tx = self.tx.clone();
        self.rt.spawn(async move {
            let p = if prefix.is_empty() { None } else { Some(prefix.as_str()) };
            match client.list_files(&bucket_id, p, Some("/")).await {
                Ok(files) => {
                    let folders: Vec<String> = files
                        .iter()
                        .filter(|f| f.action == "folder")
                        .map(|f| f.file_name.clone())
                        .collect();
                    tx.send(Msg::DestFolders(folders)).ok();
                }
                Err(_) => {
                    tx.send(Msg::DestFolders(Vec::new())).ok();
                }
            }
        });
    }

    fn next_transfer_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn log(&mut self, level: LogLevel, msg: impl Into<String>) {
        self.logs.push(LogEntry {
            time: chrono::Local::now(),
            message: msg.into(),
            level,
        });
    }

    fn process_messages(&mut self) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                Msg::Authorized(auth) => {
                    self.authenticating = false;
                    self.log(LogLevel::Success, format!("Authenticated: {}", auth.account_id));
                    self.client = Some(B2Client::new(auth, self.key_id.clone(), self.app_key.clone()));
                    self.status = "Connected".into();
                    self.error = None;
                    self.spawn_load_buckets();
                }
                Msg::BucketsLoaded(buckets) => {
                    self.log(LogLevel::Info, format!("Loaded {} bucket(s)", buckets.len()));
                    self.buckets = buckets;
                    self.buckets_loaded = true;
                    self.loading = false;
                    self.status = format!("{} bucket(s)", self.buckets.len());
                }
                Msg::FilesLoaded(files) => {
                    let folders = files.iter().filter(|f| f.action == "folder").count();
                    let file_count = files.len() - folders;
                    self.log(
                        LogLevel::Info,
                        format!("Listed: {} file(s), {} folder(s)", file_count, folders),
                    );
                    self.files = files;
                    self.selected_files.clear();
                    self.loading = false;
                    self.needs_refresh = false;
                }
                Msg::CacheSynced { bucket_id, count } => {
                    self.cache_syncing = false;
                    self.log(
                        LogLevel::Success,
                        format!("Cache synced: {} files indexed", count),
                    );
                    // Reload current view from cache
                    self.load_from_cache();
                }
                Msg::SearchResults { files, scanned } => {
                    self.files = files;
                    self.status = format!(
                        "Searching... {} matches, {} files scanned",
                        self.files.len(),
                        scanned
                    );
                }
                Msg::SearchDone { count, all_files } => {
                    self.loading = false;
                    self.status = format!(
                        "Search done: {} matches ({} total files)",
                        self.files.len(),
                        count
                    );
                    // Store full file list in SQLite cache
                    if let (Some(ref db), Some(idx)) = (&self.db, self.selected_bucket) {
                        if let Some(bucket) = self.buckets.get(idx) {
                            let _ = db.store_all(&bucket.bucket_id, &all_files);
                        }
                    }
                }
                Msg::Progress { id, done, total } => {
                    if let Some(t) = self.transfers.iter_mut().find(|t| t.id == id) {
                        t.done = done;
                        t.total = total;
                    }
                }
                Msg::FolderProgress { id, done_files, total_files, done_bytes, total_bytes, current } => {
                    if let Some(t) = self.transfers.iter_mut().find(|t| t.id == id) {
                        t.done = done_bytes;
                        t.total = total_bytes;
                        t.done_files = done_files;
                        t.total_files = total_files;
                        t.current_file = current;
                    }
                }
                Msg::FolderDone { id, name, done_files, total_files, failed, failed_files } => {
                    let is_up = self.transfers.iter().find(|t| t.id == id).map(|t| t.is_upload).unwrap_or(true);
                    let action = if is_up { "Uploaded" } else { "Downloaded" };
                    if let Some(t) = self.transfers.iter_mut().find(|t| t.id == id) {
                        t.complete = true;
                        t.done = t.total;
                        let elapsed = t.started_at.elapsed().as_secs_f64();
                        let speed = if elapsed > 0.0 { t.total as f64 / elapsed } else { 0.0 };
                        let mut msg = format!(
                            "{} folder: {} ({}/{} files, {}, {}/s)",
                            action, name, done_files, total_files,
                            ByteSize(t.total), ByteSize(speed as u64)
                        );
                        if failed > 0 {
                            msg.push_str(&format!(", {} failed", failed));
                        }
                        self.log(if failed > 0 { LogLevel::Error } else { LogLevel::Success }, msg);
                    }
                    for f in &failed_files {
                        self.log(LogLevel::Error, format!("FAILED: {}", f));
                    }
                    self.status = format!("{}: {}", action, name);
                    if is_up {
                        // cache updated via add_file during upload — just reload from local cache
                        self.load_from_cache();
                    }
                }
                Msg::TransferDone { id, name } => {
                    let mut speed_str = String::new();
                    if let Some(t) = self.transfers.iter_mut().find(|t| t.id == id) {
                        t.complete = true;
                        t.done = t.total;
                        let elapsed = t.started_at.elapsed().as_secs_f64();
                        if elapsed > 0.0 && t.total > 0 {
                            let speed = t.total as f64 / elapsed;
                            speed_str = format!(
                                " ({}, {}/s)",
                                ByteSize(t.total),
                                ByteSize(speed as u64)
                            );
                        }
                    }
                    let dir = if self.transfers.iter().find(|t| t.id == id).map(|t| t.is_upload).unwrap_or(true) {
                        "Uploaded"
                    } else {
                        "Downloaded"
                    };
                    let is_upload = dir == "Uploaded";
                    self.log(LogLevel::Success, format!("{}: {}{}", dir, name, speed_str));
                    self.status = format!("Completed: {}", name);
                    // Only refresh after uploads (downloads don't change B2)
                    if is_upload && !self.has_active_transfers() {
                        self.load_from_cache();
                    }
                }
                Msg::Deleted(name) => {
                    self.log(LogLevel::Success, format!("Deleted: {}", name));
                    self.status = format!("Deleted: {}", name);
                    // Remove deleted item from current listing directly
                    self.files.retain(|f| !f.file_name.starts_with(&name));
                }
                Msg::FolderCreated(name) => {
                    self.log(LogLevel::Success, format!("Created folder: {}", name));
                    self.status = format!("Created folder: {}", name);
                    // Add new folder to current listing directly (no API re-sync needed)
                    let folder_path = if self.current_prefix.is_empty() {
                        format!("{}/", name)
                    } else {
                        format!("{}{}/", self.current_prefix, name)
                    };
                    if !self.files.iter().any(|f| f.file_name == folder_path) {
                        self.files.push(FileInfo {
                            file_id: None,
                            file_name: folder_path,
                            content_length: 0,
                            content_type: None,
                            action: "folder".into(),
                            upload_timestamp: 0,
                        });
                        // Re-sort: folders first, then by name
                        self.files.sort_by(|a, b| {
                            let af = a.action == "folder";
                            let bf = b.action == "folder";
                            bf.cmp(&af).then(a.file_name.cmp(&b.file_name))
                        });
                    }
                }
                Msg::BucketCreated => {
                    self.show_create_bucket = false;
                    self.new_bucket_name.clear();
                    self.force_load_buckets();
                }
                Msg::BucketDeleted => {
                    self.selected_bucket = None;
                    self.files.clear();
                    // cache handled locally
                    self.force_load_buckets();
                }
                Msg::Error(e) => {
                    self.log(LogLevel::Error, format!("Error: {}", e));
                    self.error = Some(e);
                    self.authenticating = false;
                    self.loading = false;
                }
                Msg::DownloadFolder(prefix, dest) => {
                    self.spawn_download_folder(prefix, dest);
                }
                Msg::DestFolders(folders) => {
                    self.dest_folders = folders;
                }
                Msg::FilesPicked(paths) => {
                    self.upload_paths(paths);
                }
                Msg::FolderPicked(path) => {
                    self.upload_folder(path);
                }
                Msg::SavePicked(dest, remote_name) => {
                    self.download_to(dest, remote_name);
                }
            }
        }
    }

    // ── Spawn background tasks ──

    fn spawn_auth(&mut self) {
        let tx = self.tx.clone();
        let key_id = self.key_id.clone();
        let app_key = self.app_key.clone();
        self.authenticating = true;
        self.error = None;

        let _ = config::save_config(&config::Config {
            key_id: key_id.clone(),
            app_key: app_key.clone(),
        });

        self.rt.spawn(async move {
            match B2Client::authorize(&key_id, &app_key).await {
                Ok(auth) => { tx.send(Msg::Authorized(auth)).ok(); }
                Err(e) => { tx.send(Msg::Error(e.to_string())).ok(); }
            }
        });
    }

    fn spawn_load_buckets(&self) {
        // Skip if already loaded (buckets rarely change)
        if self.buckets_loaded {
            return;
        }
        let Some(client) = self.client.clone() else { return };
        let tx = self.tx.clone();
        self.rt.spawn(async move {
            match client.list_buckets().await {
                Ok(b) => { tx.send(Msg::BucketsLoaded(b)).ok(); }
                Err(e) => { tx.send(Msg::Error(e.to_string())).ok(); }
            }
        });
    }

    fn force_load_buckets(&mut self) {
        self.buckets_loaded = false;
        self.spawn_load_buckets();
    }

    /// Load from SQLite cache (instant, no API call)
    fn load_from_cache(&mut self) {
        let Some(ref db) = self.db else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        if let Ok(files) = db.list_folder(&bucket.bucket_id, &self.current_prefix) {
            self.files = files;
            self.selected_files.clear();
            self.loading = false;
        }
    }

    fn spawn_load_files(&mut self) {
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();

        // Use cached data if available (any age) — recache is manual only
        if let Some(ref db) = self.db {
            if db.cache_age(&bucket_id) > 0 {
                self.load_from_cache();
                return;
            }
        }

        // No cache at all — direct API call
        let Some(client) = self.client.clone() else { return };
        let prefix = if self.current_prefix.is_empty() {
            None
        } else {
            Some(self.current_prefix.clone())
        };
        let tx = self.tx.clone();
        self.rt.spawn(async move {
            match client
                .list_files(&bucket_id, prefix.as_deref(), Some("/"))
                .await
            {
                Ok(f) => { tx.send(Msg::FilesLoaded(f)).ok(); }
                Err(e) => { tx.send(Msg::Error(e.to_string())).ok(); }
            }
        });
    }

    /// Full cache sync: fetch ALL files from bucket, store in SQLite
    fn spawn_cache_sync(&mut self) {
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();
        let tx = self.tx.clone();
        self.cache_syncing = true;
        self.log(LogLevel::Info, "Syncing cache from B2...");

        self.rt.spawn(async move {
            match client.list_files(&bucket_id, None, None).await {
                Ok(files) => {
                    let count = files.len();
                    // Store in SQLite (done in async context but SQLite is thread-safe)
                    if let Ok(db) = crate::cache::FileCache::open() {
                        if let Err(e) = db.store_all(&bucket_id, &files) {
                            tx.send(Msg::Error(format!("Cache store error: {}", e))).ok();
                            return;
                        }
                    }
                    tx.send(Msg::CacheSynced { bucket_id, count }).ok();
                }
                Err(e) => {
                    tx.send(Msg::Error(format!("Cache sync: {}", e))).ok();
                }
            }
        });
    }

    /// Force refresh — re-sync cache from API
    fn force_load_files(&mut self) {
        self.needs_refresh = true;
        self.spawn_cache_sync();
    }

    fn spawn_search_files(&mut self) {
        // Try SQLite cache first (instant search, no API call)
        if let Some(ref db) = self.db {
            if let Some(idx) = self.selected_bucket {
                if let Some(bucket) = self.buckets.get(idx) {
                    if db.cache_age(&bucket.bucket_id) > 0 {
                        if let Ok(results) = db.search(&bucket.bucket_id, &self.search) {
                            let count = results.len();
                            self.files = results;
                            self.loading = false;
                            self.log(LogLevel::Info, format!("Search (cache): {} matches", count));
                            return;
                        }
                    }
                }
            }
        }

        // No cache — fall back to API search
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();
        let query = self.search.clone();
        let tx = self.tx.clone();
        let tx2 = self.tx.clone();

        self.rt.spawn(async move {
            let on_results = move |files: Vec<FileInfo>, scanned: usize| {
                tx2.send(Msg::SearchResults { files, scanned }).ok();
            };
            match client
                .search_files(&bucket_id, &query, on_results, 20)
                .await
            {
                Ok((_, all_files)) => {
                    let count = all_files.len();
                    tx.send(Msg::SearchDone { count, all_files }).ok();
                }
                Err(e) => {
                    tx.send(Msg::Error(e.to_string())).ok();
                }
            }
        });
    }

    fn upload_paths(&mut self, paths: Vec<PathBuf>) {
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();
        let prefix = self.current_prefix.clone();

        for path in paths {
            let fname = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or("file".into());
            let remote = if prefix.is_empty() {
                fname.clone()
            } else {
                format!("{}{}", prefix, fname)
            };

            let id = self.next_transfer_id();
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            self.transfers.push(Transfer {
                id,
                name: fname.clone(),
                is_upload: true,
                done: 0,
                total: size,
                complete: false,
                started_at: std::time::Instant::now(),
                done_files: 0,
                total_files: 0,
                current_file: String::new(),
            });

            let tx = self.tx.clone();
            let tx2 = self.tx.clone();
            let client = client.clone();
            let bucket_id = bucket_id.clone();
            let remote_clone = remote.clone();
            let fname_clone = fname.clone();

            let progress: ProgressFn = Arc::new(move |done, total| {
                tx2.send(Msg::Progress { id, done, total }).ok();
            });

            self.rt.spawn(async move {
                match client
                    .upload_file(&bucket_id, &path, &remote_clone, Some(progress))
                    .await
                {
                    Ok(_) => {
                        tx.send(Msg::TransferDone { id, name: fname_clone }).ok();
                    }
                    Err(e) => {
                        tx.send(Msg::Error(format!("Upload '{}': {}", fname_clone, e))).ok();
                    }
                }
            });
        }
    }

    fn upload_folder(&mut self, folder: PathBuf) {
        let mut entries = Vec::new();
        if let Err(e) = client::collect_files(&folder, &folder, &mut entries) {
            self.error = Some(format!("Scan folder: {}", e));
            return;
        }
        if entries.is_empty() {
            self.error = Some("Folder is empty".into());
            return;
        }

        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();
        let prefix = self.current_prefix.clone();

        let folder_name = folder
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or("folder".into());

        let total_files = entries.len();
        let total_bytes: u64 = entries
            .iter()
            .map(|(p, _)| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum();

        let id = self.next_transfer_id();
        self.transfers.push(Transfer {
            id,
            name: folder_name.clone(),
            is_upload: true,
            done: 0,
            total: total_bytes,
            complete: false,
            started_at: std::time::Instant::now(),
            done_files: 0,
            total_files,
            current_file: "scanning...".into(),
        });

        self.log(
            LogLevel::Info,
            format!(
                "Scanning folder: {} ({} files, {})",
                folder_name, total_files, ByteSize(total_bytes)
            ),
        );

        let tx = self.tx.clone();
        let fname = folder_name.clone();
        let client_for_list = client.clone();

        // Build (path, remote_name) pairs
        let file_list: Vec<(PathBuf, String)> = entries
            .into_iter()
            .map(|(path, relative)| {
                let with_folder = format!("{}/{}", folder_name, relative);
                let remote = if prefix.is_empty() {
                    with_folder
                } else {
                    format!("{}{}", prefix, with_folder)
                };
                (path, remote)
            })
            .collect();

        let concurrency = 150;

        self.rt.spawn(async move {
            // Smart sync: list existing files on B2, compare name + size
            let remote_prefix = if prefix.is_empty() {
                format!("{}/", folder_name)
            } else {
                format!("{}{}/", prefix, folder_name)
            };

            // Map: filename → size on B2
            let existing: std::collections::HashMap<String, u64> =
                match client_for_list.list_files(&bucket_id, Some(&remote_prefix), None).await {
                    Ok(files) => files.into_iter().map(|f| (f.file_name, f.content_length)).collect(),
                    Err(_) => std::collections::HashMap::new(),
                };

            let mut skipped = 0usize;
            let mut reupload_partial = 0usize;
            let to_upload: Vec<(PathBuf, String)> = file_list
                .into_iter()
                .filter(|(path, remote)| {
                    if let Some(&b2_size) = existing.get(remote) {
                        let local_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                        if local_size == b2_size {
                            skipped += 1;
                            return false; // same size, skip
                        }
                        // Size mismatch = partial/corrupt upload, re-upload
                        reupload_partial += 1;
                        return true;
                    }
                    true // not on B2, upload
                })
                .collect();

            if skipped > 0 || reupload_partial > 0 {
                let mut msg = format!("{} skipped", skipped);
                if reupload_partial > 0 {
                    msg.push_str(&format!(", {} partial/corrupted re-uploading", reupload_partial));
                }
                tx.send(Msg::FolderProgress {
                    id,
                    done_files: skipped,
                    total_files,
                    done_bytes: 0,
                    total_bytes,
                    current: msg,
                }).ok();
            }

            if to_upload.is_empty() {
                tx.send(Msg::FolderDone {
                    id,
                    name: fname,
                    done_files: total_files,
                    total_files,
                    failed: 0,
                    failed_files: Vec::new(),
                }).ok();
                return;
            }

            let upload_count = to_upload.len();
            let tx_progress = tx.clone();
            let on_progress = move |done_f: usize, _total_f: usize, done_b: u64, total_b: u64, current: &str| {
                tx_progress
                    .send(Msg::FolderProgress {
                        id,
                        done_files: skipped + done_f,
                        total_files,
                        done_bytes: done_b,
                        total_bytes: total_b,
                        current: current.to_string(),
                    })
                    .ok();
            };

            match client
                .upload_batch(&bucket_id, to_upload, concurrency, on_progress)
                .await
            {
                Ok((succeeded, fail_count, failed_list)) => {
                    tx.send(Msg::FolderDone {
                        id,
                        name: fname,
                        done_files: skipped + succeeded,
                        total_files,
                        failed: fail_count,
                        failed_files: failed_list,
                    })
                    .ok();
                }
                Err(e) => {
                    tx.send(Msg::Error(format!("Folder upload: {}", e))).ok();
                }
            }
        });
    }

    fn download_to(&mut self, dest: PathBuf, remote_name: String) {
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_name = bucket.bucket_name.clone();

        let id = self.next_transfer_id();
        let short = remote_name.rsplit('/').next().unwrap_or(&remote_name).to_string();
        self.transfers.push(Transfer {
            id,
            name: short.clone(),
            is_upload: false,
            done: 0,
            total: 0,
            complete: false,
            started_at: std::time::Instant::now(),
            done_files: 0,
            total_files: 0,
            current_file: String::new(),
        });

        let tx = self.tx.clone();
        let tx2 = self.tx.clone();
        let progress: ProgressFn = Arc::new(move |done, total| {
            tx2.send(Msg::Progress { id, done, total }).ok();
        });

        self.rt.spawn(async move {
            match client
                .download_file_by_name(&bucket_name, &remote_name, &dest, Some(progress))
                .await
            {
                Ok(_) => {
                    tx.send(Msg::TransferDone { id, name: short }).ok();
                }
                Err(e) => {
                    tx.send(Msg::Error(format!("Download: {}", e))).ok();
                }
            }
        });
    }

    fn spawn_download_folder(&mut self, prefix: String, dest_dir: PathBuf) {
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_name = bucket.bucket_name.clone();
        let bucket_id = bucket.bucket_id.clone();

        let folder_name = prefix.trim_end_matches('/').rsplit('/').next()
            .unwrap_or(&prefix).to_string();

        let id = self.next_transfer_id();
        self.transfers.push(Transfer {
            id,
            name: folder_name.clone(),
            is_upload: false,
            done: 0,
            total: 0,
            complete: false,
            started_at: std::time::Instant::now(),
            done_files: 0,
            total_files: 0,
            current_file: "scanning...".into(),
        });

        self.log(LogLevel::Info, format!("Downloading folder: {}", prefix));

        let tx = self.tx.clone();
        let concurrency = 50; // download concurrency

        self.rt.spawn(async move {
            // List all files in this folder
            let files = match client.list_files(&bucket_id, Some(&prefix), None).await {
                Ok(f) => f,
                Err(e) => {
                    tx.send(Msg::Error(format!("Download list: {}", e))).ok();
                    return;
                }
            };

            // Only downloadable files (not folders)
            let download_list: Vec<(String, std::path::PathBuf)> = files
                .iter()
                .filter(|f| f.action == "upload")
                .map(|f| {
                    let relative = f.file_name.strip_prefix(&prefix).unwrap_or(&f.file_name);
                    let dest = dest_dir.join(relative);
                    (f.file_name.clone(), dest)
                })
                .collect();

            let total_files = download_list.len();
            let total_bytes: u64 = files.iter()
                .filter(|f| f.action == "upload")
                .map(|f| f.content_length)
                .sum();

            tx.send(Msg::FolderProgress {
                id,
                done_files: 0,
                total_files,
                done_bytes: 0,
                total_bytes,
                current: format!("{} files to download", total_files),
            }).ok();

            let tx_progress = tx.clone();
            let on_progress = move |done_f: usize, total_f: usize, done_b: u64, total_b: u64, current: &str| {
                tx_progress.send(Msg::FolderProgress {
                    id,
                    done_files: done_f,
                    total_files: total_f,
                    done_bytes: done_b,
                    total_bytes,
                    current: current.to_string(),
                }).ok();
            };

            match client
                .download_batch(&bucket_name, download_list, concurrency, on_progress)
                .await
            {
                Ok((succeeded, fail_count, failed_list)) => {
                    tx.send(Msg::FolderDone {
                        id,
                        name: folder_name,
                        done_files: succeeded,
                        total_files,
                        failed: fail_count,
                        failed_files: failed_list,
                    }).ok();
                }
                Err(e) => {
                    tx.send(Msg::Error(format!("Download folder: {}", e))).ok();
                }
            }
        });
    }

    fn spawn_delete(&self, file_id: String, file_name: String) {
        let Some(client) = self.client.clone() else { return };
        let tx = self.tx.clone();
        let name = file_name.clone();
        self.rt.spawn(async move {
            match client.delete_file(&file_id, &file_name).await {
                Ok(_) => { tx.send(Msg::Deleted(name)).ok(); }
                Err(e) => { tx.send(Msg::Error(e.to_string())).ok(); }
            }
        });
    }

    fn spawn_create_folder(&mut self) {
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();
        let prefix = self.current_prefix.clone();
        let folder = self.new_folder_name.trim().trim_matches('/').to_string();
        let tx = self.tx.clone();

        // B2 folders are virtual — upload a zero-byte placeholder
        let remote = if prefix.is_empty() {
            format!("{}/.bzEmpty", folder)
        } else {
            format!("{}{}/.bzEmpty", prefix, folder)
        };

        self.show_create_folder = false;
        self.new_folder_name.clear();

        self.rt.spawn(async move {
            match client.get_upload_url(&bucket_id).await {
                Ok(url) => {
                    let encoded = crate::client::percent_encode_filename_pub(&remote);
                    let sha1_empty = "da39a3ee5e6b4b0d3255bfef95601890afd80709"; // SHA1 of empty

                    let resp = client.http_client()
                        .post(&url.upload_url)
                        .header("Authorization", &url.authorization_token)
                        .header("X-Bz-File-Name", &encoded)
                        .header("Content-Type", "application/octet-stream")
                        .header("Content-Length", "0")
                        .header("X-Bz-Content-Sha1", sha1_empty)
                        .body(vec![])
                        .send()
                        .await;

                    match resp {
                        Ok(r) if r.status().is_success() => {
                            tx.send(Msg::FolderCreated(folder.clone())).ok();
                        }
                        Ok(r) => {
                            let body = r.text().await.unwrap_or_default();
                            tx.send(Msg::Error(format!("Create folder: {}", body))).ok();
                        }
                        Err(e) => {
                            tx.send(Msg::Error(format!("Create folder: {}", e))).ok();
                        }
                    }
                }
                Err(e) => {
                    tx.send(Msg::Error(format!("Create folder: {}", e))).ok();
                }
            }
        });
    }

    fn spawn_delete_folder(&self, prefix: String) {
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();
        let tx = self.tx.clone();

        self.rt.spawn(async move {
            match client.list_files(&bucket_id, Some(&prefix), None).await {
                Ok(files) => {
                    let total = files.len();
                    let deleted = Arc::new(std::sync::atomic::AtomicU64::new(0));

                    // Delete 100 files concurrently
                    let del_tasks: Vec<_> = files
                        .iter()
                        .filter_map(|f| {
                            f.file_id.as_ref().map(|fid| (fid.clone(), f.file_name.clone()))
                        })
                        .collect();

                    futures::stream::iter(del_tasks.into_iter().map(|(fid, fname)| {
                        let client = client.clone();
                        let deleted = deleted.clone();
                        async move {
                            if client.delete_file(&fid, &fname).await.is_ok() {
                                deleted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }))
                    .buffer_unordered(100)
                    .collect::<Vec<()>>()
                    .await;

                    let count = deleted.load(std::sync::atomic::Ordering::Relaxed);
                    tx.send(Msg::Deleted(format!(
                        "{} ({}/{} files)",
                        prefix, count, total
                    )))
                    .ok();
                }
                Err(e) => {
                    tx.send(Msg::Error(format!("Delete folder '{}': {}", prefix, e)))
                        .ok();
                }
            }
        });
    }

    fn spawn_create_bucket(&self) {
        let Some(client) = self.client.clone() else { return };
        let tx = self.tx.clone();
        let name = self.new_bucket_name.clone();
        let public = self.new_bucket_public;
        self.rt.spawn(async move {
            match client.create_bucket(&name, public).await {
                Ok(_) => { tx.send(Msg::BucketCreated).ok(); }
                Err(e) => { tx.send(Msg::Error(e.to_string())).ok(); }
            }
        });
    }

    fn spawn_delete_bucket(&self, bucket_id: String) {
        let Some(client) = self.client.clone() else { return };
        let tx = self.tx.clone();
        self.rt.spawn(async move {
            match client.delete_bucket(&bucket_id).await {
                Ok(_) => { tx.send(Msg::BucketDeleted).ok(); }
                Err(e) => { tx.send(Msg::Error(e.to_string())).ok(); }
            }
        });
    }

    fn has_active_transfers(&self) -> bool {
        self.transfers.iter().any(|t| !t.complete)
    }

    fn current_bucket_name(&self) -> Option<String> {
        self.selected_bucket
            .and_then(|i| self.buckets.get(i))
            .map(|b| b.bucket_name.clone())
    }
}

// ── UI ──

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.process_messages();
        self.poll_upload_queue();

        // Handle drag-and-drop
        let dropped: Vec<_> = ctx.input(|i| {
            i.raw.dropped_files
                .iter()
                .filter_map(|f| f.path.clone())
                .collect()
        });
        if !dropped.is_empty() && self.client.is_some() && self.selected_bucket.is_some() {
            let mut files = Vec::new();
            let mut folders = Vec::new();
            for p in dropped {
                if p.is_dir() {
                    folders.push(p);
                } else {
                    files.push(p);
                }
            }
            if !files.is_empty() {
                self.upload_paths(files);
            }
            for folder in folders {
                self.upload_folder(folder);
            }
        }

        // Auto-trigger upload picker when launched via context menu
        if self.pending_upload.is_some()
            && self.client.is_some()
            && !self.buckets.is_empty()
            && !self.show_upload_picker
        {
            self.show_upload_picker = true;
        }

        if !self.setup_done {
            self.render_setup(ctx);
        } else if self.client.is_none() {
            self.render_login(ctx);
        } else {
            self.render_main(ctx);
        }

        // Upload picker modal (from --upload arg)
        if self.show_upload_picker {
            self.render_upload_picker(ctx);
        }

        // Destination picker (from right-click queue)
        if self.bring_to_front {
            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
            ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
            self.bring_to_front = false;
        }
        if self.show_dest_picker {
            self.render_dest_picker(ctx);
        }
        if self.minimize_after {
            self.minimize_after = false;
            ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(true));
        }

        if self.has_active_transfers() || self.authenticating || self.loading {
            ctx.request_repaint_after(std::time::Duration::from_millis(100));
        } else {
            // Poll for right-click queue file every 1 second
            ctx.request_repaint_after(std::time::Duration::from_secs(1));
        }
    }
}

impl App {
    /// Shown on first run, and whenever the stored credentials are cleared. Asked once:
    /// `spawn_auth` writes them to config.json, so later launches go straight to the app.
    fn render_setup(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(60.0);
                ui.heading(egui::RichText::new("B2 Storage Manager").size(28.0).strong());
                ui.add_space(8.0);
                ui.label(
                    egui::RichText::new("Connect your Backblaze B2 account")
                        .color(egui::Color32::GRAY),
                );
                ui.add_space(32.0);

                let panel_width = 380.0;
                ui.allocate_ui(egui::vec2(panel_width, 220.0), |ui| {
                    ui.vertical(|ui| {
                        ui.label("Key ID");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.key_id)
                                .desired_width(panel_width)
                                .hint_text("keyID from the B2 console"),
                        );
                        ui.add_space(12.0);

                        ui.label("Application Key");
                        let re = ui.add(
                            egui::TextEdit::singleline(&mut self.app_key)
                                .password(true)
                                .desired_width(panel_width)
                                .hint_text("applicationKey, shown only once"),
                        );
                        ui.add_space(20.0);

                        let enter = re.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                        let filled =
                            !self.key_id.trim().is_empty() && !self.app_key.trim().is_empty();

                        ui.vertical_centered(|ui| {
                            let btn = egui::Button::new("Connect")
                                .min_size(egui::vec2(panel_width, 36.0));
                            if ui.add_enabled(filled, btn).clicked() || (enter && filled) {
                                self.key_id = self.key_id.trim().to_string();
                                self.app_key = self.app_key.trim().to_string();
                                self.setup_done = true;
                                self.error = None;
                                self.spawn_auth();
                            }
                        });
                    });
                });

                ui.add_space(20.0);
                ui.label(
                    egui::RichText::new(
                        "Create a key in the Backblaze console under Account › Application Keys.\n\
                         Keys are saved to ~/.b2storage/config.json on this machine only.",
                    )
                    .size(11.0)
                    .color(egui::Color32::GRAY),
                );

                if let Some(ref err) = self.error {
                    ui.add_space(16.0);
                    ui.colored_label(egui::Color32::from_rgb(255, 80, 80), err);
                }
            });
        });
    }

    fn render_login(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            ui.vertical_centered(|ui| {
                ui.add_space(100.0);
                if self.authenticating {
                    ui.spinner();
                    ui.add_space(8.0);
                    ui.label("Connecting to B2...");
                } else if let Some(err) = self.error.clone() {
                    ui.colored_label(egui::Color32::from_rgb(255, 80, 80), &err);
                    ui.add_space(16.0);
                    if ui.button("Retry").clicked() {
                        self.spawn_auth();
                    }
                    ui.add_space(8.0);
                    // Without this a rejected key is a dead end, since the stored one is
                    // reloaded and retried on every launch.
                    if ui.button("Use different credentials").clicked() {
                        self.app_key.clear();
                        self.setup_done = false;
                        self.error = None;
                    }
                }
            });
        });
    }

    fn render_upload_picker(&mut self, ctx: &egui::Context) {
        egui::Window::new("Upload to B2 Storage")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                if let Some(ref path) = self.pending_upload.clone() {
                    ui.label(
                        egui::RichText::new(format!(
                            "Folder: {}",
                            path.file_name()
                                .map(|n| n.to_string_lossy().to_string())
                                .unwrap_or("unknown".into())
                        ))
                        .strong(),
                    );
                    ui.add_space(8.0);
                    ui.label("Select bucket:");
                    ui.add_space(4.0);

                    let mut picked = None;
                    for (i, bucket) in self.buckets.iter().enumerate() {
                        let selected = self.selected_bucket == Some(i);
                        if ui.selectable_label(selected, &bucket.bucket_name).clicked() {
                            self.selected_bucket = Some(i);
                        }
                        if selected {
                            picked = Some(i);
                        }
                    }

                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add_enabled(picked.is_some(), egui::Button::new("Upload"))
                            .clicked()
                        {
                            let upload_path = path.clone();
                            self.show_upload_picker = false;
                            self.current_prefix.clear();
                            self.nav_stack.clear();
                            self.upload_folder(upload_path);
                            self.pending_upload = None;
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_upload_picker = false;
                            self.pending_upload = None;
                        }
                    });
                }
            });
    }

    fn render_dest_picker(&mut self, ctx: &egui::Context) {
        let mut close = false;
        let mut do_upload = false;

        egui::Window::new("Choose upload destination")
            .collapsible(false)
            .resizable(true)
            .min_width(400.0)
            .min_height(300.0)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                // Show what's being uploaded
                let names: Vec<String> = self
                    .queued_uploads
                    .iter()
                    .map(|p| {
                        p.file_name()
                            .map(|n| n.to_string_lossy().to_string())
                            .unwrap_or("?".into())
                    })
                    .collect();
                ui.label(
                    egui::RichText::new(format!("Upload: {}", names.join(", ")))
                        .strong(),
                );
                ui.add_space(4.0);

                // Current destination path
                let dest_display = if self.dest_prefix.is_empty() {
                    "/ (root)".to_string()
                } else {
                    format!("/{}", self.dest_prefix)
                };
                ui.horizontal(|ui| {
                    ui.label("Destination:");
                    ui.label(
                        egui::RichText::new(&dest_display)
                            .color(egui::Color32::from_rgb(100, 200, 100))
                            .strong(),
                    );
                });
                ui.add_space(4.0);
                ui.separator();

                // Browse folders
                ui.label("Browse storage folders:");

                // Back button
                if !self.dest_prefix.is_empty() {
                    if ui.small_button("\u{2B06} Go up").clicked() {
                        // Remove last segment
                        let trimmed = self.dest_prefix.trim_end_matches('/');
                        if let Some(pos) = trimmed.rfind('/') {
                            self.dest_prefix = trimmed[..=pos].to_string();
                        } else {
                            self.dest_prefix.clear();
                        }
                        self.load_dest_folders();
                    }
                }

                egui::ScrollArea::vertical()
                    .max_height(200.0)
                    .show(ui, |ui| {
                        if self.dest_folders.is_empty() {
                            ui.label(
                                egui::RichText::new("(no subfolders)")
                                    .color(egui::Color32::GRAY)
                                    .italics(),
                            );
                        }
                        let mut navigate = None;
                        for folder in &self.dest_folders {
                            let display = folder
                                .strip_prefix(&self.dest_prefix)
                                .unwrap_or(folder)
                                .trim_end_matches('/');
                            if ui.link(format!("\u{1F4C1} {}/", display)).clicked() {
                                navigate = Some(folder.clone());
                            }
                        }
                        if let Some(f) = navigate {
                            self.dest_prefix = f;
                            self.load_dest_folders();
                        }
                    });

                ui.add_space(8.0);
                ui.separator();
                ui.horizontal(|ui| {
                    if ui.button("\u{2B06} Upload Here").clicked() {
                        do_upload = true;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
            });

        if do_upload {
            let saved_prefix = self.current_prefix.clone();
            self.current_prefix = self.dest_prefix.clone();
            let paths = std::mem::take(&mut self.queued_uploads);
            for path in paths {
                if path.is_dir() {
                    self.upload_folder(path);
                } else {
                    self.upload_paths(vec![path]);
                }
            }
            self.current_prefix = saved_prefix;
            self.show_dest_picker = false;
            self.minimize_after = true;
        }
        if close {
            self.queued_uploads.clear();
            self.show_dest_picker = false;
            self.minimize_after = true;
        }
    }

    fn render_main(&mut self, ctx: &egui::Context) {
        // ── Top bar ──
        egui::TopBottomPanel::top("topbar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading(
                    egui::RichText::new("B2 Storage Manager")
                        .size(16.0)
                        .strong(),
                );
                ui.separator();
                if let Some(ref client) = self.client {
                    ui.label(
                        egui::RichText::new(format!("Account: {}", client.account_id()))
                            .small()
                            .color(egui::Color32::GRAY),
                    );
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.button("Logout").clicked() {
                        self.client = None;
                        self.buckets.clear();
                        self.files.clear();
                        self.status = "Disconnected".into();
                    }
                    ui.separator();
                    if self.context_menu_installed {
                        if ui.small_button("\u{2699} Uninstall Right-Click Menu").clicked() {
                            match uninstall_context_menu() {
                                Ok(_) => {
                                    self.context_menu_installed = false;
                                    self.log(LogLevel::Success, "Context menu removed");
                                }
                                Err(e) => self.log(LogLevel::Error, format!("Uninstall failed: {}", e)),
                            }
                        }
                    } else if ui.small_button("\u{2699} Install Right-Click Menu").clicked() {
                        let tx = self.tx.clone();
                        std::thread::spawn(move || {
                            match install_context_menu() {
                                Ok(_) => tx.send(Msg::Deleted("Context menu installed! Right-click any folder to upload.".into())).ok(),
                                Err(e) => tx.send(Msg::Error(format!("Install failed: {}", e))).ok(),
                            };
                        });
                        self.context_menu_installed = true;
                        self.log(LogLevel::Info, "Installing context menu...");
                    }
                });
            });
        });

        // ── Status bar ──
        egui::TopBottomPanel::bottom("statusbar")
            .exact_height(24.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    let dot_color = if self.client.is_some() {
                        egui::Color32::from_rgb(80, 200, 80)
                    } else {
                        egui::Color32::from_rgb(200, 80, 80)
                    };
                    ui.colored_label(dot_color, "\u{25CF}");
                    ui.label(&self.status);
                    if self.loading {
                        ui.spinner();
                    }
                });
            });

        // ── Bottom panel: Active transfers + Logs ──
        egui::TopBottomPanel::bottom("bottom_panel")
            .resizable(true)
            .min_height(80.0)
            .default_height(180.0)
            .show(ctx, |ui| {
                // Active transfers
                let active: Vec<_> = self.transfers.iter().filter(|t| !t.complete).collect();
                if !active.is_empty() {
                    ui.strong(format!("Active Transfers ({})", active.len()));
                    ui.separator();
                    for t in &active {
                        let icon = if t.is_upload { "\u{2B06}" } else { "\u{2B07}" };
                        let elapsed = t.started_at.elapsed().as_secs_f64();
                        let speed = if elapsed > 0.0 { t.done as f64 / elapsed } else { 0.0 };

                        // Row 1: icon + name + file count
                        ui.horizontal(|ui| {
                            ui.colored_label(egui::Color32::from_rgb(100, 160, 255), icon);
                            ui.strong(&t.name);
                            if t.total_files > 0 {
                                ui.label(
                                    egui::RichText::new(format!(
                                        "({}/{} files)",
                                        t.done_files, t.total_files
                                    ))
                                    .small()
                                    .color(egui::Color32::GRAY),
                                );
                            }
                            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                                ui.label(
                                    egui::RichText::new(format!("{}/s", ByteSize(speed as u64)))
                                        .small()
                                        .color(egui::Color32::from_rgb(100, 200, 100)),
                                );
                            });
                        });

                        // Row 2: full-width progress bar
                        if t.total > 0 {
                            let frac = t.done as f32 / t.total as f32;
                            let text = format!(
                                "{} / {}  -  {:.0}%",
                                ByteSize(t.done),
                                ByteSize(t.total),
                                frac * 100.0
                            );
                            ui.add(
                                egui::ProgressBar::new(frac)
                                    .text(text)
                                    .desired_width(ui.available_width()),
                            );
                        } else {
                            ui.spinner();
                        }

                        // Row 3: current file (small gray)
                        if !t.current_file.is_empty() {
                            ui.label(
                                egui::RichText::new(format!("  \u{2192} {}", t.current_file))
                                    .small()
                                    .color(egui::Color32::from_rgb(130, 130, 130)),
                            );
                        }

                        ui.add_space(4.0);
                    }
                }

                // Logs
                ui.horizontal(|ui| {
                    ui.strong("Logs");
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.small_button("Clear").clicked() {
                            self.logs.clear();
                            self.transfers.clear();
                        }
                    });
                });
                ui.separator();

                egui::ScrollArea::vertical()
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for entry in &self.logs {
                            ui.horizontal(|ui| {
                                let time_str = entry.time.format("%H:%M:%S").to_string();
                                ui.label(
                                    egui::RichText::new(&time_str)
                                        .small()
                                        .color(egui::Color32::from_rgb(120, 120, 120)),
                                );
                                let (color, prefix) = match entry.level {
                                    LogLevel::Info => (egui::Color32::from_rgb(150, 180, 220), "INFO"),
                                    LogLevel::Success => (egui::Color32::from_rgb(80, 200, 80), " OK "),
                                    LogLevel::Error => (egui::Color32::from_rgb(255, 80, 80), " ERR"),
                                    LogLevel::Progress => (egui::Color32::from_rgb(100, 160, 255), "PROG"),
                                };
                                ui.colored_label(color, format!("[{}]", prefix));
                                ui.label(
                                    egui::RichText::new(&entry.message).color(color),
                                );
                            });
                        }
                    });
            });

        // ── Sidebar: buckets ──
        egui::SidePanel::left("buckets")
            .default_width(200.0)
            .min_width(150.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.strong("Buckets");
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.small_button("+").clicked() {
                            self.show_create_bucket = !self.show_create_bucket;
                        }
                        if ui.small_button("\u{21BB}").clicked() {
                            self.force_load_buckets();
                        }
                    });
                });
                ui.separator();

                // Create bucket inline form
                if self.show_create_bucket {
                    ui.group(|ui| {
                        ui.label("New bucket:");
                        ui.text_edit_singleline(&mut self.new_bucket_name);
                        ui.checkbox(&mut self.new_bucket_public, "Public");
                        ui.horizontal(|ui| {
                            if ui.small_button("Create").clicked()
                                && !self.new_bucket_name.is_empty()
                            {
                                self.spawn_create_bucket();
                            }
                            if ui.small_button("Cancel").clicked() {
                                self.show_create_bucket = false;
                            }
                        });
                    });
                    ui.separator();
                }

                // Bucket list
                egui::ScrollArea::vertical().show(ui, |ui| {
                    let mut clicked_idx = None;
                    for (i, bucket) in self.buckets.iter().enumerate() {
                        let selected = self.selected_bucket == Some(i);
                        let label = format!(
                            "{} {}",
                            if bucket.bucket_type == "allPublic" {
                                "\u{1F513}"
                            } else {
                                "\u{1F512}"
                            },
                            bucket.bucket_name
                        );
                        if ui.selectable_label(selected, &label).clicked() {
                            clicked_idx = Some(i);
                        }
                    }
                    if let Some(i) = clicked_idx {
                        self.selected_bucket = Some(i);
                        self.current_prefix.clear();
                        self.nav_stack.clear();
                        self.files.clear();
                        self.selected_files.clear();
                        self.search.clear();
                        self.searching = false;
                        self.loading = true;
                        self.spawn_load_files();
                    }
                });

                // Delete bucket removed — too dangerous for accidental clicks
                // Use B2 web console for bucket deletion
            });

        // ── Central: file browser ──
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.selected_bucket.is_none() {
                ui.vertical_centered(|ui| {
                    ui.add_space(100.0);
                    ui.heading(
                        egui::RichText::new("Select a bucket from the sidebar")
                            .color(egui::Color32::GRAY),
                    );
                    ui.add_space(16.0);
                    ui.label("Or drag & drop files here after selecting a bucket");
                });
                return;
            }

            // Error display
            if let Some(ref err) = self.error {
                ui.horizontal(|ui| {
                    ui.colored_label(
                        egui::Color32::from_rgb(255, 80, 80),
                        format!("Error: {}", err),
                    );
                    if ui.small_button("x").clicked() {
                        // handled below
                    }
                });
                if ui.input(|i| i.pointer.any_click()) {
                    // just keep showing until user dismisses
                }
            }
            let mut clear_error = false;
            if self.error.is_some() {
                if ui.small_button("Dismiss error").clicked() {
                    clear_error = true;
                }
                ui.separator();
            }
            if clear_error {
                self.error = None;
            }

            // ── Breadcrumb ──
            ui.horizontal(|ui| {
                let bucket_name = self.current_bucket_name().unwrap_or_default();
                if ui
                    .link(egui::RichText::new(&bucket_name).strong())
                    .clicked()
                {
                    self.current_prefix.clear();
                    self.nav_stack.clear();
                    self.loading = true;
                    self.spawn_load_files();
                }

                for (i, seg) in self.nav_stack.clone().iter().enumerate() {
                    ui.label("/");
                    if ui.link(seg).clicked() {
                        let new_prefix: String = self.nav_stack[..=i].join("");
                        self.current_prefix = new_prefix;
                        self.nav_stack.truncate(i + 1);
                        self.loading = true;
                        self.spawn_load_files();
                    }
                }
            });
            ui.add_space(4.0);

            // ── Toolbar ──
            ui.horizontal(|ui| {
                // Upload file
                if ui.button("\u{2B06} Upload Files").clicked() {
                    let tx = self.tx.clone();
                    std::thread::spawn(move || {
                        if let Some(paths) = rfd::FileDialog::new().pick_files() {
                            tx.send(Msg::FilesPicked(paths)).ok();
                        }
                    });
                }

                // Upload folder
                if ui.button("\u{1F4C1} Upload Folder").clicked() {
                    let tx = self.tx.clone();
                    std::thread::spawn(move || {
                        if let Some(path) = rfd::FileDialog::new().pick_folder() {
                            tx.send(Msg::FolderPicked(path)).ok();
                        }
                    });
                }

                // New folder
                if ui.button("\u{2795} New Folder").clicked() {
                    self.show_create_folder = !self.show_create_folder;
                    self.new_folder_name.clear();
                }

                // Download selected
                let has_selection = !self.selected_files.is_empty();
                if ui
                    .add_enabled(has_selection, egui::Button::new("\u{2B07} Download"))
                    .clicked()
                {
                    let selected_files: Vec<_> = self.files.iter()
                        .filter(|f| self.selected_files.contains(&f.file_name) && f.action != "folder")
                        .cloned().collect();
                    let selected_folders: Vec<String> = self.files.iter()
                        .filter(|f| self.selected_files.contains(&f.file_name) && f.action == "folder")
                        .map(|f| f.file_name.clone()).collect();

                    let has_folders = !selected_folders.is_empty();
                    let has_files = !selected_files.is_empty();

                    if has_folders || has_files {
                        // Always pick destination folder for consistency
                        let tx = self.tx.clone();
                        let files = selected_files;
                        let folders = selected_folders;
                        std::thread::spawn(move || {
                            if let Some(dir) = rfd::FileDialog::new().pick_folder() {
                                // Queue individual files
                                for f in &files {
                                    let fname = f.file_name.rsplit('/').next().unwrap_or(&f.file_name).to_string();
                                    let dest = dir.join(&fname);
                                    tx.send(Msg::SavePicked(dest, f.file_name.clone())).ok();
                                }
                                // Queue folder downloads
                                for folder in &folders {
                                    let folder_short = folder.trim_end_matches('/').rsplit('/').next().unwrap_or(folder);
                                    let dest = dir.join(folder_short);
                                    tx.send(Msg::DownloadFolder(folder.clone(), dest)).ok();
                                }
                            }
                        });
                    }
                }

                // Delete selected
                if ui
                    .add_enabled(
                        has_selection,
                        egui::Button::new(
                            egui::RichText::new("\u{1F5D1} Delete")
                                .color(egui::Color32::from_rgb(255, 100, 100)),
                        ),
                    )
                    .clicked()
                {
                    self.show_delete_confirm = true;
                }

                ui.separator();

                // Refresh = reload from cache, Recache = re-sync from API
                if ui.button("\u{21BB} Refresh").clicked() {
                    self.load_from_cache();
                }
                if ui.button("\u{1F504} Recache").on_hover_text("Re-sync all files from B2").clicked() {
                    self.loading = true;
                    self.force_load_files();
                }
                if self.cache_syncing {
                    ui.spinner();
                }

                // Go up
                if !self.nav_stack.is_empty() {
                    if ui.button("\u{2B06} Up").clicked() {
                        self.nav_stack.pop();
                        self.current_prefix = self.nav_stack.join("");
                        self.loading = true;
                        self.spawn_load_files();
                    }
                }

                // Back to search results
                if !self.last_search.is_empty() && !self.searching {
                    if ui.button("\u{1F519} Back to Search").clicked() {
                        self.search = self.last_search.clone();
                        self.last_search.clear();
                        self.searching = true;
                        self.loading = true;
                        self.spawn_search_files();
                    }
                }

            });

            // ── Create folder inline form ──
            if self.show_create_folder {
                ui.horizontal(|ui| {
                    ui.label("\u{1F4C1} New folder name:");
                    let enter = ui.add(
                        egui::TextEdit::singleline(&mut self.new_folder_name)
                            .desired_width(250.0)
                            .hint_text("folder name"),
                    )
                    .lost_focus()
                        && ui.input(|i| i.key_pressed(egui::Key::Enter));

                    if (ui.button("Create").clicked() || enter)
                        && !self.new_folder_name.is_empty()
                    {
                        self.spawn_create_folder();
                    }
                    if ui.button("Cancel").clicked() {
                        self.show_create_folder = false;
                    }
                });
            }

            // Search bar on its own row
            ui.horizontal(|ui| {
                ui.label("\u{1F50D}");
                let resp = ui.add(
                    egui::TextEdit::singleline(&mut self.search)
                        .desired_width(ui.available_width() - 80.0)
                        .hint_text("Search entire bucket... (Enter to search)"),
                );
                let enter_pressed = resp.lost_focus()
                    && ui.input(|i| i.key_pressed(egui::Key::Enter));

                if ui.button("Search").clicked() || enter_pressed {
                    if !self.search.is_empty() {
                        self.searching = true;
                        self.loading = true;
                        self.spawn_search_files();
                    }
                }

                if self.searching && self.search.is_empty() {
                    // Cleared search — go back to folder view
                    self.searching = false;
                    self.spawn_load_files();
                }
            });
            ui.separator();

            // ── Delete confirmation ──
            if self.show_delete_confirm {
                let selected_count = self.selected_files.len();
                ui.horizontal(|ui| {
                    ui.colored_label(
                        egui::Color32::from_rgb(255, 200, 50),
                        format!("Delete {} selected file(s)?", selected_count),
                    );
                    if ui.button("Yes, delete").clicked() {
                        // Collect files to delete directly
                        let mut direct: Vec<(String, String)> = Vec::new();
                        let mut folder_prefixes: Vec<String> = Vec::new();

                        for f in &self.files {
                            if !self.selected_files.contains(&f.file_name) {
                                continue;
                            }
                            if f.action == "folder" {
                                // Folder = delete all files inside
                                folder_prefixes.push(f.file_name.clone());
                            } else if let Some(ref id) = f.file_id {
                                direct.push((id.clone(), f.file_name.clone()));
                            }
                        }

                        for (fid, fname) in direct {
                            self.spawn_delete(fid, fname);
                        }
                        for prefix in folder_prefixes {
                            self.spawn_delete_folder(prefix);
                        }

                        self.selected_files.clear();
                        self.show_delete_confirm = false;
                    }
                    if ui.button("Cancel").clicked() {
                        self.show_delete_confirm = false;
                    }
                });
                ui.separator();
            }

            // ── File table ──
            if self.loading {
                ui.vertical_centered(|ui| {
                    ui.add_space(40.0);
                    ui.spinner();
                    ui.label("Loading...");
                });
                return;
            }

            // In search mode, files are already filtered server-side
            // In normal mode, show all files (already scoped to current prefix)
            let filtered: Vec<usize> = (0..self.files.len()).collect();

            if filtered.is_empty() && self.files.is_empty() {
                ui.vertical_centered(|ui| {
                    ui.add_space(60.0);
                    ui.label(
                        egui::RichText::new("This folder is empty")
                            .size(18.0)
                            .color(egui::Color32::GRAY),
                    );
                    ui.add_space(12.0);
                    ui.label("Upload files using the toolbar or drag & drop");
                });
                return;
            }

            // Select all checkbox and column headers
            let all_selected = !filtered.is_empty()
                && filtered.iter().all(|&i| {
                    self.selected_files.contains(self.files[i].file_id.as_deref().unwrap_or(""))
                });

            // Gather display data to avoid borrow issues
            struct Row {
                file_id: String,
                select_key: String,
                name: String,
                display_name: String,
                size_str: String,
                date_str: String,
                is_folder: bool,
                selected: bool,
            }
            let rows: Vec<Row> = filtered
                .iter()
                .map(|&i| {
                    let f = &self.files[i];
                    let is_folder = f.action == "folder";
                    let display_name = if self.searching {
                        // In search mode show full path + file count for folders
                        let mut name = f.file_name.clone();
                        if is_folder {
                            if let Some(ref info) = f.content_type {
                                name = format!("{} ({})", name, info);
                            }
                        }
                        name
                    } else if is_folder {
                        let n = f.file_name.trim_end_matches('/');
                        n.rsplit('/').next().unwrap_or(n).to_string() + "/"
                    } else {
                        f.file_name
                            .rsplit('/')
                            .next()
                            .unwrap_or(&f.file_name)
                            .to_string()
                    };
                    let size_str = if is_folder {
                        if f.content_length > 0 {
                            ByteSize(f.content_length).to_string()
                        } else {
                            "---".into()
                        }
                    } else {
                        ByteSize(f.content_length).to_string()
                    };
                    let date_str =
                        DateTime::<Utc>::from_timestamp_millis(f.upload_timestamp as i64)
                            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                            .unwrap_or_else(|| "---".into());
                    Row {
                        file_id: f.file_id.clone().unwrap_or_default(),
                        select_key: f.file_name.clone(),
                        name: f.file_name.clone(),
                        display_name,
                        size_str,
                        date_str,
                        is_folder,
                        selected: self.selected_files.contains(&f.file_name),
                    }
                })
                .collect();

            let mut toggle_all = None;
            let mut toggle_one: Vec<(String, bool)> = Vec::new();
            let mut navigate_to: Option<String> = None;
            let mut open_folder_path: Option<String> = None;

            // Header
            ui.horizontal(|ui| {
                let mut all_sel = all_selected;
                if ui.checkbox(&mut all_sel, "").changed() {
                    toggle_all = Some(all_sel);
                }
                ui.label(egui::RichText::new("Name").strong());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(egui::RichText::new("Date").strong());
                    ui.add_space(60.0);
                    ui.label(egui::RichText::new("Size").strong());
                    ui.add_space(40.0);
                });
            });
            ui.separator();

            // File rows
            egui::ScrollArea::vertical().show(ui, |ui| {
                for row in &rows {
                    let resp = ui.horizontal(|ui| {
                        let mut sel = row.selected;
                        if ui.checkbox(&mut sel, "").changed() {
                            toggle_one.push((row.select_key.clone(), sel));
                        }

                        let icon = if row.is_folder {
                            "\u{1F4C1} "
                        } else {
                            "\u{1F4C4} "
                        };
                        let label = format!("{}{}", icon, row.display_name);
                        let resp = if row.is_folder {
                            ui.link(egui::RichText::new(&label))
                        } else {
                            ui.label(&label);
                            ui.response()
                        };

                        if row.is_folder && resp.clicked() {
                            navigate_to = Some(row.name.clone());
                        }

                        ui.with_layout(
                            egui::Layout::right_to_left(egui::Align::Center),
                            |ui| {
                                ui.label(
                                    egui::RichText::new(&row.date_str)
                                        .color(egui::Color32::GRAY)
                                        .small(),
                                );
                                ui.add_space(30.0);
                                ui.label(
                                    egui::RichText::new(&row.size_str)
                                        .color(egui::Color32::LIGHT_GRAY),
                                );
                                // In search mode, show "Open Folder" button
                                if self.searching && !row.is_folder {
                                    ui.add_space(10.0);
                                    if ui.small_button("\u{1F4C2} Open Folder").clicked() {
                                        // Extract parent folder from full path
                                        if let Some(pos) = row.name.rfind('/') {
                                            open_folder_path =
                                                Some(row.name[..=pos].to_string());
                                        }
                                    }
                                }
                            },
                        );
                    });

                    // Double-click to download a file
                    if !row.is_folder && resp.response.double_clicked() {
                        let remote = row.name.clone();
                        let default_name = row.display_name.clone();
                        let tx = self.tx.clone();
                        std::thread::spawn(move || {
                            if let Some(path) = rfd::FileDialog::new()
                                .set_file_name(&default_name)
                                .save_file()
                            {
                                tx.send(Msg::SavePicked(path, remote)).ok();
                            }
                        });
                    }
                }
            });

            // Apply deferred changes
            if let Some(select_all) = toggle_all {
                for row in &rows {
                    if select_all {
                        self.selected_files.insert(row.select_key.clone());
                    } else {
                        self.selected_files.remove(&row.select_key);
                    }
                }
            }
            for (fid, sel) in toggle_one {
                if sel {
                    self.selected_files.insert(fid);
                } else {
                    self.selected_files.remove(&fid);
                }
            }
            if let Some(folder) = navigate_to {
                // Clear search when navigating into a folder
                if self.searching {
                    self.last_search = self.search.clone();
                    self.searching = false;
                    self.search.clear();
                    self.nav_stack.clear();
                    // Build nav_stack from full path
                    self.nav_stack = folder
                        .split('/')
                        .filter(|s| !s.is_empty())
                        .map(|s| format!("{}/", s))
                        .collect();
                } else {
                    let seg = folder
                        .strip_prefix(&self.current_prefix)
                        .unwrap_or(&folder)
                        .to_string();
                    self.nav_stack.push(seg);
                }
                self.current_prefix = folder;
                self.selected_files.clear();
                self.loading = true;
                self.spawn_load_files();
            }

            // Navigate to folder from search results
            if let Some(folder_path) = open_folder_path {
                self.search.clear();
                self.searching = false;
                self.current_prefix = folder_path.clone();
                // Build nav_stack from path segments
                self.nav_stack = folder_path
                    .split('/')
                    .filter(|s| !s.is_empty())
                    .map(|s| format!("{}/", s))
                    .collect();
                self.selected_files.clear();
                self.loading = true;
                self.spawn_load_files();
            }
        });
    }
}

// ── Context menu install/uninstall ──

fn run_reg(args: &[&str]) -> Result<(), String> {
    let out = std::process::Command::new("reg")
        .args(args)
        .output()
        .map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(())
}

fn is_context_menu_installed() -> bool {
    let key = r"HKCU\Software\Classes\Directory\shell\B2Upload";
    std::process::Command::new("reg")
        .args(["query", key])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn upload_queue_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".b2storage")
        .join("upload_queue.txt")
}

fn install_context_menu() -> Result<(), String> {
    let exe = std::env::current_exe()
        .map_err(|e| e.to_string())?
        .to_string_lossy()
        .to_string();

    let key = r"HKCU\Software\Classes\Directory\shell\B2Upload";
    let cmd_key = r"HKCU\Software\Classes\Directory\shell\B2Upload\command";
    // --queue writes to file and exits instantly (no new window)
    let cmd_val = format!(r#""{}" --queue "%V""#, exe);

    run_reg(&["add", key, "/ve", "/d", "Upload to B2 Storage", "/f"])?;
    run_reg(&["add", key, "/v", "Icon", "/d", &exe, "/f"])?;
    run_reg(&["add", cmd_key, "/ve", "/d", &cmd_val, "/f"])?;

    Ok(())
}

fn uninstall_context_menu() -> Result<(), String> {
    let key = r"HKCU\Software\Classes\Directory\shell\B2Upload";
    // /f = force, no prompt
    let _ = run_reg(&["delete", key, "/f"]);
    Ok(())
}

// ── Entry point ──

// ── Standalone Picker (tiny window, no main app) ──

enum PickerMsg {
    Buckets(Vec<Bucket>),
    Folders(String, Vec<String>), // (prefix, folders) — prefix to verify no stale data
    Err(String),
    ReloadFolders,
}

struct PickerApp {
    rt: tokio::runtime::Runtime,
    tx: mpsc::Sender<PickerMsg>,
    rx: mpsc::Receiver<PickerMsg>,
    client: Option<B2Client>,
    buckets: Vec<Bucket>,
    selected_bucket: Option<usize>,
    folders: Vec<String>,
    folders_loaded: bool,
    prefix: String,
    upload_path: PathBuf,
    loading: bool,
    done: bool,
    error: Option<String>,
    new_folder_name: String,
    show_new_folder: bool,
}

impl PickerApp {
    fn new(upload_path: PathBuf) -> Self {
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let (tx, rx) = mpsc::channel();

        let mut app = Self {
            rt,
            tx,
            rx,
            client: None,
            buckets: Vec::new(),
            selected_bucket: None,
            folders: Vec::new(),
            folders_loaded: false,
            prefix: String::new(),
            upload_path,
            loading: true,
            done: false,
            error: None,
            new_folder_name: String::new(),
            show_new_folder: false,
        };

        // Load cached auth and start loading buckets async
        if let Ok(auth) = config::load_auth_cache() {
            let cfg = config::load_config().unwrap_or(config::Config {
                key_id: String::new(),
                app_key: String::new(),
            });
            let client = B2Client::new(auth, cfg.key_id, cfg.app_key);
            app.client = Some(client.clone());
            let tx = app.tx.clone();
            app.rt.spawn(async move {
                match client.list_buckets().await {
                    Ok(b) => { tx.send(PickerMsg::Buckets(b)).ok(); }
                    Err(e) => { tx.send(PickerMsg::Err(e.to_string())).ok(); }
                }
            });
        } else {
            app.error = Some("Not authenticated. Open B2 Storage Manager first.".into());
            app.loading = false;
        }

        app
    }

    fn load_folders(&mut self) {
        self.folders_loaded = false;
        self.folders.clear();
        let Some(client) = self.client.clone() else { return };
        let Some(idx) = self.selected_bucket else { return };
        let Some(bucket) = self.buckets.get(idx) else { return };
        let bucket_id = bucket.bucket_id.clone();
        let prefix = self.prefix.clone();
        let tx = self.tx.clone();
        let resp_prefix = prefix.clone();
        self.rt.spawn(async move {
            let p = if prefix.is_empty() { None } else { Some(prefix.as_str()) };
            match client.list_files(&bucket_id, p, Some("/")).await {
                Ok(files) => {
                    let folders: Vec<String> = files
                        .iter()
                        .filter(|f| f.action == "folder")
                        .map(|f| f.file_name.clone())
                        .collect();
                    tx.send(PickerMsg::Folders(resp_prefix, folders)).ok();
                }
                Err(_) => { tx.send(PickerMsg::Folders(resp_prefix, Vec::new())).ok(); }
            }
        });
    }

    fn process(&mut self) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                PickerMsg::Buckets(b) => {
                    self.buckets = b;
                    self.loading = false;
                    if self.buckets.len() == 1 {
                        self.selected_bucket = Some(0);
                        self.load_folders();
                    }
                }
                PickerMsg::Folders(prefix, f) => {
                    // Only apply if prefix matches current (ignore stale responses)
                    if prefix == self.prefix {
                        self.folders = f;
                        self.folders_loaded = true;
                        self.loading = false;
                    }
                }
                PickerMsg::ReloadFolders => {
                    self.load_folders();
                }
                PickerMsg::Err(e) => {
                    self.error = Some(e);
                    self.loading = false;
                }
            }
        }
    }
}

impl eframe::App for PickerApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.process();

        if self.done {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
            return;
        }

        // Keep repainting while loading
        if self.loading {
            ctx.request_repaint_after(std::time::Duration::from_millis(50));
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Upload to B2 Storage");
            ui.separator();

            if self.loading {
                ui.vertical_centered(|ui| {
                    ui.add_space(20.0);
                    ui.spinner();
                    ui.label("Connecting to B2...");
                });
                return;
            }

            if let Some(ref err) = self.error {
                ui.colored_label(egui::Color32::from_rgb(255, 80, 80), err);
                ui.add_space(8.0);
                if ui.button("Close").clicked() {
                    self.done = true;
                }
                return;
            }

            // Show upload source
            let folder_name = self.upload_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or("?".into());
            ui.label(egui::RichText::new(format!("Folder: {}", folder_name)).strong());
            ui.add_space(4.0);

            // Bucket selector (if multiple)
            let mut bucket_changed = false;
            if self.buckets.len() > 1 {
                ui.label("Bucket:");
                let names: Vec<String> = self.buckets.iter().map(|b| b.bucket_name.clone()).collect();
                for (i, name) in names.iter().enumerate() {
                    let sel = self.selected_bucket == Some(i);
                    if ui.selectable_label(sel, name).clicked() && !sel {
                        self.selected_bucket = Some(i);
                        self.prefix.clear();
                        bucket_changed = true;
                    }
                }
                ui.add_space(4.0);
            }
            if bucket_changed {
                self.load_folders();
            }

            // Load folders when bucket first selected
            if self.selected_bucket.is_some() && !self.folders_loaded && !self.loading {
                self.load_folders();
            }

            // Destination display
            let dest = if self.prefix.is_empty() {
                "/ (root)".to_string()
            } else {
                format!("/{}", self.prefix)
            };
            ui.horizontal(|ui| {
                ui.label("Destination:");
                ui.label(
                    egui::RichText::new(&dest)
                        .color(egui::Color32::from_rgb(100, 200, 100))
                        .strong(),
                );
            });
            ui.separator();

            if self.selected_bucket.is_none() {
                ui.label(
                    egui::RichText::new("Select a bucket above")
                        .color(egui::Color32::GRAY)
                        .italics(),
                );
            } else if !self.folders_loaded {
                // Loading folders
                ui.horizontal(|ui| {
                    ui.spinner();
                    ui.label("Loading folders...");
                });
            } else {
                // Navigation
                ui.horizontal(|ui| {
                    if !self.prefix.is_empty() {
                        if ui.small_button("\u{2B06} Up").clicked() {
                            let trimmed = self.prefix.trim_end_matches('/');
                            if let Some(pos) = trimmed.rfind('/') {
                                self.prefix = trimmed[..=pos].to_string();
                            } else {
                                self.prefix.clear();
                            }
                            self.load_folders();
                        }
                    }
                    if ui.small_button("\u{2795} New Folder").clicked() {
                        self.show_new_folder = !self.show_new_folder;
                        self.new_folder_name.clear();
                    }
                });

                // New folder input
                if self.show_new_folder {
                    ui.horizontal(|ui| {
                        let enter = ui
                            .add(
                                egui::TextEdit::singleline(&mut self.new_folder_name)
                                    .desired_width(200.0)
                                    .hint_text("folder name"),
                            )
                            .lost_focus()
                            && ui.input(|i| i.key_pressed(egui::Key::Enter));

                        if (ui.small_button("Create").clicked() || enter)
                            && !self.new_folder_name.is_empty()
                        {
                            // Create folder by uploading .bzEmpty
                            let folder = self.new_folder_name.trim().trim_matches('/').to_string();
                            let remote = if self.prefix.is_empty() {
                                format!("{}/.bzEmpty", folder)
                            } else {
                                format!("{}{}/.bzEmpty", self.prefix, folder)
                            };
                            if let Some(client) = self.client.clone() {
                                if let Some(idx) = self.selected_bucket {
                                    if let Some(bucket) = self.buckets.get(idx) {
                                        let bucket_id = bucket.bucket_id.clone();
                                        let tx = self.tx.clone();
                                        self.rt.spawn(async move {
                                            if let Ok(url) = client.get_upload_url(&bucket_id).await {
                                                let encoded = client::percent_encode_filename_pub(&remote);
                                                let _ = client.http_client()
                                                    .post(&url.upload_url)
                                                    .header("Authorization", &url.authorization_token)
                                                    .header("X-Bz-File-Name", &encoded)
                                                    .header("Content-Type", "application/octet-stream")
                                                    .header("Content-Length", "0")
                                                    .header("X-Bz-Content-Sha1", "da39a3ee5e6b4b0d3255bfef95601890afd80709")
                                                    .body(vec![])
                                                    .send()
                                                    .await;
                                            }
                                            // Reload folders after creating
                                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                            tx.send(PickerMsg::ReloadFolders).ok();
                                        });
                                    }
                                }
                            }
                            self.show_new_folder = false;
                            self.new_folder_name.clear();
                            self.folders_loaded = false; // will reload
                        }
                    });
                }

                // Folder list
                egui::ScrollArea::vertical().max_height(180.0).show(ui, |ui| {
                    if self.folders.is_empty() {
                        ui.label(
                            egui::RichText::new("(no subfolders - upload here or create one)")
                                .color(egui::Color32::GRAY)
                                .italics(),
                        );
                    }
                    let mut nav = None;
                    for folder in &self.folders {
                        let display = folder
                            .strip_prefix(&self.prefix)
                            .unwrap_or(folder)
                            .trim_end_matches('/');
                        if ui.link(format!("\u{1F4C1} {}/", display)).clicked() {
                            nav = Some(folder.clone());
                        }
                    }
                    if let Some(f) = nav {
                        self.prefix = f;
                        self.load_folders();
                    }
                });
            }

            ui.add_space(8.0);
            ui.separator();
            ui.horizontal(|ui| {
                let can_upload = self.selected_bucket.is_some();
                if ui
                    .add_enabled(can_upload, egui::Button::new("\u{2B06} Upload Here"))
                    .clicked()
                {
                    // Write to queue: path|destination
                    let queue = upload_queue_path();
                    if let Some(parent) = queue.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    let line = format!(
                        "{}|{}\n",
                        self.upload_path.display(),
                        self.prefix
                    );
                    let mut existing = std::fs::read_to_string(&queue).unwrap_or_default();
                    existing.push_str(&line);
                    let _ = std::fs::write(&queue, existing);
                    self.done = true;
                }
                if ui.button("Cancel").clicked() {
                    self.done = true;
                }
            });
        });
    }
}

// ── Crash log helper ──

fn log_crash(msg: &str) {
    let path = std::env::current_exe()
        .unwrap_or_default()
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join("b2storage_crash.log");
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S");
    let line = format!("[{}] {}\n", timestamp, msg);
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
}

// ── Rendering backend selection ──
//
// On a bare-metal Windows box the GPU driver ships a real OpenGL ICD and
// everything just works. On a VPS or RDP session the only display adapter is a
// virtual one (Hyper-V Video, VMware SVGA, RDP mirror, Microsoft Basic Render)
// and none of those expose OpenGL 2.0+, so glutin fails to create a context and
// the process exits without ever showing a window.
//
// Fix: detect that situation and re-launch ourselves with Mesa's llvmpipe
// software renderer. The re-launch matters: Mesa reads GALLIUM_DRIVER and
// friends through the C runtime getenv(), which snapshots the environment at
// process start, so setting them from inside a running process is unreliable.
// Starting a child with the variables already in its environment always works.

const ENV_SOFTWARE: &str = "B2STORAGE_FORCE_SOFTWARE";
const ENV_HARDWARE: &str = "B2STORAGE_FORCE_HARDWARE";
const ENV_RELAUNCHED: &str = "B2STORAGE_RELAUNCHED";

fn env_flag(name: &str) -> bool {
    match std::env::var(name) {
        Ok(v) => {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "on"
        }
        Err(_) => false,
    }
}

fn exe_dir() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Path to the Mesa `opengl32.dll` shipped next to the exe, if present.
fn mesa_dll_path() -> Option<PathBuf> {
    let p = exe_dir().join("opengl32.dll");
    if p.is_file() {
        Some(p)
    } else {
        None
    }
}

/// Adapter names that never provide a usable OpenGL ICD.
const VIRTUAL_ADAPTERS: &[&str] = &[
    "microsoft basic",
    "microsoft remote",
    "microsoft hyper-v",
    "hyper-v",
    "remote display",
    "rdp",
    "remotefx",
    "vmware",
    "virtualbox",
    "vbox",
    "qemu",
    "qxl",
    "red hat",
    "virtio",
    "xen",
    "parallels",
    "citrix",
    "standard vga",
    "bochs",
    "cirrus",
    "spice",
    "parsec",
    "displaylink",
    "nutanix",
    "proxmox",
];

/// Returns true when no attached display adapter can be expected to provide
/// hardware OpenGL, meaning we should render with Mesa instead.
fn needs_software_rendering() -> bool {
    const ATTACHED_TO_DESKTOP: u32 = 0x0000_0001;
    const MIRRORING_DRIVER: u32 = 0x0000_0008;
    // SM_REMOTESESSION: running inside an RDP session, where the physical GPU
    // (if any) is not reachable.
    const SM_REMOTESESSION: i32 = 0x1000;

    unsafe {
        #[repr(C)]
        struct DisplayDeviceW {
            cb: u32,
            device_name: [u16; 32],
            device_string: [u16; 128],
            state_flags: u32,
            device_id: [u16; 128],
            device_key: [u16; 128],
        }

        #[link(name = "user32")]
        extern "system" {
            fn EnumDisplayDevicesW(
                device: *const u16,
                dev_num: u32,
                display_device: *mut DisplayDeviceW,
                flags: u32,
            ) -> i32;
            fn GetSystemMetrics(index: i32) -> i32;
        }

        let remote_session = GetSystemMetrics(SM_REMOTESESSION) != 0;

        let mut found_real_gpu = false;
        let mut adapters = 0usize;
        let mut i = 0u32;

        loop {
            let mut dd: DisplayDeviceW = std::mem::zeroed();
            dd.cb = std::mem::size_of::<DisplayDeviceW>() as u32;

            if EnumDisplayDevicesW(std::ptr::null(), i, &mut dd, 0) == 0 {
                break;
            }
            i += 1;

            if dd.state_flags & MIRRORING_DRIVER != 0 {
                continue;
            }
            if dd.state_flags & ATTACHED_TO_DESKTOP == 0 {
                continue;
            }

            let end = dd
                .device_string
                .iter()
                .position(|&c| c == 0)
                .unwrap_or(dd.device_string.len());
            let name = String::from_utf16_lossy(&dd.device_string[..end]);
            let name = name.trim();
            if name.is_empty() {
                continue;
            }
            adapters += 1;

            let lowered = name.to_ascii_lowercase();
            let virtualized = VIRTUAL_ADAPTERS.iter().any(|pat| lowered.contains(pat));
            log_crash(&format!(
                "Display adapter: {} ({})",
                name,
                if virtualized { "virtual" } else { "real GPU" }
            ));
            if !virtualized {
                found_real_gpu = true;
            }
        }

        if adapters == 0 {
            log_crash("No attached display adapter found, assuming software rendering");
            return true;
        }
        if remote_session {
            log_crash("Running in a remote session, using software rendering");
            return true;
        }
        !found_real_gpu
    }
}

/// Mesa binaries compiled into this exe by build.rs. Empty when nothing was
/// vendored at build time.
mod mesa_payload {
    include!(concat!(env!("OUT_DIR"), "/mesa_payload.rs"));
}

/// Read the `Machine` field out of a PE file header so an architecture
/// mismatch can be reported precisely instead of guessed at.
fn pe_machine(path: &std::path::Path) -> Option<u16> {
    let data = std::fs::read(path).ok()?;
    if data.len() < 0x40 || &data[0..2] != b"MZ" {
        return None;
    }
    let e_lfanew = u32::from_le_bytes(data[0x3C..0x40].try_into().ok()?) as usize;
    if data.len() < e_lfanew + 6 || &data[e_lfanew..e_lfanew + 4] != b"PE\0\0" {
        return None;
    }
    Some(u16::from_le_bytes(
        data[e_lfanew + 4..e_lfanew + 6].try_into().ok()?,
    ))
}

fn machine_name(machine: u16) -> &'static str {
    match machine {
        0x014c => "x86 (32-bit)",
        0x8664 => "x86-64 (64-bit)",
        0xaa64 => "ARM64",
        0x01c4 => "ARM (32-bit)",
        _ => "unknown",
    }
}

/// The architecture this exe was built for.
fn own_machine() -> u16 {
    if cfg!(target_arch = "x86_64") {
        0x8664
    } else if cfg!(target_arch = "x86") {
        0x014c
    } else if cfg!(target_arch = "aarch64") {
        0xaa64
    } else {
        0
    }
}

/// Mesa's MSVC builds link against the Visual C++ runtime, which a freshly
/// installed Windows Server does not have. Report which pieces are missing so
/// the fix is obvious from the log. The bundled build is MinGW and needs none
/// of these, so this only fires for a user-supplied DLL.
fn missing_msvc_runtime() -> Vec<&'static str> {
    const NEEDED: &[&str] = &["vcruntime140.dll", "vcruntime140_1.dll", "msvcp140.dll"];
    let mut missing = Vec::new();
    unsafe {
        use std::os::windows::ffi::OsStrExt;
        #[link(name = "kernel32")]
        extern "system" {
            fn LoadLibraryW(name: *const u16) -> *mut std::ffi::c_void;
        }
        for name in NEEDED {
            let wide: Vec<u16> = std::ffi::OsStr::new(name)
                .encode_wide()
                .chain(Some(0))
                .collect();
            if LoadLibraryW(wide.as_ptr()).is_null() {
                missing.push(*name);
            }
        }
    }
    missing
}

fn load_error_hint(code: u32) -> &'static str {
    match code {
        126 => "ERROR_MOD_NOT_FOUND: the DLL itself or one of its dependencies is missing",
        193 => "ERROR_BAD_EXE_FORMAT: the DLL is built for a different CPU architecture",
        5 => "ERROR_ACCESS_DENIED: the file is blocked or unreadable",
        1114 => "ERROR_DLL_INIT_FAILED: the DLL loaded but its initialisation failed",
        _ => "see the Windows system error code for details",
    }
}

/// Is a software renderer obtainable at all, either bundled or dropped next to
/// the exe by the user?
fn mesa_available() -> bool {
    mesa_dll_path().is_some() || !mesa_payload::MESA_FILES.is_empty()
}

/// Where the bundled Mesa gets unpacked. Keyed by version and architecture so
/// upgrading the app never reuses stale DLLs.
fn mesa_cache_dir() -> PathBuf {
    let base = dirs::data_local_dir().unwrap_or_else(std::env::temp_dir);
    base.join("b2storage").join("mesa").join(format!(
        "{}-{}",
        mesa_payload::MESA_VERSION,
        mesa_payload::MESA_ARCH
    ))
}

/// Unpack the embedded Mesa DLLs into the cache directory, skipping any file
/// that is already there at the right size. Returns the directory holding a
/// usable `opengl32.dll`.
fn unpack_bundled_mesa() -> Option<PathBuf> {
    if mesa_payload::MESA_FILES.is_empty() {
        return None;
    }

    let dir = mesa_cache_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        log_crash(&format!(
            "Cannot create Mesa cache directory {}: {}",
            dir.display(),
            e
        ));
        return None;
    }

    for (name, packed, raw_len) in mesa_payload::MESA_FILES {
        let dest = dir.join(name);

        // Already unpacked by an earlier run or another instance.
        if std::fs::metadata(&dest).map(|m| m.len()).ok() == Some(*raw_len) {
            continue;
        }

        let started = std::time::Instant::now();
        let mut decoded: Vec<u8> = Vec::with_capacity(*raw_len as usize);
        let mut reader = std::io::BufReader::new(*packed);
        if let Err(e) = lzma_rs::xz_decompress(&mut reader, &mut decoded) {
            log_crash(&format!("Failed to decompress bundled {}: {}", name, e));
            return None;
        }
        if decoded.len() as u64 != *raw_len {
            log_crash(&format!(
                "Bundled {} decompressed to {} bytes, expected {}",
                name,
                decoded.len(),
                raw_len
            ));
            return None;
        }

        // Write to a private temp name first so a half-written DLL is never
        // visible to a second instance starting at the same time.
        let tmp = dir.join(format!("{}.{}.tmp", name, std::process::id()));
        if let Err(e) = std::fs::write(&tmp, &decoded) {
            log_crash(&format!("Failed to write {}: {}", tmp.display(), e));
            return None;
        }
        let _ = std::fs::remove_file(&dest);
        if let Err(e) = std::fs::rename(&tmp, &dest) {
            let _ = std::fs::remove_file(&tmp);
            // Another instance may have won the race and the file is now in
            // use. That is fine as long as the result is the right size.
            if std::fs::metadata(&dest).map(|m| m.len()).ok() != Some(*raw_len) {
                log_crash(&format!("Failed to place {}: {}", dest.display(), e));
                return None;
            }
            continue;
        }

        log_crash(&format!(
            "Unpacked bundled {} ({} bytes) in {} ms",
            name,
            raw_len,
            started.elapsed().as_millis()
        ));
    }

    log_crash(&format!(
        "Bundled Mesa {} ({}) ready at {}",
        mesa_payload::MESA_VERSION,
        mesa_payload::MESA_ARCH,
        dir.display()
    ));
    Some(dir)
}

/// Decide which Mesa to use. A DLL the user dropped next to the exe wins, so a
/// newer or different build can be tried without rebuilding the app.
fn provision_mesa() -> Option<PathBuf> {
    if let Some(p) = mesa_dll_path() {
        log_crash(&format!(
            "Using opengl32.dll found next to the exe: {}",
            p.display()
        ));
        return p.parent().map(|d| d.to_path_buf());
    }
    unpack_bundled_mesa()
}

/// Load Mesa's `opengl32.dll` from `dir` so that the delay-loaded `opengl32`
/// imports resolve to it instead of the one in System32.
fn load_mesa_from(dir: &std::path::Path) -> bool {
    let mesa_dll = dir.join("opengl32.dll");
    if !mesa_dll.is_file() {
        log_crash(&format!("No opengl32.dll in {}", dir.display()));
        return false;
    }

    let size = std::fs::metadata(&mesa_dll).map(|m| m.len()).unwrap_or(0);
    log_crash(&format!(
        "Mesa candidate: {} ({} bytes)",
        mesa_dll.display(),
        size
    ));

    // A 32-bit DLL next to a 64-bit exe can never load. Say so before trying.
    match pe_machine(&mesa_dll) {
        Some(machine) => {
            log_crash(&format!(
                "Mesa opengl32.dll architecture: {} (this exe is {})",
                machine_name(machine),
                machine_name(own_machine())
            ));
            if machine != own_machine() {
                log_crash(
                    "Architecture mismatch. Remove that opengl32.dll to use the bundled Mesa, \
                     or replace it with a build matching this exe.",
                );
                return false;
            }
        }
        None => {
            log_crash("Mesa opengl32.dll is not a valid PE file (truncated or wrong file?)");
            return false;
        }
    }

    unsafe {
        use std::os::windows::ffi::OsStrExt;
        const LOAD_WITH_ALTERED_SEARCH_PATH: u32 = 0x0000_0008;

        #[link(name = "kernel32")]
        extern "system" {
            fn LoadLibraryExW(
                name: *const u16,
                file: *mut std::ffi::c_void,
                flags: u32,
            ) -> *mut std::ffi::c_void;
            fn GetModuleFileNameW(module: *mut std::ffi::c_void, buf: *mut u16, size: u32) -> u32;
            fn SetDllDirectoryW(path: *const u16) -> i32;
            fn GetLastError() -> u32;
        }

        // Mesa's opengl32.dll pulls in libgallium_wgl.dll from its own folder.
        let dir_wide: Vec<u16> = dir.as_os_str().encode_wide().chain(Some(0)).collect();
        SetDllDirectoryW(dir_wide.as_ptr());

        let dll_wide: Vec<u16> = mesa_dll.as_os_str().encode_wide().chain(Some(0)).collect();
        let h = LoadLibraryExW(
            dll_wide.as_ptr(),
            std::ptr::null_mut(),
            LOAD_WITH_ALTERED_SEARCH_PATH,
        );
        if h.is_null() {
            let code = GetLastError();
            log_crash(&format!(
                "Failed to load Mesa opengl32.dll: Windows error {} ({})",
                code,
                load_error_hint(code)
            ));

            let missing = missing_msvc_runtime();
            if !missing.is_empty() {
                log_crash(&format!(
                    "Missing Visual C++ runtime DLLs: {}. Install the Microsoft Visual C++ \
                     2015-2022 Redistributable (x64), or delete the opengl32.dll next to the \
                     exe so the bundled MinGW build is used instead.",
                    missing.join(", ")
                ));
            } else {
                log_crash(
                    "The Visual C++ runtime is present, so a Mesa-specific dependency is \
                     missing. libgallium_wgl.dll must sit in the same folder as opengl32.dll.",
                );
            }
            return false;
        }

        let mut path_buf = [0u16; 512];
        let len = GetModuleFileNameW(h, path_buf.as_mut_ptr(), 512);
        if len > 0 {
            log_crash(&format!(
                "Loaded Mesa opengl32.dll from: {}",
                String::from_utf16_lossy(&path_buf[..len as usize])
            ));
        }
    }

    true
}

/// Environment the software-rendering child process needs.
fn software_env() -> [(&'static str, &'static str); 6] {
    [
        (ENV_SOFTWARE, "1"),
        (ENV_RELAUNCHED, "1"),
        ("GALLIUM_DRIVER", "llvmpipe"),
        ("LIBGL_ALWAYS_SOFTWARE", "1"),
        ("MESA_GL_VERSION_OVERRIDE", "4.5"),
        ("MESA_GLSL_VERSION_OVERRIDE", "450"),
    ]
}

/// Restart this exe with Mesa software rendering forced on.
fn relaunch_software(args: &[String]) -> bool {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            log_crash(&format!("Cannot locate own exe for relaunch: {}", e));
            return false;
        }
    };

    let mut cmd = std::process::Command::new(&exe);
    cmd.args(&args[1..]);
    for (k, v) in software_env() {
        cmd.env(k, v);
    }
    cmd.current_dir(exe_dir());

    match cmd.spawn() {
        Ok(child) => {
            log_crash(&format!(
                "Relaunched in software-rendering mode (pid {})",
                child.id()
            ));
            true
        }
        Err(e) => {
            log_crash(&format!("Relaunch failed: {}", e));
            false
        }
    }
}

/// Log the OpenGL strings we actually ended up with, so a remote machine can be
/// diagnosed from the crash log alone.
fn log_gl_info(cc: &eframe::CreationContext<'_>) {
    use eframe::glow::HasContext as _;
    if let Some(gl) = cc.gl.as_ref() {
        unsafe {
            log_crash(&format!(
                "OpenGL vendor={} renderer={} version={}",
                gl.get_parameter_string(eframe::glow::VENDOR),
                gl.get_parameter_string(eframe::glow::RENDERER),
                gl.get_parameter_string(eframe::glow::VERSION),
            ));
        }
    } else {
        log_crash("No glow context on CreationContext");
    }
}

// ── Entry point ──

fn main() -> eframe::Result {
    // Install the panic hook before anything else so early failures are logged.
    std::panic::set_hook(Box::new(|info| {
        log_crash(&format!("PANIC: {}", info));
    }));

    let args: Vec<String> = std::env::args().collect();

    let already_relaunched = env_flag(ENV_RELAUNCHED);
    let force_hardware = env_flag(ENV_HARDWARE);
    let force_software = env_flag(ENV_SOFTWARE);

    // Decide up front whether this machine can do hardware OpenGL at all.
    if !already_relaunched && !force_hardware {
        let want_software = force_software || needs_software_rendering();
        if want_software {
            if mesa_available() {
                if relaunch_software(&args) {
                    return Ok(());
                }
            } else {
                log_crash(
                    "Software rendering is required but this build has no bundled Mesa and no \
                     opengl32.dll sits next to the exe.",
                );
            }
        }
    }

    // In the relaunched child, pull Mesa in before glutin touches opengl32.
    let software = already_relaunched && force_software;
    if software {
        match provision_mesa() {
            Some(dir) => {
                if !load_mesa_from(&dir) {
                    log_crash("Continuing without Mesa; context creation will probably fail");
                }
            }
            None => log_crash("No Mesa renderer available; the window will fail to open"),
        }
    }

    // Always ask for the preferred config. Requesting HardwareAcceleration::Off
    // makes glutin demand WGL_NO_ACCELERATION_ARB, which Mesa's ICD never
    // advertises, so the config search would come back empty.
    let hw_accel = eframe::HardwareAcceleration::Preferred;

    // --queue "path": open ONLY a tiny picker window (no main app)
    if args.len() >= 3 && args[1] == "--queue" {
        let path = PathBuf::from(&args[2]);
        let options = eframe::NativeOptions {
            viewport: egui::ViewportBuilder::default()
                .with_inner_size([420.0, 450.0])
                .with_always_on_top()
                .with_active(true)
                .with_resizable(false)
                .with_maximize_button(false)
                .with_minimize_button(false),
            centered: true,
            renderer: eframe::Renderer::Glow,
            hardware_acceleration: hw_accel,
            multisampling: 0,
            depth_buffer: 0,
            stencil_buffer: 0,
            vsync: false,
            ..Default::default()
        };
        let result = eframe::run_native(
            "Upload to B2 Storage",
            options,
            Box::new(move |cc| {
                log_gl_info(cc);
                cc.egui_ctx.set_visuals(egui::Visuals::dark());
                Ok(Box::new(PickerApp::new(path)))
            }),
        );
        if let Err(ref e) = result {
            log_crash(&format!("eframe picker error: {}", e));
            if !software && !already_relaunched && mesa_available() {
                log_crash("Retrying picker with software rendering");
                if relaunch_software(&args) {
                    return Ok(());
                }
            }
        }
        return result;
    }

    // --upload "path": open main GUI with upload picker
    let upload_path = if args.len() >= 3 && args[1] == "--upload" {
        Some(PathBuf::from(&args[2]))
    } else {
        None
    };

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1100.0, 700.0])
            .with_min_inner_size([800.0, 500.0]),
        renderer: eframe::Renderer::Glow,
        hardware_acceleration: hw_accel,
        multisampling: 0,
        depth_buffer: 0,
        stencil_buffer: 0,
        vsync: false,
        ..Default::default()
    };
    log_crash(&format!(
        "Starting eframe::run_native (software={}, relaunched={})...",
        software, already_relaunched
    ));
    let result = eframe::run_native(
        "B2 Storage Manager",
        options,
        Box::new(move |cc| {
            log_gl_info(cc);
            cc.egui_ctx.set_visuals(egui::Visuals::dark());
            let mut app = App::new(cc);
            if let Some(path) = upload_path {
                app.pending_upload = Some(path);
            }
            Ok(Box::new(app))
        }),
    );
    if let Err(ref e) = result {
        log_crash(&format!("eframe main error: {}", e));
        // Hardware path failed after all: fall back to Mesa and try again.
        if !software && !already_relaunched && mesa_available() {
            log_crash("Retrying with software rendering");
            if relaunch_software(&args) {
                return Ok(());
            }
        }
    }
    result
}
