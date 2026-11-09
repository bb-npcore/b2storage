use anyhow::{bail, Context, Result};
use futures::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use sha1::{Digest, Sha1};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

use crate::config::{self, AuthCache};
use tokio::sync::RwLock;

fn deserialize_null_u64<'de, D>(d: D) -> std::result::Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<u64> = Option::deserialize(d)?;
    Ok(opt.unwrap_or(0))
}

pub type ProgressFn = Arc<dyn Fn(u64, u64) + Send + Sync>;

// ── API response types ──

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bucket {
    pub bucket_id: String,
    pub bucket_name: String,
    pub bucket_type: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBucketsResponse {
    pub buckets: Vec<Bucket>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct FileInfo {
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub file_name: String,
    #[serde(default, deserialize_with = "deserialize_null_u64")]
    pub content_length: u64,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub action: String,
    #[serde(default, deserialize_with = "deserialize_null_u64")]
    pub upload_timestamp: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListFilesResponse {
    pub files: Vec<FileInfo>,
    pub next_file_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadUrlResponse {
    pub upload_url: String,
    pub authorization_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct UploadResponse {
    pub file_id: String,
    pub file_name: String,
    pub content_length: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartLargeFileResponse {
    pub file_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadPartUrlResponse {
    pub upload_url: String,
    pub authorization_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct DeleteFileResponse {
    pub file_id: String,
    pub file_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ApiError {
    message: String,
    code: String,
}

// ── B2 Client ──

#[derive(Clone)]
pub struct B2Client {
    http: Client,
    auth: Arc<RwLock<AuthCache>>,
    key_id: String,
    app_key: String,
}

impl B2Client {
    pub fn new(auth: AuthCache, key_id: String, app_key: String) -> Self {
        Self {
            http: Client::new(),
            auth: Arc::new(RwLock::new(auth)),
            key_id,
            app_key,
        }
    }

    pub async fn get_auth(&self) -> AuthCache {
        self.auth.read().await.clone()
    }

    /// Non-async access for UI contexts. Returns empty string if lock is held.
    pub fn account_id(&self) -> String {
        self.auth
            .try_read()
            .map(|a| a.account_id.clone())
            .unwrap_or_default()
    }

    pub fn http_client(&self) -> &Client {
        &self.http
    }

    async fn check_response(resp: reqwest::Response) -> Result<reqwest::Response> {
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if let Ok(err) = serde_json::from_str::<ApiError>(&body) {
            bail!("B2 error {}: {} ({})", status, err.message, err.code);
        }
        bail!("B2 error {}: {}", status, body);
    }

    /// Send a request with auth, automatically re-authorizing on expired token.
    async fn authed_request(
        &self,
        build: impl Fn(&Client, &AuthCache) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response> {
        let auth = self.auth.read().await.clone();
        let resp = build(&self.http, &auth).send().await?;

        if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Self::check_response(resp).await;
        }

        // Re-authorize and retry on any 401 (expired token, unauthorized, etc.)
        let new_auth = Self::authorize(&self.key_id, &self.app_key).await?;
        *self.auth.write().await = new_auth.clone();

        let resp = build(&self.http, &new_auth).send().await?;
        Self::check_response(resp).await
    }

    // ── Auth ──

    pub async fn authorize(key_id: &str, app_key: &str) -> Result<AuthCache> {
        let client = Client::new();

        let resp = client
            .get("https://api.backblazeb2.com/b2api/v3/b2_authorize_account")
            .basic_auth(key_id, Some(app_key))
            .send()
            .await?;
        let resp = Self::check_response(resp).await?;
        let body = resp.text().await?;
        let v: serde_json::Value = serde_json::from_str(&body)
            .context(format!("Invalid JSON from B2: {}", &body[..body.len().min(300)]))?;

        let storage = v.pointer("/apiInfo/storageApi");

        let get_str = |key: &str| -> String {
            if let Some(s) = storage.and_then(|s| s.get(key)).and_then(|v| v.as_str()) {
                return s.to_string();
            }
            v.get(key).and_then(|v| v.as_str()).unwrap_or("").to_string()
        };
        let get_u64 = |key: &str, default: u64| -> u64 {
            if let Some(n) = storage.and_then(|s| s.get(key)).and_then(|v| v.as_u64()) {
                return n;
            }
            v.get(key).and_then(|v| v.as_u64()).unwrap_or(default)
        };

        let auth_token = v["authorizationToken"]
            .as_str()
            .unwrap_or("")
            .to_string();
        if auth_token.is_empty() {
            bail!(
                "Authorization failed (no token). Response: {}",
                &body[..body.len().min(500)]
            );
        }

        let cache = AuthCache {
            account_id: v["accountId"].as_str().unwrap_or("").to_string(),
            auth_token,
            api_url: get_str("apiUrl"),
            download_url: get_str("downloadUrl"),
            s3_api_url: get_str("s3ApiUrl"),
            recommended_part_size: get_u64("recommendedPartSize", 100_000_000),
            absolute_minimum_part_size: get_u64("absoluteMinimumPartSize", 5_000_000),
        };
        config::save_auth_cache(&cache)?;
        Ok(cache)
    }

    // ── Buckets ──

    pub async fn list_buckets(&self) -> Result<Vec<Bucket>> {
        let resp = self.authed_request(|http, auth| {
            http.post(format!("{}/b2api/v3/b2_list_buckets", auth.api_url))
                .header("Authorization", &auth.auth_token)
                .json(&serde_json::json!({ "accountId": auth.account_id }))
        }).await?;
        let body = resp.text().await?;
        let result: ListBucketsResponse = serde_json::from_str(&body)
            .context(format!("list_buckets parse error: {}", &body[..body.len().min(500)]))?;
        Ok(result.buckets)
    }

    pub async fn create_bucket(&self, name: &str, public: bool) -> Result<Bucket> {
        let bucket_type = if public { "allPublic" } else { "allPrivate" };
        let btype = bucket_type.to_string();
        let bname = name.to_string();
        let resp = self.authed_request(|http, auth| {
            http.post(format!("{}/b2api/v3/b2_create_bucket", auth.api_url))
                .header("Authorization", &auth.auth_token)
                .json(&serde_json::json!({
                    "accountId": auth.account_id,
                    "bucketName": bname,
                    "bucketType": btype,
                }))
        }).await?;
        let body = resp.text().await?;
        serde_json::from_str(&body)
            .context(format!("create_bucket parse error: {}", &body[..body.len().min(500)]))
    }

    pub async fn delete_bucket(&self, bucket_id: &str) -> Result<()> {
        let bid = bucket_id.to_string();
        self.authed_request(|http, auth| {
            http.post(format!("{}/b2api/v3/b2_delete_bucket", auth.api_url))
                .header("Authorization", &auth.auth_token)
                .json(&serde_json::json!({
                    "accountId": auth.account_id,
                    "bucketId": bid,
                }))
        }).await?;
        Ok(())
    }

    // ── List files ──

    pub async fn list_files(
        &self,
        bucket_id: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
    ) -> Result<Vec<FileInfo>> {
        let mut all_files = Vec::new();
        let mut start_file_name: Option<String> = None;
        let bid = bucket_id.to_string();
        let pfx = prefix.unwrap_or("").to_string();
        let delim = delimiter.map(|d| d.to_string());

        loop {
            let sfn = start_file_name.clone();
            let bid = bid.clone();
            let pfx = pfx.clone();
            let delim = delim.clone();
            let resp = self.authed_request(|http, auth| {
                let mut body = serde_json::json!({
                    "bucketId": bid,
                    "maxFileCount": 10000,
                    "prefix": pfx,
                });
                if let Some(ref d) = delim {
                    body["delimiter"] = serde_json::Value::String(d.clone());
                }
                if let Some(ref s) = sfn {
                    body["startFileName"] = serde_json::Value::String(s.clone());
                }
                http.post(format!("{}/b2api/v3/b2_list_file_names", auth.api_url))
                    .header("Authorization", &auth.auth_token)
                    .json(&body)
            }).await?;
            let text = resp.text().await?;
            let result: ListFilesResponse = serde_json::from_str(&text)
                .context(format!("list_files parse error: {}", &text[..text.len().min(500)]))?;
            all_files.extend(result.files);

            match result.next_file_name {
                Some(next) => start_file_name = Some(next),
                None => break,
            }
        }
        Ok(all_files)
    }

    /// Search files across entire bucket, page by page.
    /// Calls `on_results` with accumulated matches after each page.
    /// Stops after scanning `max_pages` pages (each page = 10,000 files).
    pub async fn search_files(
        &self,
        bucket_id: &str,
        query: &str,
        on_results: impl Fn(Vec<FileInfo>, usize) + Send,
        max_pages: usize,
    ) -> Result<(Vec<FileInfo>, Vec<FileInfo>)> {
        // Returns (matched, all_files)
        let query_lower = query.to_lowercase();
        let mut matched = Vec::new();
        let mut all_files = Vec::new();
        let mut start_file_name: Option<String> = None;

        for _ in 0..max_pages {
            let sfn = start_file_name.clone();
            let bid = bucket_id.to_string();
            let resp = self.authed_request(|http, auth| {
                let mut body = serde_json::json!({
                    "bucketId": bid,
                    "maxFileCount": 10000,
                });
                if let Some(ref s) = sfn {
                    body["startFileName"] = serde_json::Value::String(s.clone());
                }
                http.post(format!("{}/b2api/v3/b2_list_file_names", auth.api_url))
                    .header("Authorization", &auth.auth_token)
                    .json(&body)
            }).await?;
            let text = resp.text().await?;
            let result: ListFilesResponse = serde_json::from_str(&text)
                .context("search parse error")?;

            let scanned = all_files.len() + result.files.len();
            for f in result.files {
                if f.file_name.to_lowercase().contains(&query_lower) {
                    matched.push(f.clone());
                }
                all_files.push(f);
            }

            on_results(matched.clone(), scanned);

            match result.next_file_name {
                Some(next) => start_file_name = Some(next),
                None => break,
            }
        }
        Ok((matched, all_files))
    }

    // ── Upload (small file) ──

    pub async fn upload_file(
        &self,
        bucket_id: &str,
        local_path: &Path,
        remote_name: &str,
        on_progress: Option<ProgressFn>,
    ) -> Result<UploadResponse> {
        let file_size = tokio::fs::metadata(local_path).await?.len();

        if file_size > self.get_auth().await.recommended_part_size {
            return self
                .upload_large_file(bucket_id, local_path, remote_name, on_progress)
                .await;
        }

        if let Some(ref cb) = on_progress {
            cb(0, file_size);
        }

        // Read file and compute SHA1
        let data = tokio::fs::read(local_path).await?;
        let mut hasher = Sha1::new();
        hasher.update(&data);
        let sha1_hex = format!("{:x}", hasher.finalize());

        // Get upload URL
        let bid = bucket_id.to_string();
        let resp = self.authed_request(|http, auth| {
            http.get(format!(
                "{}/b2api/v3/b2_get_upload_url?bucketId={}",
                auth.api_url, bid
            ))
            .header("Authorization", &auth.auth_token)
        }).await?;
        let text = resp.text().await?;
        let upload_url: UploadUrlResponse = serde_json::from_str(&text)
            .context(format!("get_upload_url parse error: {}", &text[..text.len().min(500)]))?;

        let encoded_name = percent_encode_filename(remote_name);

        // Stream upload with progress tracking
        let uploaded = Arc::new(AtomicU64::new(0));
        let uploaded_clone = uploaded.clone();
        let progress_cb = on_progress.clone();
        let chunk_size = 64 * 1024; // 64KB chunks
        let chunks: Vec<::bytes::Bytes> = data
            .chunks(chunk_size)
            .map(|c| ::bytes::Bytes::copy_from_slice(c))
            .collect();

        let stream = futures::stream::iter(chunks.into_iter().map(move |chunk: ::bytes::Bytes| {
            let len = chunk.len() as u64;
            let prev = uploaded_clone.fetch_add(len, Ordering::Relaxed);
            if let Some(ref cb) = progress_cb {
                cb(prev + len, file_size);
            }
            Ok::<_, std::io::Error>(chunk)
        }));

        let body = reqwest::Body::wrap_stream(stream);

        let resp = self
            .http
            .post(&upload_url.upload_url)
            .header("Authorization", &upload_url.authorization_token)
            .header("X-Bz-File-Name", &encoded_name)
            .header("Content-Type", "b2/x-auto")
            .header("Content-Length", file_size.to_string())
            .header("X-Bz-Content-Sha1", &sha1_hex)
            .body(body)
            .send()
            .await?;
        let resp = Self::check_response(resp).await?;

        if let Some(ref cb) = on_progress {
            cb(file_size, file_size);
        }
        let text = resp.text().await?;
        serde_json::from_str(&text)
            .context(format!("upload parse error: {}", &text[..text.len().min(500)]))
    }

    // ── Large file upload ──

    async fn upload_large_file(
        &self,
        bucket_id: &str,
        local_path: &Path,
        remote_name: &str,
        on_progress: Option<ProgressFn>,
    ) -> Result<UploadResponse> {
        let file_size = tokio::fs::metadata(local_path).await?.len();
        let part_size = self.get_auth().await.recommended_part_size;
        let num_parts = ((file_size + part_size - 1) / part_size) as u32;
        let encoded_name = percent_encode_filename(remote_name);

        let bid = bucket_id.to_string();
        let ename = encoded_name.clone();
        let resp = self.authed_request(|http, auth| {
            http.post(format!("{}/b2api/v3/b2_start_large_file", auth.api_url))
                .header("Authorization", &auth.auth_token)
                .json(&serde_json::json!({
                    "bucketId": bid,
                    "fileName": ename,
                    "contentType": "b2/x-auto",
                }))
        }).await?;
        let text = resp.text().await?;
        let start: StartLargeFileResponse = serde_json::from_str(&text)
            .context(format!("start_large_file parse error: {}", &text[..text.len().min(500)]))?;

        let mut sha1_array: Vec<String> = Vec::with_capacity(num_parts as usize);
        let mut file = tokio::fs::File::open(local_path).await?;
        let mut uploaded: u64 = 0;

        for part_num in 1..=num_parts {
            let this_part_size = if part_num == num_parts {
                file_size - (part_num as u64 - 1) * part_size
            } else {
                part_size
            };

            let mut buf = vec![0u8; this_part_size as usize];
            file.read_exact(&mut buf).await?;

            let mut hasher = Sha1::new();
            hasher.update(&buf);
            let sha1_hex = format!("{:x}", hasher.finalize());

            let fid = start.file_id.clone();
            let resp = self.authed_request(|http, auth| {
                http.get(format!(
                    "{}/b2api/v3/b2_get_upload_part_url?fileId={}",
                    auth.api_url, fid
                ))
                .header("Authorization", &auth.auth_token)
            }).await?;
            let text = resp.text().await?;
            let part_url: UploadPartUrlResponse = serde_json::from_str(&text)
                .context(format!("get_upload_part_url parse error: {}", &text[..text.len().min(500)]))?;

            let resp = self
                .http
                .post(&part_url.upload_url)
                .header("Authorization", &part_url.authorization_token)
                .header("X-Bz-Part-Number", part_num.to_string())
                .header("Content-Length", this_part_size.to_string())
                .header("X-Bz-Content-Sha1", &sha1_hex)
                .body(buf)
                .send()
                .await?;
            Self::check_response(resp).await?;

            sha1_array.push(sha1_hex);
            uploaded += this_part_size;
            if let Some(ref cb) = on_progress {
                cb(uploaded, file_size);
            }
        }

        let fid = start.file_id.clone();
        let sha1s = sha1_array.clone();
        let resp = self.authed_request(|http, auth| {
            http.post(format!("{}/b2api/v3/b2_finish_large_file", auth.api_url))
                .header("Authorization", &auth.auth_token)
                .json(&serde_json::json!({
                    "fileId": fid,
                    "partSha1Array": sha1s,
                }))
        }).await?;

        let v: serde_json::Value = resp.json().await?;
        Ok(UploadResponse {
            file_id: v["fileId"].as_str().unwrap_or("").to_string(),
            file_name: v["fileName"].as_str().unwrap_or("").to_string(),
            content_length: v["contentLength"].as_u64().unwrap_or(0),
        })
    }

    // ── Download ──

    pub async fn download_file_by_name(
        &self,
        bucket_name: &str,
        file_name: &str,
        dest: &Path,
        on_progress: Option<ProgressFn>,
    ) -> Result<()> {
        let bname = bucket_name.to_string();
        let fname = percent_encode_filename(file_name);
        let resp = self.authed_request(|http, auth| {
            let url = format!("{}/file/{}/{}", auth.download_url, bname, fname);
            http.get(&url).header("Authorization", &auth.auth_token)
        }).await?;
        let total = resp.content_length().unwrap_or(0);

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = tokio::fs::File::create(dest).await?;
        let mut stream = resp.bytes_stream();
        let mut downloaded: u64 = 0;

        use futures::StreamExt;
        use tokio::io::AsyncWriteExt;

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            downloaded += chunk.len() as u64;
            if let Some(ref cb) = on_progress {
                cb(downloaded, total);
            }
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        Ok(())
    }

    /// Batch download with N concurrent workers
    pub async fn download_batch(
        &self,
        bucket_name: &str,
        files: Vec<(String, std::path::PathBuf)>, // (remote_name, local_dest)
        concurrency: usize,
        on_progress: impl Fn(usize, usize, u64, u64, &str) + Send + Sync + 'static,
    ) -> Result<(usize, usize, Vec<String>)> {
        let total = files.len();
        let total_bytes: u64 = 0; // we don't know total until headers arrive
        let done_files = Arc::new(AtomicU64::new(0));
        let done_bytes = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));
        let failed_names: Arc<tokio::sync::Mutex<Vec<String>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let on_progress = Arc::new(on_progress);

        let mut chunks: Vec<Vec<(String, std::path::PathBuf)>> =
            (0..concurrency).map(|_| Vec::new()).collect();
        for (i, file) in files.into_iter().enumerate() {
            chunks[i % concurrency].push(file);
        }

        let mut handles = Vec::new();
        for chunk in chunks {
            if chunk.is_empty() { continue; }
            let client = self.clone();
            let bucket = bucket_name.to_string();
            let done_files = done_files.clone();
            let done_bytes = done_bytes.clone();
            let failed = failed.clone();
            let failed_names = failed_names.clone();
            let on_progress = on_progress.clone();

            let handle = tokio::spawn(async move {
                for (remote, dest) in &chunk {
                    match client
                        .download_file_by_name(&bucket, remote, dest, None)
                        .await
                    {
                        Ok(_) => {
                            done_files.fetch_add(1, Ordering::Relaxed);
                            let size = tokio::fs::metadata(dest)
                                .await.map(|m| m.len()).unwrap_or(0);
                            done_bytes.fetch_add(size, Ordering::Relaxed);
                        }
                        Err(_) => {
                            failed.fetch_add(1, Ordering::Relaxed);
                            failed_names.lock().await.push(remote.clone());
                        }
                    }
                    let df = done_files.load(Ordering::Relaxed) as usize;
                    let db = done_bytes.load(Ordering::Relaxed);
                    let short = remote.rsplit('/').next().unwrap_or(remote);
                    on_progress(df, total, db, total_bytes, short);
                }
            });
            handles.push(handle);
        }

        for h in handles {
            let _ = h.await;
        }

        let succeeded = done_files.load(Ordering::Relaxed) as usize;
        let fail_count = failed.load(Ordering::Relaxed) as usize;
        let failed_list = failed_names.lock().await.clone();
        Ok((succeeded, fail_count, failed_list))
    }

    // ── Delete ──

    pub async fn delete_file(
        &self,
        file_id: &str,
        file_name: &str,
    ) -> Result<DeleteFileResponse> {
        let fid = file_id.to_string();
        let fname = file_name.to_string();
        let resp = self.authed_request(|http, auth| {
            http.post(format!("{}/b2api/v3/b2_delete_file_version", auth.api_url))
                .header("Authorization", &auth.auth_token)
                .json(&serde_json::json!({
                    "fileId": fid,
                    "fileName": fname,
                }))
        }).await?;
        let text = resp.text().await?;
        serde_json::from_str(&text)
            .context(format!("delete parse error: {}", &text[..text.len().min(500)]))
    }

    // ── Get upload URL (reusable) ──

    pub async fn get_upload_url(&self, bucket_id: &str) -> Result<UploadUrlResponse> {
        let bid = bucket_id.to_string();
        let resp = self.authed_request(|http, auth| {
            http.get(format!(
                "{}/b2api/v3/b2_get_upload_url?bucketId={}",
                auth.api_url, bid
            ))
            .header("Authorization", &auth.auth_token)
        }).await?;
        let text = resp.text().await?;
        serde_json::from_str(&text).context("get_upload_url parse error")
    }

    // ── Upload file using a pre-fetched upload URL (fast, no extra API call) ──

    pub async fn upload_file_with_url(
        &self,
        upload_url: &UploadUrlResponse,
        local_path: &Path,
        remote_name: &str,
    ) -> Result<UploadResponse> {
        let data = tokio::fs::read(local_path).await?;
        let file_size = data.len() as u64;

        let mut hasher = Sha1::new();
        hasher.update(&data);
        let sha1_hex = format!("{:x}", hasher.finalize());

        let encoded_name = percent_encode_filename(remote_name);

        let resp = self
            .http
            .post(&upload_url.upload_url)
            .header("Authorization", &upload_url.authorization_token)
            .header("X-Bz-File-Name", &encoded_name)
            .header("Content-Type", "b2/x-auto")
            .header("Content-Length", file_size.to_string())
            .header("X-Bz-Content-Sha1", &sha1_hex)
            .body(data)
            .send()
            .await?;
        let resp = Self::check_response(resp).await?;
        let text = resp.text().await?;
        serde_json::from_str(&text).context("upload parse error")
    }

    /// Batch upload with N persistent workers.
    /// Each worker gets ONE upload URL and reuses it for all its files (B2 recommended).
    /// Only fetches a new URL if the old one gets rejected.
    pub async fn upload_batch(
        &self,
        bucket_id: &str,
        files: Vec<(std::path::PathBuf, String)>,
        concurrency: usize,
        on_progress: impl Fn(usize, usize, u64, u64, &str) + Send + Sync + 'static,
    ) -> Result<(usize, usize, Vec<String>)> {
        let total = files.len();
        let total_bytes: u64 = files
            .iter()
            .map(|(p, _)| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum();

        let done_files = Arc::new(AtomicU64::new(0));
        let done_bytes = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));
        let failed_names: Arc<tokio::sync::Mutex<Vec<String>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let on_progress = Arc::new(on_progress);

        // Split files into chunks for each worker
        let mut chunks: Vec<Vec<(std::path::PathBuf, String)>> =
            (0..concurrency).map(|_| Vec::new()).collect();
        for (i, file) in files.into_iter().enumerate() {
            chunks[i % concurrency].push(file);
        }

        let mut handles = Vec::new();

        for chunk in chunks {
            if chunk.is_empty() {
                continue;
            }
            let client = self.clone();
            let bucket_id = bucket_id.to_string();
            let done_files = done_files.clone();
            let done_bytes = done_bytes.clone();
            let failed = failed.clone();
            let failed_names = failed_names.clone();
            let on_progress = on_progress.clone();

            let handle = tokio::spawn(async move {
                // Each worker gets ONE upload URL and reuses it
                let mut url = match client.get_upload_url(&bucket_id).await {
                    Ok(u) => u,
                    Err(_) => return,
                };

                for (path, remote) in &chunk {
                    let file_size = tokio::fs::metadata(&path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);

                    // Upload with current URL
                    let mut success = client
                        .upload_file_with_url(&url, path, remote)
                        .await
                        .is_ok();

                    // If failed, get new URL and retry once
                    if !success {
                        if let Ok(new_url) = client.get_upload_url(&bucket_id).await {
                            url = new_url;
                            success = client
                                .upload_file_with_url(&url, path, remote)
                                .await
                                .is_ok();
                        }
                    }

                    if success {
                        done_files.fetch_add(1, Ordering::Relaxed);
                    } else {
                        failed.fetch_add(1, Ordering::Relaxed);
                        failed_names.lock().await.push(remote.clone());
                    }
                    done_bytes.fetch_add(file_size, Ordering::Relaxed);

                    let df = done_files.load(Ordering::Relaxed) as usize;
                    let db = done_bytes.load(Ordering::Relaxed);
                    let short = remote.rsplit('/').next().unwrap_or(remote);
                    on_progress(df, total, db, total_bytes, short);
                }
            });
            handles.push(handle);
        }

        for h in handles {
            let _ = h.await;
        }

        let succeeded = done_files.load(Ordering::Relaxed) as usize;
        let fail_count = failed.load(Ordering::Relaxed) as usize;
        let failed_list = failed_names.lock().await.clone();
        Ok((succeeded, fail_count, failed_list))
    }
}

// ── Helpers ──

pub fn collect_files(
    base: &Path,
    dir: &Path,
    out: &mut Vec<(std::path::PathBuf, String)>,
) -> Result<()> {
    for entry in std::fs::read_dir(dir).context("Cannot read directory")? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(base, &path, out)?;
        } else {
            let relative = path
                .strip_prefix(base)?
                .to_string_lossy()
                .replace('\\', "/");
            out.push((path, relative));
        }
    }
    Ok(())
}

pub fn percent_encode_filename_pub(name: &str) -> String {
    percent_encode_filename(name)
}

fn percent_encode_filename(name: &str) -> String {
    let mut encoded = String::new();
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    encoded
}
