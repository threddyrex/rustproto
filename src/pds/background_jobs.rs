//! background jobs for the PDS
//!
//! This module provides background job scheduling for maintenance tasks such as:
//! - Updating log levels from the database
//! - Cleaning up old log files
//! - Deleting old firehose events
//! - Deleting old OAuth requests
//! - Requesting crawls from configured crawlers
//! - Deleting stale admin sessions
//! - Backing up blobs and the database to Azure storage

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::fs::LocalFileSystem;
use crate::log::{LogLevel, Logger};
use crate::pds::blob_db::{create_blob_db, BlobDb};
use crate::pds::db::{PdsDb, PdsDbError};

/// Configuration for retention periods (in hours for firehose events).
const FIREHOSE_RETENTION_HOURS: i64 = 72;

/// Admin session timeout in minutes.
const ADMIN_SESSION_TIMEOUT_MINUTES: i32 = 120;

const DB_BACKUP_SAS_URL: &str = "AzureStorageSASURL_Db";
const LAST_DB_BACKUP_UTC_DATE: &str = "LastDbBackupUtcDate";

/// Background jobs manager for the PDS.
///
/// Starts periodic background tasks for database cleanup and maintenance.
pub struct BackgroundJobs {
    /// Logger instance.
    log: &'static Logger,
    /// Local file system access.
    lfs: LocalFileSystem,
    /// PDS database access.
    db: Arc<PdsDb>,
    /// Handles for spawned background tasks.
    handles: Vec<JoinHandle<()>>,
}

impl BackgroundJobs {
    /// Create a new BackgroundJobs instance.
    ///
    /// # Arguments
    ///
    /// * `lfs` - LocalFileSystem instance for file operations
    /// * `log` - Logger instance reference (static lifetime)
    /// * `db` - PDS database instance (wrapped in Arc for sharing)
    pub fn new(lfs: LocalFileSystem, log: &'static Logger, db: Arc<PdsDb>) -> Self {
        Self {
            log,
            lfs,
            db,
            handles: Vec::new(),
        }
    }

    /// Start all background jobs.
    ///
    /// This spawns async tasks that run periodically:
    /// - Update log level: every 15 seconds
    /// - Cleanup old logs: every hour
    /// - Delete old firehose events: every hour
    /// - Delete old OAuth requests: every hour
    /// - Request crawl (if enabled): every 5 minutes (starts after 30 seconds)
    /// - Delete stale admin sessions: every hour (starts after 30 seconds)
    /// - Back up blobs to Azure storage: every day (starts after 60 seconds)
    /// - Back up the database to Azure storage: once per UTC day (checked hourly)
    pub fn start(&mut self) {
        self.log.info("[BACKGROUND] Starting background jobs");

        // Job: Update log level every 15 seconds
        let log = self.log;
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(15));
            loop {
                timer.tick().await;
                job_update_log_level(log, &db);
            }
        });
        self.handles.push(handle);

        // Job: Cleanup old logs every hour
        let log = self.log;
        let lfs = self.lfs.clone();
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(3600));
            loop {
                timer.tick().await;
                job_cleanup_old_logs(log, &lfs, &db).await;
            }
        });
        self.handles.push(handle);

        // Job: Delete old firehose events every hour
        let log = self.log;
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(3600));
            loop {
                timer.tick().await;
                job_delete_old_firehose_events(log, &db);
            }
        });
        self.handles.push(handle);

        // Job: Delete old OAuth requests every hour
        let log = self.log;
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(3600));
            loop {
                timer.tick().await;
                job_delete_old_oauth_requests(log, &db);
            }
        });
        self.handles.push(handle);

        // Job: Request crawl every 5 minutes (starts after 30 seconds)
        let log = self.log;
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            // Initial delay of 30 seconds
            tokio::time::sleep(Duration::from_secs(30)).await;
            let mut timer = interval(Duration::from_secs(300));
            loop {
                timer.tick().await;
                job_request_crawl_if_enabled(log, &db).await;
            }
        });
        self.handles.push(handle);

        // Job: Delete stale admin sessions every hour (starts after 30 seconds)
        let log = self.log;
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            // Initial delay of 30 seconds
            tokio::time::sleep(Duration::from_secs(30)).await;
            let mut timer = interval(Duration::from_secs(3600));
            loop {
                timer.tick().await;
                job_delete_stale_admin_sessions(log, &db);
            }
        });
        self.handles.push(handle);

        
        // Job delete old statistics
        let log = self.log;
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            let mut timer = interval(Duration::from_secs(3600));
            loop {
                timer.tick().await;
                job_delete_old_statistics(log, &db);
            }
        });
        self.handles.push(handle);

        // Job: Back up blobs to Azure storage once per day (starts after 60 seconds)
        let log = self.log;
        let lfs = self.lfs.clone();
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let mut timer = interval(Duration::from_secs(86400));
            loop {
                timer.tick().await;
                job_backup_blobs_to_azure(log, &lfs, &db).await;
            }
        });
        self.handles.push(handle);

        // Check hourly so unsuccessful backups can be retried on the same UTC day.
        let log = self.log;
        let db = Arc::clone(&self.db);
        let handle = tokio::spawn(async move {
            let client = match reqwest::Client::builder()
                .https_only(true)
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(30 * 60))
                .build()
            {
                Ok(client) => client,
                Err(e) => {
                    log.error(&format!(
                        "[BACKGROUND][BACKUPDB] Failed to create HTTP client: {}",
                        e
                    ));
                    return;
                }
            };
            tokio::time::sleep(Duration::from_secs(60)).await;
            let mut timer = interval(Duration::from_secs(3600));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                timer.tick().await;
                if let Err(e) =
                    job_backup_db_to_azure(log, &client, &db, chrono::Utc::now().date_naive()).await
                {
                    log.error(&format!("[BACKGROUND][BACKUPDB] {}", e));
                }
            }
        });
        self.handles.push(handle);

        self.log.info("[BACKGROUND] Background jobs started");
    }

    /// Stop all background jobs.
    pub fn stop(&mut self) {
        self.log.info("[BACKGROUND] Stopping background jobs");
        for handle in self.handles.drain(..) {
            handle.abort();
        }
    }
}

impl Clone for BackgroundJobs {
    fn clone(&self) -> Self {
        Self {
            log: self.log,
            lfs: self.lfs.clone(),
            db: Arc::clone(&self.db),
            handles: Vec::new(), // Don't clone handles
        }
    }
}

/// Job: Update the logger's log level from the database.
fn job_update_log_level(log: &'static Logger, db: &PdsDb) {
    match db.get_log_level() {
        Ok(new_level_str) => {
            let current_level = log.level();
            let new_level: LogLevel = new_level_str.parse().unwrap_or(LogLevel::Info);

            if current_level != new_level {
                log.info(&format!(
                    "[BACKGROUND][UPDATELOGLEVEL] UpdateLogLevel currentLevel=[{}] newLevel=[{}]",
                    current_level, new_level
                ));
                log.set_level(new_level);
            }
        }
        Err(e) => {
            log.error(&format!("[BACKGROUND][UPDATELOGLEVEL] UpdateLogLevel error: {}", e));
        }
    }
}

/// Job: Cleanup old log files based on retention period.
async fn job_cleanup_old_logs(log: &'static Logger, lfs: &LocalFileSystem, db: &PdsDb) {
    let log_retention_days = match db.get_config_property_int("LogRetentionDays") {
        Ok(days) => days,
        Err(e) => {
            log.error(&format!("[BACKGROUND][CLEANUPOLDLOGS] CleanupOldLogs error getting retention days: {}", e));
            return;
        }
    };

    let logs_dir = lfs.get_path_logs_dir();
    if !logs_dir.exists() {
        return;
    }

    let entries = match std::fs::read_dir(&logs_dir) {
        Ok(entries) => entries,
        Err(e) => {
            log.error(&format!("[BACKGROUND][CLEANUPOLDLOGS] CleanupOldLogs error reading logs dir: {}", e));
            return;
        }
    };

    let cutoff = chrono::Local::now() - chrono::Duration::days(log_retention_days as i64);

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        
        // Only delete .bak files (rotated logs)
        if !file_name.ends_with(".bak") {
            log.info(&format!("[BACKGROUND][CLEANUPOLDLOGS] Keeping log file: {}", path.display()));
            continue;
        }

        // Check file modification time
        let metadata = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        let modified = match metadata.modified() {
            Ok(m) => m,
            Err(_) => continue,
        };

        let modified_datetime: chrono::DateTime<chrono::Local> = modified.into();
        if modified_datetime < cutoff {
            // Back up the log file to Azure storage before removing it.
            backup_log_to_azure(log, db, &path, file_name).await;

            log.info(&format!("[BACKGROUND][CLEANUPOLDLOGS] Deleting old log file: {}", path.display()));
            if let Err(e) = std::fs::remove_file(&path) {
                log.error(&format!(
                    "[BACKGROUND][CLEANUPOLDLOGS] Failed to delete log file: {}. Exception: {}",
                    path.display(), e
                ));
            }
        } else {
            log.info(&format!("[BACKGROUND][CLEANUPOLDLOGS] Keeping log file: {}", path.display()));
        }
    }
}

/// Back up a log file to the Azure storage container referenced by the
/// `AzureStorageSASURL` config property before it is deleted.
///
/// If the config property is not set, a warning is logged and the caller
/// proceeds to remove the file anyway.
async fn backup_log_to_azure(
    log: &'static Logger,
    db: &PdsDb,
    path: &std::path::Path,
    file_name: &str,
) {
    let sas_url = match db.get_config_property("AzureStorageSASURL") {
        Ok(url) if !url.is_empty() => url,
        _ => {
            log.warning(&format!(
                "[BACKGROUND][CLEANUPOLDLOGS] CleanupOldLogs: AzureStorageSASURL not set, skipping backup of log file: {}",
                file_name
            ));
            return;
        }
    };

    let pds_hostname = db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "unknown".to_string());

    let blob_name = format!("{}-{}", pds_hostname, file_name);

    let file_bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) => {
            log.error(&format!(
                "[BACKGROUND][CLEANUPOLDLOGS] CleanupOldLogs: failed to read log file for backup: {}. Exception: {}",
                path.display(), e
            ));
            return;
        }
    };

    // Insert the blob name into the container SAS URL path, before the query string.
    let upload_url = match sas_url.split_once('?') {
        Some((base, query)) => format!("{}/{}?{}", base.trim_end_matches('/'), blob_name, query),
        None => format!("{}/{}", sas_url.trim_end_matches('/'), blob_name),
    };

    let client = reqwest::Client::new();
    let result = client
        .put(&upload_url)
        .header("x-ms-blob-type", "BlockBlob")
        .header("x-ms-version", "2021-08-06")
        .body(file_bytes)
        .send()
        .await;

    match result {
        Ok(resp) if resp.status().is_success() => {
            log.info(&format!(
                "[BACKGROUND][CLEANUPOLDLOGS] CleanupOldLogs: backed up log file to Azure storage as blob: {}",
                blob_name
            ));
        }
        Ok(resp) => {
            log.error(&format!(
                "[BACKGROUND][CLEANUPOLDLOGS] CleanupOldLogs: Azure backup failed for blob {} with status {}",
                blob_name, resp.status()
            ));
        }
        Err(e) => {
            log.error(&format!(
                "[BACKGROUND][CLEANUPOLDLOGS] CleanupOldLogs: Azure backup request error for blob {}: {}",
                blob_name, e
            ));
        }
    }
}

/// Job: Back up PDS blobs to the Azure storage container referenced by the
/// `AzureStorageSASURL_Blobs` config property.
///
/// Lists the blobs already in the container, compares against the blobs
/// stored in the PDS, and uploads any PDS blobs that are missing. Blobs are
/// named by their CID in the container.
async fn job_backup_blobs_to_azure(log: &'static Logger, lfs: &LocalFileSystem, db: &PdsDb) {
    let sas_url = match db.get_config_property("AzureStorageSASURL_Blobs") {
        Ok(url) if !url.trim().is_empty() => url.trim().to_string(),
        _ => {
            log.info("[BACKGROUND][BACKUPBLOBS] BackupBlobs: AzureStorageSASURL_Blobs not set, skipping");
            return;
        }
    };

    let client = reqwest::Client::new();

    let container_blobs = match list_azure_container_blobs(&client, &sas_url).await {
        Ok(names) => names,
        Err(e) => {
            log.error(&format!("[BACKGROUND][BACKUPBLOBS] BackupBlobs: failed to list container blobs: {}", e));
            return;
        }
    };

    let pds_blobs = match db.get_all_blobs() {
        Ok(blobs) => blobs,
        Err(e) => {
            log.error(&format!("[BACKGROUND][BACKUPBLOBS] BackupBlobs: failed to get PDS blobs: {}", e));
            return;
        }
    };

    let missing: Vec<_> = pds_blobs
        .iter()
        .filter(|b| !container_blobs.contains(&b.cid))
        .collect();

    log.info(&format!(
        "[BACKGROUND][BACKUPBLOBS] BackupBlobs containerBlobCount={} pdsBlobCount={} missingBlobCount={} (writing missing blobs to container)",
        container_blobs.len(), pds_blobs.len(), missing.len()
    ));

    let blob_db = create_blob_db(lfs, log);
    let mut uploaded = 0;
    let mut failed = 0;

    for blob in missing {
        let bytes = match blob_db.get_blob_bytes(&blob.cid) {
            Ok(bytes) => bytes,
            Err(e) => {
                log.error(&format!(
                    "[BACKGROUND][BACKUPBLOBS] BackupBlobs: failed to read blob bytes for {}: {}",
                    blob.cid, e
                ));
                failed += 1;
                continue;
            }
        };

        let upload_url = azure_blob_url(&sas_url, &blob.cid);
        let result = client
            .put(&upload_url)
            .header("x-ms-blob-type", "BlockBlob")
            .header("x-ms-version", "2021-08-06")
            .header("x-ms-blob-content-type", &blob.content_type)
            .body(bytes)
            .send()
            .await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                log.info(&format!("[BACKGROUND][BACKUPBLOBS] BackupBlobs: uploaded blob {}", blob.cid));
                uploaded += 1;
            }
            Ok(resp) => {
                log.error(&format!(
                    "[BACKGROUND][BACKUPBLOBS] BackupBlobs: upload failed for blob {} with status {}",
                    blob.cid, resp.status()
                ));
                failed += 1;
            }
            Err(e) => {
                log.error(&format!(
                    "[BACKGROUND][BACKUPBLOBS] BackupBlobs: upload request error for blob {}: {}",
                    blob.cid, e
                ));
                failed += 1;
            }
        }
    }

    log.info(&format!(
        "[BACKGROUND][BACKUPBLOBS] BackupBlobs complete uploaded={} failed={}",
        uploaded, failed
    ));
}

async fn job_backup_db_to_azure(
    log: &Logger,
    client: &reqwest::Client,
    db: &Arc<PdsDb>,
    date: chrono::NaiveDate,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sas_url = match db.get_config_property(DB_BACKUP_SAS_URL) {
        Ok(url) if !url.trim().is_empty() => url,
        Ok(_) | Err(PdsDbError::ConfigPropertyNotFound(_)) => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    let utc_day = date.format("%Y-%m-%d").to_string();
    match db.get_config_property(LAST_DB_BACKUP_UTC_DATE) {
        Ok(last_date) if last_date == utc_day => return Ok(()),
        Ok(_) | Err(PdsDbError::ConfigPropertyNotFound(_)) => {}
        Err(e) => return Err(e.into()),
    }

    let hostname = db.get_config_property("PdsHostname")?;
    let blob_name = format!("{}-{}-pds.db", hostname, date.format("%Y%m%d"));
    let upload_url = azure_blob_url(sas_url.trim(), &urlencoding::encode(&blob_name));

    let snapshot_db = Arc::clone(db);
    let snapshot = tokio::task::spawn_blocking(move || snapshot_pds_db(&snapshot_db)).await??;
    let file = tokio::fs::File::open(snapshot.path()).await?;
    let content_length = file.metadata().await?.len();
    let response = client
        .put(upload_url)
        .header("x-ms-blob-type", "BlockBlob")
        .header("x-ms-version", "2021-08-06")
        .header(reqwest::header::IF_NONE_MATCH, "*")
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .header(reqwest::header::CONTENT_LENGTH, content_length)
        .body(file)
        .send()
        .await
        .map_err(reqwest::Error::without_url)?;

    let status = response.status();
    let azure_error = response
        .headers()
        .get("x-ms-error-code")
        .and_then(|v| v.to_str().ok());
    // The blob may already exist if Azure committed a previous PUT before we saved the date.
    let already_exists = matches!(
        (status, azure_error),
        (reqwest::StatusCode::PRECONDITION_FAILED, Some("ConditionNotMet"))
            | (reqwest::StatusCode::CONFLICT, Some("BlobAlreadyExists"))
    );
    if status != reqwest::StatusCode::CREATED && !already_exists {
        return Err(format!("Upload failed for {} with status {}", blob_name, status).into());
    }

    db.set_config_property(LAST_DB_BACKUP_UTC_DATE, &utc_day)?;
    if already_exists {
        log.info(&format!("[BACKGROUND][BACKUPDB] Backup already exists in Azure: {}", blob_name));
    } else {
        log.info(&format!("[BACKGROUND][BACKUPDB] Uploaded database backup: {}", blob_name));
    }
    Ok(())
}

fn snapshot_pds_db(db: &PdsDb) -> Result<tempfile::NamedTempFile, PdsDbError> {
    let snapshot = tempfile::NamedTempFile::new()?;
    let path = snapshot.path().to_str().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "Snapshot path is not UTF-8")
    })?;

    // BEGIN IMMEDIATE waits for any active writer, then blocks other writers while
    // a separate SQLite connection copies the committed state, including WAL data.
    let mut lock_conn = db.get_connection()?;
    let write_lock = lock_conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    db.get_connection_read_only()?.execute("VACUUM INTO ?1", [path])?;
    write_lock.rollback()?;
    Ok(snapshot)
}

/// Build the URL for a blob within a container SAS URL by inserting the blob
/// name into the path, before the query string.
fn azure_blob_url(container_sas_url: &str, blob_name: &str) -> String {
    match container_sas_url.split_once('?') {
        Some((base, query)) => format!("{}/{}?{}", base.trim_end_matches('/'), blob_name, query),
        None => format!("{}/{}", container_sas_url.trim_end_matches('/'), blob_name),
    }
}

/// Build the List Blobs URL for a container SAS URL, with an optional marker.
fn azure_list_blobs_url(container_sas_url: &str, marker: Option<&str>) -> String {
    let (base, query) = match container_sas_url.split_once('?') {
        Some((base, query)) => (base.trim_end_matches('/'), query),
        None => (container_sas_url.trim_end_matches('/'), ""),
    };
    let mut url = format!("{}?restype=container&comp=list", base);
    if !query.is_empty() {
        url.push('&');
        url.push_str(query);
    }
    if let Some(m) = marker {
        url.push_str("&marker=");
        url.push_str(&urlencoding::encode(m));
    }
    url
}

/// List all blob names in an Azure storage container (following pagination markers).
async fn list_azure_container_blobs(
    client: &reqwest::Client,
    container_sas_url: &str,
) -> Result<HashSet<String>, String> {
    let mut names = HashSet::new();
    let mut marker: Option<String> = None;

    loop {
        let url = azure_list_blobs_url(container_sas_url, marker.as_deref());
        let resp = client
            .get(&url)
            .header("x-ms-version", "2021-08-06")
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = resp.status();
        let body = resp.text().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("status {}", status));
        }

        names.extend(extract_xml_values(&body, "Name"));

        match extract_xml_values(&body, "NextMarker").into_iter().next() {
            Some(next) if !next.is_empty() => marker = Some(next),
            _ => break,
        }
    }

    Ok(names)
}

/// Extract the text of every `<tag>...</tag>` element in an XML document.
fn extract_xml_values(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let mut values = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        values.push(xml_unescape(&after[..end]));
        rest = &after[end + close.len()..];
    }
    values
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Job: Delete old firehose events.
fn job_delete_old_firehose_events(log: &'static Logger, db: &PdsDb) {
    match db.get_count_of_old_firehose_events(FIREHOSE_RETENTION_HOURS) {
        Ok(old_event_count) => {
            if old_event_count > 0 {
                if let Err(e) = db.delete_old_firehose_events(FIREHOSE_RETENTION_HOURS) {
                    log.error(&format!("[BACKGROUND][DELETEOLDFIREHOSEEVENTS] DeleteOldFirehoseEvents error: {}", e));
                    return;
                }
            }

            let old_event_count_after = db
                .get_count_of_old_firehose_events(FIREHOSE_RETENTION_HOURS)
                .unwrap_or(0);

            log.info(&format!(
                "[BACKGROUND][DELETEOLDFIREHOSEEVENTS] DeleteOldFirehoseEvents beforeCount={} afterCount={}",
                old_event_count, old_event_count_after
            ));
        }
        Err(e) => {
            log.error(&format!("[BACKGROUND][DELETEOLDFIREHOSEEVENTS] DeleteOldFirehoseEvents error: {}", e));
        }
    }
}

/// Job: Delete old OAuth requests.
fn job_delete_old_oauth_requests(log: &'static Logger, db: &PdsDb) {
    log.info("[BACKGROUND][DELETEOLDOAUTHREQUESTS] DeleteOldOauthRequests");
    if let Err(e) = db.delete_old_oauth_requests() {
        log.error(&format!("[BACKGROUND][DELETEOLDOAUTHREQUESTS] DeleteOldOauthRequests error: {}", e));
    }
}

/// Job: Request crawl from configured crawlers if enabled.
async fn job_request_crawl_if_enabled(log: &'static Logger, db: &PdsDb) {
    let request_crawl_enabled = match db.get_config_property_bool("FeatureEnabled_RequestCrawl") {
        Ok(enabled) => enabled,
        Err(_) => false, // Feature disabled by default if property not found
    };

    if !request_crawl_enabled {
        return;
    }

    let pds_hostname = match db.get_config_property("PdsHostname") {
        Ok(hostname) => hostname,
        Err(e) => {
            log.error(&format!("[BACKGROUND][REQUESTCRAWL] RequestCrawl error getting hostname: {}", e));
            return;
        }
    };

    let crawlers_str = match db.get_config_property("PdsCrawlers") {
        Ok(crawlers) => crawlers,
        Err(e) => {
            log.error(&format!("[BACKGROUND][REQUESTCRAWL] RequestCrawl error getting crawlers: {}", e));
            return;
        }
    };

    let crawlers: Vec<&str> = crawlers_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let client = reqwest::Client::new();

    for crawler in crawlers {
        let url = format!("https://{}/xrpc/com.atproto.sync.requestCrawl", crawler);
        let body = serde_json::json!({
            "hostname": pds_hostname
        });

        match client.post(&url).json(&body).send().await {
            Ok(response) => {
                let response_text = response.text().await.unwrap_or_default();
                log.info(&format!(
                    "[BACKGROUND][REQUESTCRAWL] RequestCrawl. pdsHostname={} crawler={} response={}",
                    pds_hostname, crawler, response_text
                ));
            }
            Err(e) => {
                log.error(&format!(
                    "[BACKGROUND][REQUESTCRAWL] RequestCrawl error for crawler {}: {}",
                    crawler, e
                ));
            }
        }
    }
}

/// Job: Delete stale admin sessions.
fn job_delete_stale_admin_sessions(log: &'static Logger, db: &PdsDb) {
    log.info("[BACKGROUND][DELETESTALEADMINSESSIONS] DeleteStaleAdminSessions");
    if let Err(e) = db.delete_stale_admin_sessions(ADMIN_SESSION_TIMEOUT_MINUTES) {
        log.error(&format!("[BACKGROUND][DELETESTALEADMINSESSIONS] DeleteStaleAdminSessions error: {}", e));
    }
}

fn job_delete_old_statistics(log: &'static Logger, db: &PdsDb) {
    log.info("[BACKGROUND][DELETEOLDSTATISTICS] DeleteOldStatistics");
    if let Err(e) = db.delete_old_statistics(24) {
        log.error(&format!("[BACKGROUND][DELETEOLDSTATISTICS] DeleteOldStatistics error: {}", e));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pds::installer::Installer;

    fn test_backup_db() -> (tempfile::TempDir, LocalFileSystem, Arc<PdsDb>) {
        let dir = tempfile::tempdir().unwrap();
        let lfs = LocalFileSystem::initialize_with_create(dir.path()).unwrap();
        Installer::install_db(&lfs, &Logger::default_logger(), false).unwrap();
        let db = Arc::new(PdsDb::connect(&lfs).unwrap());
        (dir, lfs, db)
    }

    #[test]
    fn db_snapshot_includes_committed_wal_data_and_is_standalone() {
        let (_dir, _lfs, db) = test_backup_db();
        let conn = db.get_connection().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        conn.execute("INSERT INTO ConfigProperty VALUES ('BackupTest', 'before')", []).unwrap();

        let snapshot = snapshot_pds_db(&db).unwrap();
        db.set_config_property("BackupTest", "after").unwrap();
        let copy = crate::pds::db::SqliteDb::get_connection_read_only(snapshot.path()).unwrap();
        let value: String = copy.query_row(
            "SELECT Value FROM ConfigProperty WHERE Key = 'BackupTest'", [], |row| row.get(0),
        ).unwrap();
        assert_eq!(value, "before");
        let integrity: String = copy.query_row("PRAGMA integrity_check", [], |row| row.get(0)).unwrap();
        assert_eq!(integrity, "ok");
    }

    #[tokio::test]
    async fn db_backup_retries_failures_and_skips_days_already_written() {
        use axum::{
            body::Bytes,
            extract::OriginalUri,
            http::{HeaderMap, StatusCode},
            response::IntoResponse,
            routing::put,
            Router,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (_dir, lfs, db) = test_backup_db();
        let count = Arc::new(AtomicUsize::new(0));
        let requests = Arc::clone(&count);
        let mock = Router::new().route("/backup/{blob}", put(
            move |OriginalUri(uri): OriginalUri, headers: HeaderMap, body: Bytes| {
                let requests = Arc::clone(&requests);
                async move {
                    let request = requests.fetch_add(1, Ordering::SeqCst);
                    let day = if request < 2 { "20261117" } else { "20261118" };
                    assert_eq!(uri.path(), format!("/backup/pds.example.test-{day}-pds.db"));
                    assert_eq!(uri.query(), Some("sp=c&sig=mock"));
                    assert_eq!(headers["if-none-match"], "*");
                    assert_eq!(headers["x-ms-blob-type"], "BlockBlob");
                    assert_eq!(headers["content-length"].to_str().unwrap().parse::<usize>().unwrap(), body.len());
                    assert!(body.starts_with(b"SQLite format 3\0"));
                    match request {
                        0 => StatusCode::FORBIDDEN.into_response(),
                        1 => StatusCode::CREATED.into_response(),
                        _ => (StatusCode::PRECONDITION_FAILED, [("x-ms-error-code", "ConditionNotMet")]).into_response(),
                    }
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
        db.set_config_property("PdsHostname", "pds.example.test").unwrap();
        db.set_config_property(DB_BACKUP_SAS_URL, &format!("http://{addr}/backup?sp=c&sig=mock")).unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let log = Logger::default_logger();
        let day = chrono::NaiveDate::from_ymd_opt(2026, 11, 17).unwrap();
        assert!(job_backup_db_to_azure(&log, &client, &db, day).await.is_err());
        assert!(!db.config_property_exists(LAST_DB_BACKUP_UTC_DATE).unwrap());
        job_backup_db_to_azure(&log, &client, &db, day).await.unwrap();
        drop(db);

        let db = Arc::new(PdsDb::connect(&lfs).unwrap());
        assert_eq!(db.get_config_property(LAST_DB_BACKUP_UTC_DATE).unwrap(), "2026-11-17");
        job_backup_db_to_azure(&log, &client, &db, day).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);

        let next_day = day.succ_opt().unwrap();
        job_backup_db_to_azure(&log, &client, &db, next_day).await.unwrap();
        assert_eq!(db.get_config_property(LAST_DB_BACKUP_UTC_DATE).unwrap(), "2026-11-18");
        job_backup_db_to_azure(&log, &client, &db, next_day).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[test]
    fn test_azure_blob_url() {
        assert_eq!(
            azure_blob_url("https://a.blob.core.windows.net/c?sv=1&sig=x", "bafy"),
            "https://a.blob.core.windows.net/c/bafy?sv=1&sig=x"
        );
        assert_eq!(azure_blob_url("https://a/c/", "b"), "https://a/c/b");
    }

    #[test]
    fn test_azure_list_blobs_url() {
        assert_eq!(
            azure_list_blobs_url("https://a/c?sv=1&sig=x", None),
            "https://a/c?restype=container&comp=list&sv=1&sig=x"
        );
        assert_eq!(
            azure_list_blobs_url("https://a/c?sv=1", Some("m/1")),
            "https://a/c?restype=container&comp=list&sv=1&marker=m%2F1"
        );
    }

    #[test]
    fn test_extract_xml_values() {
        let xml = "<EnumerationResults><Blobs><Blob><Name>a</Name></Blob><Blob><Name>b&amp;c</Name></Blob></Blobs><NextMarker /></EnumerationResults>";
        assert_eq!(extract_xml_values(xml, "Name"), vec!["a", "b&c"]);
        assert!(extract_xml_values(xml, "NextMarker").is_empty());
        let xml2 = "<NextMarker>abc</NextMarker>";
        assert_eq!(extract_xml_values(xml2, "NextMarker"), vec!["abc"]);
    }
    #[test]
    fn test_background_jobs_creation() {
        // This test just verifies the struct can be constructed
        // Full integration testing would require a database
    }
}
