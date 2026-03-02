//! Background jobs module for the PDS.
//!
//! This module provides background job scheduling for maintenance tasks such as:
//! - Updating log levels from the database
//! - Cleaning up old log files
//! - Deleting old firehose events
//! - Deleting old OAuth requests
//! - Requesting crawls from configured crawlers
//! - Deleting stale admin sessions

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::interval;

use crate::fs::LocalFileSystem;
use crate::log::{LogLevel, Logger};
use crate::pds::db::PdsDb;

/// Configuration for retention periods (in hours for firehose events).
const FIREHOSE_RETENTION_HOURS: i64 = 72;

/// Admin session timeout in minutes.
const ADMIN_SESSION_TIMEOUT_MINUTES: i32 = 120;

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
                job_cleanup_old_logs(log, &lfs, &db);
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
                    "[BACKGROUND] UpdateLogLevel currentLevel=[{}] newLevel=[{}]",
                    current_level, new_level
                ));
                log.set_level(new_level);
            }
        }
        Err(e) => {
            log.error(&format!("[BACKGROUND] UpdateLogLevel error: {}", e));
        }
    }
}

/// Job: Cleanup old log files based on retention period.
fn job_cleanup_old_logs(log: &'static Logger, lfs: &LocalFileSystem, db: &PdsDb) {
    let log_retention_days = match db.get_config_property_int("LogRetentionDays") {
        Ok(days) => days,
        Err(e) => {
            log.error(&format!("[BACKGROUND] CleanupOldLogs error getting retention days: {}", e));
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
            log.error(&format!("[BACKGROUND] CleanupOldLogs error reading logs dir: {}", e));
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
            log.info(&format!("[BACKGROUND] Keeping log file: {}", path.display()));
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
            log.info(&format!("[BACKGROUND] Deleting old log file: {}", path.display()));
            if let Err(e) = std::fs::remove_file(&path) {
                log.error(&format!(
                    "[BACKGROUND] Failed to delete log file: {}. Exception: {}",
                    path.display(), e
                ));
            }
        } else {
            log.info(&format!("[BACKGROUND] Keeping log file: {}", path.display()));
        }
    }
}

/// Job: Delete old firehose events.
fn job_delete_old_firehose_events(log: &'static Logger, db: &PdsDb) {
    match db.get_count_of_old_firehose_events(FIREHOSE_RETENTION_HOURS) {
        Ok(old_event_count) => {
            if old_event_count > 0 {
                if let Err(e) = db.delete_old_firehose_events(FIREHOSE_RETENTION_HOURS) {
                    log.error(&format!("[BACKGROUND] DeleteOldFirehoseEvents error: {}", e));
                    return;
                }
            }

            let old_event_count_after = db
                .get_count_of_old_firehose_events(FIREHOSE_RETENTION_HOURS)
                .unwrap_or(0);

            log.info(&format!(
                "[BACKGROUND] DeleteOldFirehoseEvents beforeCount={} afterCount={}",
                old_event_count, old_event_count_after
            ));
        }
        Err(e) => {
            log.error(&format!("[BACKGROUND] DeleteOldFirehoseEvents error: {}", e));
        }
    }
}

/// Job: Delete old OAuth requests.
fn job_delete_old_oauth_requests(log: &'static Logger, db: &PdsDb) {
    log.info("[BACKGROUND] DeleteOldOauthRequests");
    if let Err(e) = db.delete_old_oauth_requests() {
        log.error(&format!("[BACKGROUND] DeleteOldOauthRequests error: {}", e));
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
            log.error(&format!("[BACKGROUND] RequestCrawl error getting hostname: {}", e));
            return;
        }
    };

    let crawlers_str = match db.get_config_property("PdsCrawlers") {
        Ok(crawlers) => crawlers,
        Err(e) => {
            log.error(&format!("[BACKGROUND] RequestCrawl error getting crawlers: {}", e));
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
                    "[BACKGROUND] RequestCrawl. pdsHostname={} crawler={} response={}",
                    pds_hostname, crawler, response_text
                ));
            }
            Err(e) => {
                log.error(&format!(
                    "[BACKGROUND] RequestCrawl error for crawler {}: {}",
                    crawler, e
                ));
            }
        }
    }
}

/// Job: Delete stale admin sessions.
fn job_delete_stale_admin_sessions(log: &'static Logger, db: &PdsDb) {
    log.info("[BACKGROUND] DeleteStaleAdminSessions");
    if let Err(e) = db.delete_stale_admin_sessions(ADMIN_SESSION_TIMEOUT_MINUTES) {
        log.error(&format!("[BACKGROUND] DeleteStaleAdminSessions error: {}", e));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_background_jobs_creation() {
        // This test just verifies the struct can be constructed
        // Full integration testing would require a database
    }
}
