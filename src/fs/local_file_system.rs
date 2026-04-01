//! Local file system for storing repos and data.
//!
//! This module provides functionality to manage the local data directory
//! structure, including subdirectories for actors, repos, sessions, etc.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

use crate::log::logger;
use crate::ws::{ActorInfo, BlueskyClient};

/// Errors that can occur during file system operations.
#[derive(Error, Debug)]
pub enum LocalFileSystemError {
    #[error("Data directory is null or does not exist: {0}")]
    DataDirNotFound(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Actor info not found or expired: {0}")]
    ActorInfoNotFound(String),

    #[error("JSON parse error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// Subdirectories to create in the data directory.
const SUBDIRS: &[&str] = &[
    "actors",
    "backups",
    "blobs",
    "repos",
    "preferences",
    "sessions",
    "pds",
    "scratch",
    "logs",
    "records",
    "static",
];

/// Subdirectories to create under pds directory.
const PDS_SUBDIRS: &[&str] = &["blobs"];

/// Provides access to the local file system for storing repos and backups.
#[derive(Clone)]
pub struct LocalFileSystem {
    data_dir: PathBuf,
}

impl LocalFileSystem {
    /// Ensures that the root dir exists, and creates subdirs if needed.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Path to the data directory
    ///
    /// # Returns
    ///
    /// A LocalFileSystem instance if successful
    ///
    pub fn initialize<P: AsRef<Path>>(data_dir: P) -> Result<Self, LocalFileSystemError> {
        let data_dir = data_dir.as_ref();

        if !data_dir.exists() {
            return Err(LocalFileSystemError::DataDirNotFound(
                data_dir.to_string_lossy().to_string(),
            ));
        }

        // Create main subdirectories
        for subdir in SUBDIRS {
            let full_subdir = data_dir.join(subdir);
            if !full_subdir.exists() {
                fs::create_dir_all(&full_subdir)?;
            }
        }

        // Create pds subdirectories
        for subdir in PDS_SUBDIRS {
            let full_subdir = data_dir.join("pds").join(subdir);
            if !full_subdir.exists() {
                fs::create_dir_all(&full_subdir)?;
            }
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Creates the data directory if it doesn't exist, then initializes.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Path to the data directory
    ///
    /// # Returns
    ///
    /// A LocalFileSystem instance if successful
    pub fn initialize_with_create<P: AsRef<Path>>(
        data_dir: P,
    ) -> Result<Self, LocalFileSystemError> {
        let data_dir = data_dir.as_ref();

        if !data_dir.exists() {
            fs::create_dir_all(data_dir)?;
        }

        Self::initialize(data_dir)
    }

    /// Gets the data directory path.
    pub fn get_data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Gets the path to the repo file for the given DID.
    ///
    /// Returns a path like `{data_dir}/repos/{safe_did}.car`
    pub fn get_path_repo_file(&self, did: &str) -> Result<PathBuf, LocalFileSystemError> {
        if did.is_empty() {
            return Err(LocalFileSystemError::InvalidArgument(
                "did is null or empty".to_string(),
            ));
        }

        let safe_did = Self::get_safe_string(did);
        let repo_file = self.data_dir.join("repos").join(format!("{}.car", safe_did));
        Ok(repo_file)
    }

    /// Gets the path to the actor info file for the given actor.
    ///
    /// Returns a path like `{data_dir}/actors/{safe_actor}.json`
    pub fn get_path_actor_file(&self, actor: &str) -> Result<PathBuf, LocalFileSystemError> {
        if actor.is_empty() {
            return Err(LocalFileSystemError::InvalidArgument(
                "actor is null or empty".to_string(),
            ));
        }

        let safe_actor = Self::get_safe_string(actor);
        let actor_file = self
            .data_dir
            .join("actors")
            .join(format!("{}.json", safe_actor));
        Ok(actor_file)
    }

    /// Gets the path to the account backup directory for the given DID.
    ///
    /// Returns a path like `{data_dir}/backups/{safe_did}/`
    pub fn get_path_account_backup_dir(&self, did: &str) -> Result<PathBuf, LocalFileSystemError> {
        if did.is_empty() {
            return Err(LocalFileSystemError::InvalidArgument(
                "did is null or empty".to_string(),
            ));
        }

        let safe_did = Self::get_safe_string(did);
        let backup_dir = self.data_dir.join("backups").join(safe_did);
        Ok(backup_dir)
    }

    /// Gets the path to the blob directory for the given DID.
    ///
    /// Returns a path like `{data_dir}/blobs/{safe_did}/`
    pub fn get_path_blob_dir(&self, did: &str) -> Result<PathBuf, LocalFileSystemError> {
        if did.is_empty() {
            return Err(LocalFileSystemError::InvalidArgument(
                "did is null or empty".to_string(),
            ));
        }

        let safe_did = Self::get_safe_string(did);
        let blob_dir = self.data_dir.join("blobs").join(safe_did);
        Ok(blob_dir)
    }

    /// Gets the path to the preferences file for the given DID.
    ///
    /// Returns a path like `{data_dir}/preferences/{safe_did}.json`
    pub fn get_path_preferences(&self, did: &str) -> Result<PathBuf, LocalFileSystemError> {
        if did.is_empty() {
            return Err(LocalFileSystemError::InvalidArgument(
                "did is null or empty".to_string(),
            ));
        }

        let safe_did = Self::get_safe_string(did);
        let prefs_file = self
            .data_dir
            .join("preferences")
            .join(format!("{}.json", safe_did));
        Ok(prefs_file)
    }

    /// Gets the path to the session file for the given DID.
    ///
    /// Returns a path like `{data_dir}/sessions/{safe_did}.json`
    pub fn get_path_session_file(&self, did: &str) -> Result<PathBuf, LocalFileSystemError> {
        if did.is_empty() {
            return Err(LocalFileSystemError::InvalidArgument(
                "did is null or empty".to_string(),
            ));
        }

        let safe_did = Self::get_safe_string(did);
        let session_file = self
            .data_dir
            .join("sessions")
            .join(format!("{}.json", safe_did));
        Ok(session_file)
    }

    /// Gets the path to the scratch directory.
    ///
    /// Returns a path like `{data_dir}/scratch/`
    pub fn get_path_scratch_dir(&self) -> PathBuf {
        self.data_dir.join("scratch")
    }

    /// Gets the path to the logs directory.
    ///
    /// Returns a path like `{data_dir}/logs/`
    pub fn get_path_logs_dir(&self) -> PathBuf {
        self.data_dir.join("logs")
    }

    /// Gets the path to the PDS database file.
    ///
    /// Returns a path like `{data_dir}/pds/pds.db`
    pub fn get_path_pds_db(&self) -> PathBuf {
        self.data_dir.join("pds").join("pds.db")
    }

    /// Gets the path to the static directory.
    ///
    /// Returns a path like `{data_dir}/static/`
    pub fn get_path_static_dir(&self) -> PathBuf {
        self.data_dir.join("static")
    }

    /// Default cache expiry time in minutes for actor info files.
    pub const DEFAULT_CACHE_EXPIRY_MINUTES: u64 = 15;

    /// Resolves actor info, checking the local cache first and falling back
    /// to BlueskyClient if the file does not exist or is stale.
    ///
    /// On a successful remote resolve, the result is saved to the cache.
    ///
    /// # Arguments
    ///
    /// * `actor` - The actor handle or DID to resolve
    /// * `cache_expiry_minutes` - Maximum age of the cached file in minutes (default: 15)
    ///
    /// # Returns
    ///
    /// The resolved ActorInfo (from cache or remote), or an error.
    ///
    pub async fn resolve_actor_info(
        &self,
        actor: &str,
        cache_expiry_minutes: Option<u64>,
        app_view_host_name: &str,
    ) -> Result<ActorInfo, LocalFileSystemError> {
        let start_time = Instant::now();

        if actor.is_empty() {
            logger().error("[ACTOR] [LFS] actor is null or empty");
            return Err(LocalFileSystemError::InvalidArgument(
                "actor is null or empty".to_string(),
            ));
        }

        let cache_expiry = cache_expiry_minutes.unwrap_or(Self::DEFAULT_CACHE_EXPIRY_MINUTES);
        let actor_file = self.get_path_actor_file(actor)?;

        if actor_file.exists() {
            // Check file age
            let metadata = fs::metadata(&actor_file)?;
            let modified = metadata.modified()?;
            let age = SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::MAX);
            let age_minutes = age.as_secs() / 60;
            let age_minutes_f = age.as_secs_f64() / 60.0;
            let file_old = age_minutes > cache_expiry;

            if file_old {
                logger().info(&format!(
                    "[ACTOR] [LFS] actor={} fileExists=true fileAgeMinutes={:.1} cacheExpiryMinutes={} fileOld=true",
                    actor, age_minutes_f, cache_expiry
                ));
            } else {
                // Load and parse the file
                let json = fs::read_to_string(&actor_file)?;
                let info = ActorInfo::from_json_string(&json)?;

                // Validate that the file has a DID
                if !info.has_did() {
                    logger().warning(&format!(
                        "[ACTOR] [LFS] actor={} fileExists=true fileAgeMinutes={:.1} cacheExpiryMinutes={} fileOld=false missingDid=true",
                        actor, age_minutes_f, cache_expiry
                    ));
                } else {
                    let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
                    logger().info(&format!(
                        "[ACTOR] [LFS] actor={} fileExists=true fileAgeMinutes={:.1} cacheExpiryMinutes={} fileOld=false [{:.2}ms]",
                        actor, age_minutes_f, cache_expiry, elapsed_ms
                    ));
                    return Ok(info);
                }
            }
        } else {
            logger().info(&format!(
                "[ACTOR] [LFS] actor={} fileExists=false",
                actor
            ));
        }

        // Cache miss or stale — resolve via BlueskyClient and save to cache
        let client = BlueskyClient::new(app_view_host_name);
        let actor_info = client.resolve_actor_info(actor, None).await.map_err(|e| {
            let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            logger().error(&format!(
                "[ACTOR] [LFS] actor={} resolve=true resolveFailed=true error={} [{:.2}ms]",
                actor, e, elapsed_ms
            ));
            LocalFileSystemError::ActorInfoNotFound(format!(
                "Failed to resolve actor info: {}",
                e
            ))
        })?;

        if !actor_info.has_did() {
            let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            logger().warning(&format!(
                "[ACTOR] [LFS] actor={} resolve=true missingDid=true [{:.2}ms]",
                actor, elapsed_ms
            ));
            return Err(LocalFileSystemError::ActorInfoNotFound(
                "Resolved actor info is missing DID".to_string(),
            ));
        }

        let _ = self.save_actor_info(actor, &actor_info);

        let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
        logger().info(&format!(
            "[ACTOR] [LFS] actor={} resolve=true [{:.2}ms]",
            actor, elapsed_ms
        ));

        Ok(actor_info)
    }

    /// Saves actor info to the local cache (file system).
    ///
    /// # Arguments
    ///
    /// * `actor` - The actor handle or DID (used for the filename)
    /// * `info` - The ActorInfo to save
    ///
    pub fn save_actor_info(
        &self,
        actor: &str,
        info: &ActorInfo,
    ) -> Result<(), LocalFileSystemError> {
        if actor.is_empty() {
            return Err(LocalFileSystemError::InvalidArgument(
                "actor is null or empty".to_string(),
            ));
        }

        let actor_file = self.get_path_actor_file(actor)?;
        let json = info.to_json_string()?;
        fs::write(&actor_file, json)?;

        Ok(())
    }

    /// Makes a string safe for use as a file or directory name.
    ///
    /// Replaces `:`, `/`, `.`, and `@` with underscores.
    pub fn get_safe_string(input: &str) -> String {
        input
            .replace(':', "_")
            .replace('/', "_")
            .replace('.', "_")
            .replace('@', "_")
    }

    /// Loads a session file for the given DID.
    ///
    /// Returns the parsed JSON value if the session file exists and is not expired.
    ///
    /// # Arguments
    ///
    /// * `did` - The DID to load the session for
    /// * `cache_expiry_minutes` - Maximum age of the session file in minutes (default: 30)
    pub fn load_session(
        &self,
        did: &str,
        cache_expiry_minutes: Option<u64>,
    ) -> Option<serde_json::Value> {
        let cache_expiry = cache_expiry_minutes.unwrap_or(30);

        let session_file = match self.get_path_session_file(did) {
            Ok(p) => p,
            Err(e) => {
                logger().warning(&format!("Failed to get session file path: {}", e));
                return None;
            }
        };

        if !session_file.exists() {
            logger().warning(&format!(
                "Session file does not exist: {}",
                session_file.display()
            ));
            return None;
        }

        // Check file age
        let metadata = match fs::metadata(&session_file) {
            Ok(m) => m,
            Err(e) => {
                logger().warning(&format!("Failed to read session file metadata: {}", e));
                return None;
            }
        };

        if let Ok(modified) = metadata.modified() {
            let age = SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::MAX);
            let age_minutes = age.as_secs() / 60;

            if age_minutes > cache_expiry {
                logger().warning(&format!(
                    "Session file is older than {} minutes, will not use: {}",
                    cache_expiry,
                    session_file.display()
                ));
                return None;
            }
        }

        logger().info(&format!(
            "Reading session file: {}",
            session_file.display()
        ));

        let json_str = match fs::read_to_string(&session_file) {
            Ok(s) => s,
            Err(e) => {
                logger().warning(&format!("Failed to read session file: {}", e));
                return None;
            }
        };

        match serde_json::from_str(&json_str) {
            Ok(v) => Some(v),
            Err(e) => {
                logger().warning(&format!("Failed to parse session file JSON: {}", e));
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_get_safe_string() {
        assert_eq!(
            LocalFileSystem::get_safe_string("did:plc:abc123"),
            "did_plc_abc123"
        );
        assert_eq!(
            LocalFileSystem::get_safe_string("alice.bsky.social"),
            "alice_bsky_social"
        );
        assert_eq!(
            LocalFileSystem::get_safe_string("test@example.com"),
            "test_example_com"
        );
    }

    #[test]
    fn test_get_path_repo_file() {
        let temp_dir = env::temp_dir().join("rustproto_test_lfs");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let lfs = LocalFileSystem::initialize(&temp_dir).unwrap();
        let repo_path = lfs.get_path_repo_file("did:plc:abc123").unwrap();

        assert!(repo_path.to_string_lossy().contains("repos"));
        assert!(repo_path.to_string_lossy().contains("did_plc_abc123.car"));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_empty_did_returns_error() {
        let temp_dir = env::temp_dir().join("rustproto_test_lfs2");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let lfs = LocalFileSystem::initialize(&temp_dir).unwrap();
        let result = lfs.get_path_repo_file("");

        assert!(result.is_err());

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_save_and_resolve_actor_info() {
        let temp_dir = env::temp_dir().join("rustproto_test_actor_info");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let lfs = LocalFileSystem::initialize(&temp_dir).unwrap();

        // Create an ActorInfo to save
        let mut info = ActorInfo::with_actor("alice.bsky.social");
        info.did = Some("did:plc:abc123".to_string());
        info.pds = Some("bsky.social".to_string());

        // Save it
        lfs.save_actor_info("alice.bsky.social", &info).unwrap();

        // Resolve it (should succeed since it's fresh)
        let resolved = lfs.resolve_actor_info("alice.bsky.social", None, "public.api.bsky.app").await.unwrap();
        assert_eq!(resolved.did, Some("did:plc:abc123".to_string()));
        assert_eq!(resolved.pds, Some("bsky.social".to_string()));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_resolve_actor_info_not_found() {
        let temp_dir = env::temp_dir().join("rustproto_test_actor_not_found");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let lfs = LocalFileSystem::initialize(&temp_dir).unwrap();

        // Try to resolve a truly non-existent actor (will attempt network call which will fail)
        let result = lfs.resolve_actor_info("this-handle-does-not-exist-xyz123.invalid", None, "public.api.bsky.app").await;
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
