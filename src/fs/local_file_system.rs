//! Local file system for storing repos and data.
//!
//! This module provides functionality to manage the local data directory
//! structure, including subdirectories for actors, repos, sessions, etc.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

use crate::log::logger;
use crate::ws::ActorInfo;

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
    "repos",
    "preferences",
    "sessions",
    "pds",
    "scratch",
    "logs",
    "records",
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
    /// # Examples
    ///
    /// ```no_run
    /// use rustproto::fs::LocalFileSystem;
    ///
    /// let lfs = LocalFileSystem::initialize("./data").unwrap();
    /// println!("Data dir: {:?}", lfs.get_data_dir());
    /// ```
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

    /// Default cache expiry time in minutes for actor info files.
    pub const DEFAULT_CACHE_EXPIRY_MINUTES: u64 = 15;

    /// Resolves actor info from the local cache (file system).
    ///
    /// Loads the actor info from disk if the file exists and is fresh
    /// (not older than `cache_expiry_minutes`).
    ///
    /// # Arguments
    ///
    /// * `actor` - The actor handle or DID to resolve
    /// * `cache_expiry_minutes` - Maximum age of the cached file in minutes (default: 15)
    ///
    /// # Returns
    ///
    /// The cached ActorInfo if found and fresh, or an error if not found/expired.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rustproto::fs::LocalFileSystem;
    ///
    /// let lfs = LocalFileSystem::initialize("./data").unwrap();
    /// match lfs.resolve_actor_info("alice.bsky.social", None) {
    ///     Ok(info) => println!("Cached DID: {:?}", info.did),
    ///     Err(e) => println!("Not in cache: {}", e),
    /// }
    /// ```
    pub fn resolve_actor_info(
        &self,
        actor: &str,
        cache_expiry_minutes: Option<u64>,
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
        let file_exists = actor_file.exists();

        if !file_exists {
            let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            logger().info(&format!(
                "[ACTOR] [LFS] actor={} fileExists=false [{:.2}ms]",
                actor, elapsed_ms
            ));
            return Err(LocalFileSystemError::ActorInfoNotFound(format!(
                "Actor info file not found: {}",
                actor_file.display()
            )));
        }

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
            let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            logger().info(&format!(
                "[ACTOR] [LFS] actor={} fileExists=true fileAgeMinutes={:.1} cacheExpiryMinutes={} fileOld=true filePath={} [{:.2}ms]",
                actor, age_minutes_f, cache_expiry, actor_file.display(), elapsed_ms
            ));
            return Err(LocalFileSystemError::ActorInfoNotFound(format!(
                "Actor info file expired (age: {} minutes, max: {} minutes)",
                age_minutes, cache_expiry
            )));
        }

        // Load and parse the file
        let json = fs::read_to_string(&actor_file)?;
        let info = ActorInfo::from_json_string(&json)?;

        // Validate that the file has a DID
        if !info.has_did() {
            let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
            logger().warning(&format!(
                "[ACTOR] [LFS] actor={} fileExists=true fileAgeMinutes={:.1} cacheExpiryMinutes={} fileOld=false missingDid=true [{:.2}ms]",
                actor, age_minutes_f, cache_expiry, elapsed_ms
            ));
            return Err(LocalFileSystemError::ActorInfoNotFound(
                "Actor info loaded from file is missing DID".to_string(),
            ));
        }

        let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
        logger().info(&format!(
            "[ACTOR] [LFS] actor={} fileExists=true fileAgeMinutes={:.1} cacheExpiryMinutes={} fileOld=false [{:.2}ms]",
            actor, age_minutes_f, cache_expiry, elapsed_ms
        ));

        Ok(info)
    }

    /// Saves actor info to the local cache (file system).
    ///
    /// # Arguments
    ///
    /// * `actor` - The actor handle or DID (used for the filename)
    /// * `info` - The ActorInfo to save
    ///
    /// # Example
    ///
    /// ```no_run
    /// use rustproto::fs::LocalFileSystem;
    /// use rustproto::ws::ActorInfo;
    ///
    /// let lfs = LocalFileSystem::initialize("./data").unwrap();
    /// let info = ActorInfo::with_actor("alice.bsky.social");
    /// lfs.save_actor_info("alice.bsky.social", &info).unwrap();
    /// ```
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

    #[test]
    fn test_save_and_resolve_actor_info() {
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
        let resolved = lfs.resolve_actor_info("alice.bsky.social", None).unwrap();
        assert_eq!(resolved.did, Some("did:plc:abc123".to_string()));
        assert_eq!(resolved.pds, Some("bsky.social".to_string()));

        let _ = fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_resolve_actor_info_not_found() {
        let temp_dir = env::temp_dir().join("rustproto_test_actor_not_found");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let lfs = LocalFileSystem::initialize(&temp_dir).unwrap();

        // Try to resolve a non-existent actor
        let result = lfs.resolve_actor_info("nonexistent.bsky.social", None);
        assert!(result.is_err());

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
