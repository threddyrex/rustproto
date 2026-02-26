//! Local file system for storing repos and data.
//!
//! This module provides functionality to manage the local data directory
//! structure, including subdirectories for actors, repos, sessions, etc.

use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Errors that can occur during file system operations.
#[derive(Error, Debug)]
pub enum LocalFileSystemError {
    #[error("Data directory is null or does not exist: {0}")]
    DataDirNotFound(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
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
    /// use rstproto::fs::LocalFileSystem;
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
        let temp_dir = env::temp_dir().join("rstproto_test_lfs");
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
        let temp_dir = env::temp_dir().join("rstproto_test_lfs2");
        let _ = fs::remove_dir_all(&temp_dir);
        fs::create_dir_all(&temp_dir).unwrap();

        let lfs = LocalFileSystem::initialize(&temp_dir).unwrap();
        let result = lfs.get_path_repo_file("");

        assert!(result.is_err());

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
