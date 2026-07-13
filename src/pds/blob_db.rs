//! storage implementation for blobs
//!
//! This module provides file-based storage for blob bytes.
//! Blob metadata is stored in SQLite (via PdsDb), while the actual
//! blob bytes are stored as files on disk.
//! The BlobDb trait defines the interface for blob storage, and BlobFileDb
//! is a concrete implementation that stores blobs as files in the `pds/blobs` directory.
//! Later we might provide other implementations (cloud storage, etc.).

use std::fs;
use std::io;
use std::path::PathBuf;

use crate::fs::LocalFileSystem;
use crate::log::Logger;


pub trait BlobDb {
    fn insert_blob_bytes(&self, cid: &str, bytes: &[u8]) -> Result<(), BlobDbError>;
    fn has_blob_bytes(&self, cid: &str) -> Result<bool, BlobDbError>;
    fn get_blob_bytes(&self, cid: &str) -> Result<Vec<u8>, BlobDbError>;
    fn delete_blob_bytes(&self, cid: &str) -> Result<(), BlobDbError>;
    fn update_blob_bytes(&self, cid: &str, bytes: &[u8]) -> Result<(), BlobDbError>;
}

#[derive(Debug, thiserror::Error)]
pub enum BlobDbError {
    #[error("IO error: {0}")]
    IoError(#[from] io::Error),
    #[error("Blob not found: {0}")]
    NotFound(String),
}

/// Entry point for callers.
pub fn create_blob_db<'a>(lfs: &'a LocalFileSystem, log: &'a Logger) -> impl BlobDb + 'a {
    BlobFileDb::new(lfs, log)
}


/// Blob database for storing and retrieving blob bytes.
///
/// Stores blobs as files in the `pds/blobs` directory, with filenames
/// derived from the CID (with invalid characters replaced).
pub struct BlobFileDb<'a> {
    lfs: &'a LocalFileSystem,
    log: &'a Logger,
}

impl<'a> BlobFileDb<'a> {
    /// Create a new BlobDb instance.
    ///
    /// # Arguments
    ///
    /// * `lfs` - LocalFileSystem instance
    /// * `log` - Logger instance
    fn new(lfs: &'a LocalFileSystem, log: &'a Logger) -> Self {
        Self { lfs, log }
    }

    /// Get the file path for a blob.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    fn get_blob_file_path(&self, cid: &str) -> PathBuf {
        let safe_cid = get_safe_filename(cid);
        self.lfs.get_data_dir().join("pds").join("blobs").join(safe_cid)
    }
}

impl<'a> BlobDb for BlobFileDb<'a> {


    /// Insert blob bytes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    /// * `bytes` - The blob bytes
    fn insert_blob_bytes(&self, cid: &str, bytes: &[u8]) -> Result<(), BlobDbError> {
        let file_path = self.get_blob_file_path(cid);
        self.log.info(&format!("[BLOB] InsertBlobBytes: {:?}", file_path));
        fs::write(&file_path, bytes).map_err(BlobDbError::IoError)
    }

    /// Check if blob bytes exist.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    fn has_blob_bytes(&self, cid: &str) -> Result<bool, BlobDbError> {
        let file_path = self.get_blob_file_path(cid);
        Ok(file_path.exists())
    }

    /// Get blob bytes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    ///
    /// # Returns
    ///
    /// The blob bytes if found, or an error if not found.
    fn get_blob_bytes(&self, cid: &str) -> Result<Vec<u8>, BlobDbError> {
        let file_path = self.get_blob_file_path(cid);
        if !file_path.exists() {
            return Err(BlobDbError::NotFound(format!("Blob not found: {}", cid)));
        }
        self.log.info(&format!("[BLOB] GetBlobBytes: {:?}", file_path));
        fs::read(&file_path).map_err(BlobDbError::IoError)
    }

    /// Delete blob bytes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    fn delete_blob_bytes(&self, cid: &str) -> Result<(), BlobDbError> {
        let file_path = self.get_blob_file_path(cid);
        if file_path.exists() {
            fs::remove_file(&file_path).map_err(BlobDbError::IoError)?;
        }
        Ok(())
    }

    /// Update blob bytes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    /// * `bytes` - The new blob bytes
    fn update_blob_bytes(&self, cid: &str, bytes: &[u8]) -> Result<(), BlobDbError> {
        let file_path = self.get_blob_file_path(cid);
        self.log.info(&format!("[BLOB] UpdateBlobBytes: {:?}", file_path));
        fs::write(&file_path, bytes).map_err(BlobDbError::IoError)
    }
}

/// Convert a string to a safe filename by replacing invalid characters.
fn get_safe_filename(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_safe_filename() {
        assert_eq!(get_safe_filename("abc123"), "abc123");
        assert_eq!(get_safe_filename("abc/123"), "abc_123");
        assert_eq!(get_safe_filename("abc:123"), "abc_123");
        assert_eq!(
            get_safe_filename("bafkreihcduyzpj4kzp2pbusw7lz5h3eud33y4uvahjqqn73xxnkegel5lq"),
            "bafkreihcduyzpj4kzp2pbusw7lz5h3eud33y4uvahjqqn73xxnkegel5lq"
        );
    }
}
