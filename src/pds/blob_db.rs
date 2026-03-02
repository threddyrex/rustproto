//! Blob database operations.
//!
//! This module provides file-based storage for blob bytes.
//! Blob metadata is stored in SQLite (via PdsDb), while the actual
//! blob bytes are stored as files on disk.

use std::fs;
use std::io;
use std::path::PathBuf;

use crate::fs::LocalFileSystem;
use crate::log::Logger;

/// Blob database for storing and retrieving blob bytes.
///
/// Stores blobs as files in the `pds/blobs` directory, with filenames
/// derived from the CID (with invalid characters replaced).
pub struct BlobDb<'a> {
    lfs: &'a LocalFileSystem,
    log: &'a Logger,
}

impl<'a> BlobDb<'a> {
    /// Create a new BlobDb instance.
    ///
    /// # Arguments
    ///
    /// * `lfs` - LocalFileSystem instance
    /// * `log` - Logger instance
    pub fn new(lfs: &'a LocalFileSystem, log: &'a Logger) -> Self {
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

    /// Insert blob bytes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    /// * `bytes` - The blob bytes
    pub fn insert_blob_bytes(&self, cid: &str, bytes: &[u8]) -> io::Result<()> {
        let file_path = self.get_blob_file_path(cid);
        self.log.info(&format!("[BLOB] InsertBlobBytes: {:?}", file_path));
        fs::write(&file_path, bytes)
    }

    /// Check if blob bytes exist.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    pub fn has_blob_bytes(&self, cid: &str) -> bool {
        let file_path = self.get_blob_file_path(cid);
        file_path.exists()
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
    pub fn get_blob_bytes(&self, cid: &str) -> io::Result<Vec<u8>> {
        let file_path = self.get_blob_file_path(cid);
        if !file_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Blob not found: {}", cid),
            ));
        }
        self.log.info(&format!("[BLOB] GetBlobBytes: {:?}", file_path));
        fs::read(&file_path)
    }

    /// Delete blob bytes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    pub fn delete_blob_bytes(&self, cid: &str) -> io::Result<()> {
        let file_path = self.get_blob_file_path(cid);
        if file_path.exists() {
            fs::remove_file(&file_path)?;
        }
        Ok(())
    }

    /// Update blob bytes.
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the blob
    /// * `bytes` - The new blob bytes
    pub fn update_blob_bytes(&self, cid: &str, bytes: &[u8]) -> io::Result<()> {
        let file_path = self.get_blob_file_path(cid);
        self.log.info(&format!("[BLOB] UpdateBlobBytes: {:?}", file_path));
        fs::write(&file_path, bytes)
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
