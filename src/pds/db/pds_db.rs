//! PDS Database operations.
//!
//! This module provides the main database operations for the PDS,
//! including table creation, configuration storage, and data access.

use std::path::PathBuf;
use std::sync::Mutex;

use chrono::Utc;
use rusqlite::Connection;
use thiserror::Error;

use super::entities::*;
use super::SqliteDb;
use crate::fs::LocalFileSystem;
use crate::log::Logger;

/// Errors that can occur during PDS database operations.
#[derive(Error, Debug)]
pub enum PdsDbError {
    #[error("Database directory does not exist: {0}")]
    DbDirNotFound(String),

    #[error("Database file does not exist: {0}")]
    DbFileNotFound(String),

    #[error("SQLite error: {0}")]
    SqliteError(#[from] rusqlite::Error),

    #[error("Config property not found: {0}")]
    ConfigPropertyNotFound(String),

    #[error("Config property has invalid value: {0}")]
    ConfigPropertyInvalid(String),

    #[error("Repo header not found")]
    RepoHeaderNotFound,

    #[error("Repo commit not found")]
    RepoCommitNotFound,

    #[error("Repo record not found: {0}/{1}")]
    RepoRecordNotFound(String, String),

    #[error("Firehose event not found: {0}")]
    FirehoseEventNotFound(i64),

    #[error("OAuth request not found: {0}")]
    OauthRequestNotFound(String),

    #[error("OAuth session not found: {0}")]
    OauthSessionNotFound(String),

    #[error("Passkey not found: {0}")]
    PasskeyNotFound(String),

    #[error("Statistic not found")]
    StatisticNotFound,

    #[error("Invalid repo commit: {0}")]
    InvalidRepoCommit(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Lock for sequence number operations.
static SEQUENCE_NUMBER_LOCK: Mutex<()> = Mutex::new(());

/// PDS Database - entry point for PDS database operations.
///
/// The PDS database is a local SQLite file that stores configuration,
/// repository data, sessions, and other PDS state.
pub struct PdsDb {
    db_path: PathBuf,
}

impl PdsDb {
    /// Connect to an existing PDS database.
    ///
    /// # Arguments
    ///
    /// * `lfs` - LocalFileSystem instance
    ///
    /// # Returns
    ///
    /// A PdsDb instance if the database exists, or an error if not found.
    pub fn connect(lfs: &LocalFileSystem) -> Result<Self, PdsDbError> {
        let db_dir = lfs.get_data_dir().join("pds");
        let db_path = lfs.get_path_pds_db();

        if !db_dir.exists() {
            return Err(PdsDbError::DbDirNotFound(
                db_dir.to_string_lossy().to_string(),
            ));
        }

        if !db_path.exists() {
            return Err(PdsDbError::DbFileNotFound(
                db_path.to_string_lossy().to_string(),
            ));
        }

        Ok(Self { db_path })
    }

    /// Get a read/write connection to the database.
    pub fn get_connection(&self) -> Result<Connection, PdsDbError> {
        Ok(SqliteDb::get_connection(&self.db_path)?)
    }

    /// Get a read/write/create connection to the database.
    pub fn get_connection_create(&self) -> Result<Connection, PdsDbError> {
        Ok(SqliteDb::get_connection_create(&self.db_path)?)
    }

    /// Get a read-only connection to the database.
    pub fn get_connection_read_only(&self) -> Result<Connection, PdsDbError> {
        Ok(SqliteDb::get_connection_read_only(&self.db_path)?)
    }

    // =========================================================================
    // DATE TIME HELPERS
    // =========================================================================

    /// Format a datetime for database storage.
    pub fn format_datetime_for_db(dt: chrono::DateTime<Utc>) -> String {
        format_datetime_for_db(dt)
    }

    /// Get the current datetime formatted for database storage.
    pub fn get_current_datetime_for_db() -> String {
        get_current_datetime_for_db()
    }

    // =========================================================================
    // CONFIG PROPERTY
    // =========================================================================

    /// Create the ConfigProperty table.
    pub fn create_table_config_property(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: ConfigProperty");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ConfigProperty (
                Key TEXT PRIMARY KEY NOT NULL,
                Value TEXT NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Get a configuration property value.
    pub fn get_config_property(&self, key: &str) -> Result<String, PdsDbError> {
        let conn = self.get_connection()?;
        let result: Result<String, rusqlite::Error> = conn.query_row(
            "SELECT Value FROM ConfigProperty WHERE Key = ?1",
            [key],
            |row| row.get(0),
        );

        match result {
            Ok(value) => Ok(value),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(PdsDbError::ConfigPropertyNotFound(key.to_string()))
            }
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Set a configuration property value.
    pub fn set_config_property(&self, key: &str, value: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT OR REPLACE INTO ConfigProperty (Key, Value) VALUES (?1, ?2)",
            [key, value],
        )?;
        Ok(())
    }

    /// Set a boolean configuration property.
    pub fn set_config_property_bool(&self, key: &str, value: bool) -> Result<(), PdsDbError> {
        self.set_config_property(key, if value { "1" } else { "0" })
    }

    /// Get a boolean configuration property.
    pub fn get_config_property_bool(&self, key: &str) -> Result<bool, PdsDbError> {
        let val = self.get_config_property(key)?;
        Ok(val == "1")
    }

    /// Set an integer configuration property.
    pub fn set_config_property_int(&self, key: &str, value: i32) -> Result<(), PdsDbError> {
        self.set_config_property(key, &value.to_string())
    }

    /// Get an integer configuration property.
    pub fn get_config_property_int(&self, key: &str) -> Result<i32, PdsDbError> {
        let val = self.get_config_property(key)?;
        val.parse::<i32>().map_err(|_| {
            PdsDbError::ConfigPropertyInvalid(format!(
                "Property '{}' has non-integer value: {}",
                key, val
            ))
        })
    }

    /// Delete all configuration properties.
    pub fn delete_all_config_properties(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM ConfigProperty", [])?;
        Ok(())
    }

    /// Check if a configuration property exists.
    pub fn config_property_exists(&self, key: &str) -> Result<bool, PdsDbError> {
        let conn = self.get_connection()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM ConfigProperty WHERE Key = ?1",
            [key],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Get a configuration property as a HashSet of comma-separated values.
    pub fn get_config_property_hash_set(
        &self,
        key: &str,
    ) -> Result<std::collections::HashSet<String>, PdsDbError> {
        let val = self.get_config_property(key)?;
        let items: std::collections::HashSet<String> = val
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Ok(items)
    }

    /// Get all configuration properties as key-value pairs.
    pub fn get_all_config_properties(&self) -> Result<Vec<(String, String)>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt = conn.prepare("SELECT Key, Value FROM ConfigProperty ORDER BY Key")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut properties = Vec::new();
        for row in rows {
            properties.push(row?);
        }
        Ok(properties)
    }

    // =========================================================================
    // BLOB
    // =========================================================================

    /// Create the Blob table.
    pub fn create_table_blob(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: Blob");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS Blob (
                Cid TEXT PRIMARY KEY,
                ContentType TEXT NOT NULL,
                ContentLength INTEGER NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Check if a blob exists.
    pub fn blob_exists(&self, cid: &str) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM Blob WHERE Cid = ?1",
            [cid],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Insert a new blob.
    pub fn insert_blob(&self, blob: &Blob) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO Blob (Cid, ContentType, ContentLength) VALUES (?1, ?2, ?3)",
            rusqlite::params![blob.cid, blob.content_type, blob.content_length],
        )?;
        Ok(())
    }

    /// Update an existing blob.
    pub fn update_blob(&self, blob: &Blob) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "UPDATE Blob SET ContentType = ?1, ContentLength = ?2 WHERE Cid = ?3",
            rusqlite::params![blob.content_type, blob.content_length, blob.cid],
        )?;
        Ok(())
    }

    /// Get a blob by CID.
    pub fn get_blob_by_cid(&self, cid: &str) -> Result<Option<Blob>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result = conn.query_row(
            "SELECT Cid, ContentType, ContentLength FROM Blob WHERE Cid = ?1 LIMIT 1",
            [cid],
            |row| {
                Ok(Blob {
                    cid: row.get(0)?,
                    content_type: row.get(1)?,
                    content_length: row.get(2)?,
                })
            },
        );

        match result {
            Ok(blob) => Ok(Some(blob)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// List blob CIDs with pagination.
    pub fn list_blobs_with_cursor(
        &self,
        cursor: Option<&str>,
        limit: i32,
    ) -> Result<Vec<String>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut blobs = Vec::new();

        match cursor {
            Some(c) => {
                let mut stmt = conn.prepare(
                    "SELECT Cid FROM Blob WHERE Cid > ?1 ORDER BY Cid ASC LIMIT ?2",
                )?;
                let rows = stmt.query_map(rusqlite::params![c, limit], |row| row.get(0))?;
                for row in rows {
                    blobs.push(row?);
                }
            }
            None => {
                let mut stmt =
                    conn.prepare("SELECT Cid FROM Blob ORDER BY Cid ASC LIMIT ?1")?;
                let rows = stmt.query_map([limit], |row| row.get(0))?;
                for row in rows {
                    blobs.push(row?);
                }
            }
        }

        Ok(blobs)
    }

    /// Delete all blobs.
    pub fn delete_all_blobs(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM Blob", [])?;
        Ok(())
    }

    // =========================================================================
    // PREFERENCES
    // =========================================================================

    /// Create the Preferences table.
    pub fn create_table_preferences(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: Preferences");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS Preferences (
                Prefs TEXT NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Get preferences.
    pub fn get_preferences(&self) -> Result<String, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result: Result<String, rusqlite::Error> =
            conn.query_row("SELECT Prefs FROM Preferences LIMIT 1", [], |row| row.get(0));

        match result {
            Ok(prefs) => Ok(prefs),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(String::new()),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Get the count of preference rows.
    pub fn get_preferences_count(&self) -> Result<i32, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let count: i32 =
            conn.query_row("SELECT COUNT(*) FROM Preferences", [], |row| row.get(0))?;
        Ok(count)
    }

    /// Insert preferences.
    pub fn insert_preferences(&self, prefs: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("INSERT INTO Preferences (Prefs) VALUES (?1)", [prefs])?;
        Ok(())
    }

    /// Update preferences.
    pub fn update_preferences(&self, prefs: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("UPDATE Preferences SET Prefs = ?1", [prefs])?;
        Ok(())
    }

    /// Delete preferences.
    pub fn delete_preferences(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM Preferences", [])?;
        Ok(())
    }

    // =========================================================================
    // REPO HEADER
    // =========================================================================

    /// Create the RepoHeader table.
    pub fn create_table_repo_header(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: RepoHeader");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS RepoHeader (
                RepoCommitCid TEXT PRIMARY KEY,
                Version INTEGER NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Check if a repo header exists.
    pub fn repo_header_exists(&self) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM RepoHeader", [], |row| row.get(0))?;
        Ok(count == 1)
    }

    /// Get the repo header.
    pub fn get_repo_header(&self) -> Result<DbRepoHeader, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result = conn.query_row(
            "SELECT RepoCommitCid, Version FROM RepoHeader LIMIT 1",
            [],
            |row| {
                Ok(DbRepoHeader {
                    repo_commit_cid: row.get(0)?,
                    version: row.get(1)?,
                })
            },
        );

        match result {
            Ok(header) => Ok(header),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(PdsDbError::RepoHeaderNotFound),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Insert or update the repo header.
    pub fn insert_update_repo_header(&self, header: &DbRepoHeader) -> Result<(), PdsDbError> {
        if self.repo_header_exists()? {
            self.update_repo_header(header)
        } else {
            self.insert_repo_header(header)
        }
    }

    fn insert_repo_header(&self, header: &DbRepoHeader) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO RepoHeader (RepoCommitCid, Version) VALUES (?1, ?2)",
            rusqlite::params![header.repo_commit_cid, header.version],
        )?;
        Ok(())
    }

    fn update_repo_header(&self, header: &DbRepoHeader) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "UPDATE RepoHeader SET Version = ?1, RepoCommitCid = ?2",
            rusqlite::params![header.version, header.repo_commit_cid],
        )?;
        Ok(())
    }

    /// Delete the repo header.
    pub fn delete_repo_header(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM RepoHeader", [])?;
        Ok(())
    }

    // =========================================================================
    // REPO COMMIT
    // =========================================================================

    /// Create the RepoCommit table.
    pub fn create_table_repo_commit(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: RepoCommit");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS RepoCommit (
                Version INTEGER NOT NULL,
                Cid TEXT PRIMARY KEY,
                RootMstNodeCid TEXT NOT NULL,
                Rev TEXT NOT NULL,
                PrevMstNodeCid TEXT,
                Signature BLOB NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Check if a repo commit exists (returns true if table is empty).
    pub fn repo_commit_exists(&self) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let count: i64 =
            conn.query_row("SELECT COUNT(*) FROM RepoCommit", [], |row| row.get(0))?;
        Ok(count == 0)
    }

    /// Get the repo commit.
    pub fn get_repo_commit(&self) -> Result<DbRepoCommit, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result = conn.query_row(
            "SELECT Version, Cid, RootMstNodeCid, Rev, PrevMstNodeCid, Signature FROM RepoCommit LIMIT 1",
            [],
            |row| {
                Ok(DbRepoCommit {
                    version: row.get(0)?,
                    cid: row.get(1)?,
                    root_mst_node_cid: row.get(2)?,
                    rev: row.get(3)?,
                    prev_mst_node_cid: row.get(4)?,
                    signature: row.get(5)?,
                })
            },
        );

        match result {
            Ok(commit) => Ok(commit),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(PdsDbError::RepoCommitNotFound),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Insert or update the repo commit.
    pub fn insert_update_repo_commit(&self, commit: &DbRepoCommit) -> Result<(), PdsDbError> {
        if self.repo_commit_exists()? {
            self.insert_repo_commit(commit)
        } else {
            self.update_repo_commit(commit)
        }
    }

    fn validate_repo_commit(commit: &DbRepoCommit) -> Result<(), PdsDbError> {
        if commit.cid.is_empty() || commit.rev.is_empty() || commit.signature.is_empty() {
            return Err(PdsDbError::InvalidRepoCommit(
                "RepoCommit needs Rev, Signature, Cid.".to_string(),
            ));
        }
        Ok(())
    }

    fn insert_repo_commit(&self, commit: &DbRepoCommit) -> Result<(), PdsDbError> {
        Self::validate_repo_commit(commit)?;
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO RepoCommit (Version, Cid, RootMstNodeCid, Rev, PrevMstNodeCid, Signature)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                commit.version,
                commit.cid,
                commit.root_mst_node_cid,
                commit.rev,
                commit.prev_mst_node_cid,
                commit.signature
            ],
        )?;
        Ok(())
    }

    /// Update the repo commit.
    pub fn update_repo_commit(&self, commit: &DbRepoCommit) -> Result<(), PdsDbError> {
        Self::validate_repo_commit(commit)?;
        let conn = self.get_connection()?;
        conn.execute(
            "UPDATE RepoCommit SET Version = ?1, Cid = ?2, RootMstNodeCid = ?3, Rev = ?4, PrevMstNodeCid = ?5, Signature = ?6",
            rusqlite::params![
                commit.version,
                commit.cid,
                commit.root_mst_node_cid,
                commit.rev,
                commit.prev_mst_node_cid,
                commit.signature
            ],
        )?;
        Ok(())
    }

    /// Delete the repo commit.
    pub fn delete_repo_commit(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM RepoCommit", [])?;
        Ok(())
    }

    // =========================================================================
    // REPO RECORD
    // =========================================================================

    /// Create the RepoRecord table.
    pub fn create_table_repo_record(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: RepoRecord");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS RepoRecord (
                Collection TEXT NOT NULL,
                Rkey TEXT NOT NULL,
                Cid TEXT NOT NULL,
                DagCborObject BLOB NOT NULL,
                PRIMARY KEY (Collection, Rkey)
            )",
            [],
        )?;
        Ok(())
    }

    /// Insert a repo record.
    pub fn insert_repo_record(
        &self,
        collection: &str,
        rkey: &str,
        cid: &str,
        dag_cbor_bytes: &[u8],
    ) -> Result<(), PdsDbError> {
        if collection.is_empty() || rkey.is_empty() {
            return Err(PdsDbError::InvalidInput(
                "Collection and Rkey cannot be empty.".to_string(),
            ));
        }

        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO RepoRecord (Collection, Rkey, Cid, DagCborObject) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![collection, rkey, cid, dag_cbor_bytes],
        )?;
        Ok(())
    }

    /// Get a repo record.
    pub fn get_repo_record(&self, collection: &str, rkey: &str) -> Result<DbRepoRecord, PdsDbError> {
        if collection.is_empty() || rkey.is_empty() {
            return Err(PdsDbError::InvalidInput(
                "Collection and Rkey cannot be empty.".to_string(),
            ));
        }

        let conn = self.get_connection_read_only()?;
        let result = conn.query_row(
            "SELECT Collection, Rkey, Cid, DagCborObject FROM RepoRecord WHERE Collection = ?1 AND Rkey = ?2 LIMIT 1",
            [collection, rkey],
            |row| {
                Ok(DbRepoRecord {
                    collection: row.get(0)?,
                    rkey: row.get(1)?,
                    cid: row.get(2)?,
                    dag_cbor_bytes: row.get(3)?,
                })
            },
        );

        match result {
            Ok(record) => Ok(record),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(PdsDbError::RepoRecordNotFound(collection.to_string(), rkey.to_string()))
            }
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Check if a record exists.
    pub fn record_exists(&self, collection: &str, rkey: &str) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result: Result<i32, rusqlite::Error> = conn.query_row(
            "SELECT 1 FROM RepoRecord WHERE Collection = ?1 AND Rkey = ?2 LIMIT 1",
            [collection, rkey],
            |row| row.get(0),
        );

        match result {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Get all repo records.
    pub fn get_all_repo_records(&self) -> Result<Vec<DbRepoRecord>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt =
            conn.prepare("SELECT Collection, Rkey, Cid, DagCborObject FROM RepoRecord")?;
        let rows = stmt.query_map([], |row| {
            Ok(DbRepoRecord {
                collection: row.get(0)?,
                rkey: row.get(1)?,
                cid: row.get(2)?,
                dag_cbor_bytes: row.get(3)?,
            })
        })?;

        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }
        Ok(records)
    }

    /// Get unique collections.
    pub fn get_unique_collections(&self) -> Result<Vec<String>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt = conn.prepare("SELECT DISTINCT Collection FROM RepoRecord")?;
        let rows = stmt.query_map([], |row| row.get(0))?;

        let mut collections = Vec::new();
        for row in rows {
            collections.push(row?);
        }
        Ok(collections)
    }

    /// List repo records by collection with pagination.
    pub fn list_repo_records_by_collection(
        &self,
        collection: &str,
        limit: i32,
        cursor: Option<&str>,
        reverse: bool,
    ) -> Result<Vec<(String, DbRepoRecord)>, PdsDbError> {
        let cursor = cursor.unwrap_or("0");
        let conn = self.get_connection_read_only()?;

        let sql = if reverse {
            "SELECT Collection, Rkey, Cid, DagCborObject FROM RepoRecord 
             WHERE Collection = ?1 AND Rkey > ?2 ORDER BY Rkey ASC LIMIT ?3"
        } else {
            "SELECT Collection, Rkey, Cid, DagCborObject FROM RepoRecord 
             WHERE Collection = ?1 AND Rkey > ?2 ORDER BY Rkey DESC LIMIT ?3"
        };

        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::params![collection, cursor, limit], |row| {
            Ok(DbRepoRecord {
                collection: row.get(0)?,
                rkey: row.get(1)?,
                cid: row.get(2)?,
                dag_cbor_bytes: row.get(3)?,
            })
        })?;

        let mut records = Vec::new();
        for row in rows {
            let record = row?;
            let rkey = record.rkey.clone();
            records.push((rkey, record));
        }
        Ok(records)
    }

    /// Delete a repo record.
    pub fn delete_repo_record(&self, collection: &str, rkey: &str) -> Result<(), PdsDbError> {
        if collection.is_empty() || rkey.is_empty() {
            return Ok(());
        }

        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM RepoRecord WHERE Collection = ?1 AND Rkey = ?2",
            [collection, rkey],
        )?;
        Ok(())
    }

    /// Delete all repo records.
    pub fn delete_all_repo_records(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM RepoRecord", [])?;
        Ok(())
    }

    // =========================================================================
    // SEQUENCE NUMBER
    // =========================================================================

    /// Create the SequenceNumber table.
    pub fn create_table_sequence_number(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: SequenceNumber");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS SequenceNumber (
                Seq INTEGER NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Get a new sequence number for the firehose (thread-safe).
    pub fn get_new_sequence_number_for_firehose(&self) -> Result<i64, PdsDbError> {
        let _lock = SEQUENCE_NUMBER_LOCK.lock().unwrap();
        let current_seq = self.internal_get_current_sequence_number()?;
        self.delete_sequence_number()?;
        let new_seq = current_seq + 1;
        self.internal_insert_sequence_number(new_seq)?;
        Ok(new_seq)
    }

    /// Get the most recently used sequence number (thread-safe).
    pub fn get_most_recently_used_sequence_number(&self) -> Result<i64, PdsDbError> {
        let _lock = SEQUENCE_NUMBER_LOCK.lock().unwrap();
        self.internal_get_current_sequence_number()
    }

    fn internal_get_current_sequence_number(&self) -> Result<i64, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result: Result<i64, rusqlite::Error> =
            conn.query_row("SELECT Seq FROM SequenceNumber LIMIT 1", [], |row| row.get(0));

        match result {
            Ok(seq) => Ok(seq),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    fn internal_insert_sequence_number(&self, seq: i64) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO SequenceNumber (Seq) VALUES (?1)",
            [seq],
        )?;
        Ok(())
    }

    /// Delete all sequence numbers.
    pub fn delete_sequence_number(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM SequenceNumber", [])?;
        Ok(())
    }

    // =========================================================================
    // FIREHOSE EVENT
    // =========================================================================

    /// Create the FirehoseEvent table.
    pub fn create_table_firehose_event(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: FirehoseEvent");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS FirehoseEvent (
                SequenceNumber INTEGER PRIMARY KEY,
                CreatedDate TEXT NOT NULL,
                Header_op INTEGER NOT NULL,
                Header_t TEXT,
                Header_DagCborObject BLOB NOT NULL,
                Body_DagCborObject BLOB NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Insert a firehose event.
    pub fn insert_firehose_event(&self, event: &FirehoseEvent) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO FirehoseEvent (SequenceNumber, CreatedDate, Header_op, Header_t, Header_DagCborObject, Body_DagCborObject)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                event.sequence_number,
                event.created_date,
                event.header_op,
                event.header_t,
                event.header_dag_cbor_bytes,
                event.body_dag_cbor_bytes
            ],
        )?;
        Ok(())
    }

    /// Get a firehose event by sequence number.
    pub fn get_firehose_event(&self, sequence_number: i64) -> Result<FirehoseEvent, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result = conn.query_row(
            "SELECT SequenceNumber, CreatedDate, Header_op, Header_t, Header_DagCborObject, Body_DagCborObject
             FROM FirehoseEvent WHERE SequenceNumber = ?1 LIMIT 1",
            [sequence_number],
            |row| {
                Ok(FirehoseEvent {
                    sequence_number: row.get(0)?,
                    created_date: row.get(1)?,
                    header_op: row.get(2)?,
                    header_t: row.get(3)?,
                    header_dag_cbor_bytes: row.get(4)?,
                    body_dag_cbor_bytes: row.get(5)?,
                })
            },
        );

        match result {
            Ok(event) => Ok(event),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(PdsDbError::FirehoseEventNotFound(sequence_number))
            }
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Get firehose events for subscribeRepos.
    pub fn get_firehose_events_for_subscribe_repos(
        &self,
        cursor: i64,
        limit: i32,
        num_hours_look_back: i64,
    ) -> Result<Vec<FirehoseEvent>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let after_date = FirehoseEvent::get_created_date_minus_hours(num_hours_look_back);

        let mut stmt = conn.prepare(
            "SELECT SequenceNumber, CreatedDate, Header_op, Header_t, Header_DagCborObject, Body_DagCborObject
             FROM FirehoseEvent
             WHERE SequenceNumber > ?1 AND CreatedDate >= ?2
             ORDER BY SequenceNumber ASC
             LIMIT ?3",
        )?;

        let rows = stmt.query_map(rusqlite::params![cursor, after_date, limit], |row| {
            Ok(FirehoseEvent {
                sequence_number: row.get(0)?,
                created_date: row.get(1)?,
                header_op: row.get(2)?,
                header_t: row.get(3)?,
                header_dag_cbor_bytes: row.get(4)?,
                body_dag_cbor_bytes: row.get(5)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        Ok(events)
    }

    /// Delete all firehose events.
    pub fn delete_all_firehose_events(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM FirehoseEvent", [])?;
        Ok(())
    }

    /// Delete old firehose events.
    pub fn delete_old_firehose_events(&self, num_hours_look_back: i64) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        let after_date = FirehoseEvent::get_created_date_minus_hours(num_hours_look_back);
        conn.execute(
            "DELETE FROM FirehoseEvent WHERE CreatedDate < ?1",
            [after_date],
        )?;
        Ok(())
    }

    /// Get count of old firehose events.
    pub fn get_count_of_old_firehose_events(&self, num_hours_look_back: i64) -> Result<i32, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let after_date = FirehoseEvent::get_created_date_minus_hours(num_hours_look_back);
        let count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM FirehoseEvent WHERE CreatedDate < ?1",
            [after_date],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Hide a firehose event by negating its sequence number.
    pub fn hide_firehose_event(&self, sequence_number: i64) -> Result<(), PdsDbError> {
        let new_sequence_number = -sequence_number;
        let conn = self.get_connection()?;
        conn.execute(
            "UPDATE FirehoseEvent SET SequenceNumber = ?1 WHERE SequenceNumber = ?2",
            [new_sequence_number, sequence_number],
        )?;
        Ok(())
    }

    // =========================================================================
    // LOG LEVEL
    // =========================================================================

    /// Create the LogLevel table.
    pub fn create_table_log_level(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: LogLevel");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS LogLevel (
                Level TEXT NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Get the log level.
    pub fn get_log_level(&self) -> Result<String, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result: Result<String, rusqlite::Error> =
            conn.query_row("SELECT Level FROM LogLevel LIMIT 1", [], |row| row.get(0));

        match result {
            Ok(level) => Ok(level),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok("info".to_string()),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Get the log level count.
    pub fn get_log_level_count(&self) -> Result<i32, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let count: i32 =
            conn.query_row("SELECT COUNT(*) FROM LogLevel", [], |row| row.get(0))?;
        Ok(count)
    }

    /// Set the log level.
    pub fn set_log_level(&self, level: &str) -> Result<(), PdsDbError> {
        if self.get_log_level_count()? > 0 {
            self.delete_log_level()?;
        }
        self.insert_log_level(level)
    }

    fn insert_log_level(&self, level: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("INSERT INTO LogLevel (Level) VALUES (?1)", [level])?;
        Ok(())
    }

    /// Delete the log level.
    pub fn delete_log_level(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM LogLevel", [])?;
        Ok(())
    }

    // =========================================================================
    // OAUTH REQUEST
    // =========================================================================

    /// Create the OauthRequest table.
    pub fn create_table_oauth_request(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: OauthRequest");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS OauthRequest (
                RequestUri TEXT PRIMARY KEY,
                ExpiresDate TEXT NOT NULL,
                Dpop TEXT NOT NULL,
                Body TEXT NOT NULL,
                AuthorizationCode TEXT,
                AuthType TEXT
            )",
            [],
        )?;
        Ok(())
    }

    /// Insert an OAuth request.
    pub fn insert_oauth_request(&self, request: &OauthRequest) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO OauthRequest (RequestUri, ExpiresDate, Dpop, Body, AuthorizationCode, AuthType)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                request.request_uri,
                request.expires_date,
                request.dpop,
                request.body,
                request.authorization_code,
                request.auth_type
            ],
        )?;
        Ok(())
    }

    /// Check if an OAuth request exists.
    pub fn oauth_request_exists(&self, request_uri: &str) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let right_now = get_current_datetime_for_db();
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM OauthRequest WHERE RequestUri = ?1 AND ExpiresDate > ?2",
            rusqlite::params![request_uri, right_now],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Check if an OAuth request exists by authorization code.
    pub fn oauth_request_exists_by_authorization_code(
        &self,
        authorization_code: &str,
    ) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let right_now = get_current_datetime_for_db();
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM OauthRequest WHERE AuthorizationCode = ?1 AND ExpiresDate > ?2",
            rusqlite::params![authorization_code, right_now],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Get an OAuth request by request URI.
    pub fn get_oauth_request(&self, request_uri: &str) -> Result<OauthRequest, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let right_now = get_current_datetime_for_db();
        let result = conn.query_row(
            "SELECT RequestUri, ExpiresDate, Dpop, Body, AuthorizationCode, AuthType
             FROM OauthRequest WHERE RequestUri = ?1 AND ExpiresDate > ?2",
            rusqlite::params![request_uri, right_now],
            |row| {
                Ok(OauthRequest {
                    request_uri: row.get(0)?,
                    expires_date: row.get(1)?,
                    dpop: row.get(2)?,
                    body: row.get(3)?,
                    authorization_code: row.get(4)?,
                    auth_type: row.get(5)?,
                })
            },
        );

        match result {
            Ok(request) => Ok(request),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(PdsDbError::OauthRequestNotFound(request_uri.to_string()))
            }
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Get an OAuth request by authorization code.
    pub fn get_oauth_request_by_authorization_code(
        &self,
        authorization_code: &str,
    ) -> Result<OauthRequest, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let right_now = get_current_datetime_for_db();
        let result = conn.query_row(
            "SELECT RequestUri, ExpiresDate, Dpop, Body, AuthorizationCode, AuthType
             FROM OauthRequest WHERE AuthorizationCode = ?1 AND ExpiresDate > ?2",
            rusqlite::params![authorization_code, right_now],
            |row| {
                Ok(OauthRequest {
                    request_uri: row.get(0)?,
                    expires_date: row.get(1)?,
                    dpop: row.get(2)?,
                    body: row.get(3)?,
                    authorization_code: row.get(4)?,
                    auth_type: row.get(5)?,
                })
            },
        );

        match result {
            Ok(request) => Ok(request),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(PdsDbError::OauthRequestNotFound(authorization_code.to_string()))
            }
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Update an OAuth request.
    pub fn update_oauth_request(&self, request: &OauthRequest) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "UPDATE OauthRequest SET ExpiresDate = ?1, Dpop = ?2, Body = ?3, AuthorizationCode = ?4, AuthType = ?5
             WHERE RequestUri = ?6",
            rusqlite::params![
                request.expires_date,
                request.dpop,
                request.body,
                request.authorization_code,
                request.auth_type,
                request.request_uri
            ],
        )?;
        Ok(())
    }

    /// Delete all OAuth requests.
    pub fn delete_all_oauth_requests(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM OauthRequest", [])?;
        Ok(())
    }

    /// Delete old OAuth requests.
    pub fn delete_old_oauth_requests(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        let right_now = get_current_datetime_for_db();
        conn.execute(
            "DELETE FROM OauthRequest WHERE ExpiresDate < ?1",
            [right_now],
        )?;
        Ok(())
    }

    /// Delete an OAuth request by authorization code.
    pub fn delete_oauth_request_by_authorization_code(
        &self,
        authorization_code: &str,
    ) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM OauthRequest WHERE AuthorizationCode = ?1",
            [authorization_code],
        )?;
        Ok(())
    }

    // =========================================================================
    // OAUTH SESSION
    // =========================================================================

    /// Create the OauthSession table.
    pub fn create_table_oauth_session(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: OauthSession");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS OauthSession (
                SessionId TEXT PRIMARY KEY,
                ClientId TEXT NOT NULL,
                Scope TEXT NOT NULL,
                DpopJwkThumbprint TEXT NOT NULL,
                RefreshToken TEXT NOT NULL,
                RefreshTokenExpiresDate TEXT NOT NULL,
                CreatedDate TEXT NOT NULL,
                IpAddress TEXT NOT NULL,
                AuthType TEXT NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS IX_OauthSession_DpopJwkThumbprint 
             ON OauthSession(DpopJwkThumbprint)",
            [],
        )?;

        Ok(())
    }

    /// Insert an OAuth session.
    pub fn insert_oauth_session(&self, session: &OauthSession) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO OauthSession (SessionId, ClientId, Scope, DpopJwkThumbprint, RefreshToken, RefreshTokenExpiresDate, CreatedDate, IpAddress, AuthType)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                session.session_id,
                session.client_id,
                session.scope,
                session.dpop_jwk_thumbprint,
                session.refresh_token,
                session.refresh_token_expires_date,
                session.created_date,
                session.ip_address,
                session.auth_type
            ],
        )?;
        Ok(())
    }

    /// Get an OAuth session by session ID.
    pub fn get_oauth_session_by_session_id(
        &self,
        session_id: &str,
    ) -> Result<OauthSession, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result = conn.query_row(
            "SELECT SessionId, ClientId, Scope, DpopJwkThumbprint, RefreshToken, RefreshTokenExpiresDate, CreatedDate, IpAddress, AuthType
             FROM OauthSession WHERE SessionId = ?1",
            [session_id],
            |row| {
                Ok(OauthSession {
                    session_id: row.get(0)?,
                    client_id: row.get(1)?,
                    scope: row.get(2)?,
                    dpop_jwk_thumbprint: row.get(3)?,
                    refresh_token: row.get(4)?,
                    refresh_token_expires_date: row.get(5)?,
                    created_date: row.get(6)?,
                    ip_address: row.get(7)?,
                    auth_type: row.get(8)?,
                })
            },
        );

        match result {
            Ok(session) => Ok(session),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(PdsDbError::OauthSessionNotFound(session_id.to_string()))
            }
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Check if an OAuth session exists by refresh token.
    pub fn has_oauth_session_by_refresh_token(&self, refresh_token: &str) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let right_now = get_current_datetime_for_db();
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM OauthSession WHERE RefreshToken = ?1 AND RefreshTokenExpiresDate > ?2",
            rusqlite::params![refresh_token, right_now],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Get an OAuth session by refresh token.
    pub fn get_oauth_session_by_refresh_token(
        &self,
        refresh_token: &str,
    ) -> Result<OauthSession, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let right_now = get_current_datetime_for_db();
        let result = conn.query_row(
            "SELECT SessionId, ClientId, Scope, DpopJwkThumbprint, RefreshToken, RefreshTokenExpiresDate, CreatedDate, IpAddress, AuthType
             FROM OauthSession WHERE RefreshToken = ?1 AND RefreshTokenExpiresDate > ?2",
            rusqlite::params![refresh_token, right_now],
            |row| {
                Ok(OauthSession {
                    session_id: row.get(0)?,
                    client_id: row.get(1)?,
                    scope: row.get(2)?,
                    dpop_jwk_thumbprint: row.get(3)?,
                    refresh_token: row.get(4)?,
                    refresh_token_expires_date: row.get(5)?,
                    created_date: row.get(6)?,
                    ip_address: row.get(7)?,
                    auth_type: row.get(8)?,
                })
            },
        );

        match result {
            Ok(session) => Ok(session),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(PdsDbError::OauthSessionNotFound(refresh_token.to_string()))
            }
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Update an OAuth session.
    pub fn update_oauth_session(&self, session: &OauthSession) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "UPDATE OauthSession SET ClientId = ?1, Scope = ?2, DpopJwkThumbprint = ?3, RefreshToken = ?4, 
             RefreshTokenExpiresDate = ?5, CreatedDate = ?6, IpAddress = ?7, AuthType = ?8
             WHERE SessionId = ?9",
            rusqlite::params![
                session.client_id,
                session.scope,
                session.dpop_jwk_thumbprint,
                session.refresh_token,
                session.refresh_token_expires_date,
                session.created_date,
                session.ip_address,
                session.auth_type,
                session.session_id
            ],
        )?;
        Ok(())
    }

    /// Delete an OAuth session by refresh token.
    pub fn delete_oauth_session_by_refresh_token(&self, refresh_token: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM OauthSession WHERE RefreshToken = ?1",
            [refresh_token],
        )?;
        Ok(())
    }

    /// Delete an OAuth session by session ID.
    pub fn delete_oauth_session_by_session_id(&self, session_id: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM OauthSession WHERE SessionId = ?1",
            [session_id],
        )?;
        Ok(())
    }

    /// Check if a valid OAuth session exists by DPoP thumbprint.
    pub fn has_valid_oauth_session_by_dpop_thumbprint(
        &self,
        dpop_jwk_thumbprint: &str,
    ) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let right_now = get_current_datetime_for_db();
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM OauthSession WHERE DpopJwkThumbprint = ?1 AND RefreshTokenExpiresDate > ?2",
            rusqlite::params![dpop_jwk_thumbprint, right_now],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Get an OAuth session by DPoP thumbprint.
    pub fn get_oauth_session_by_dpop_thumbprint(
        &self,
        dpop_jwk_thumbprint: &str,
    ) -> Result<Option<OauthSession>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let right_now = get_current_datetime_for_db();
        let result = conn.query_row(
            "SELECT SessionId, ClientId, Scope, DpopJwkThumbprint, RefreshToken, RefreshTokenExpiresDate, CreatedDate, IpAddress, AuthType
             FROM OauthSession WHERE DpopJwkThumbprint = ?1 AND RefreshTokenExpiresDate > ?2",
            rusqlite::params![dpop_jwk_thumbprint, right_now],
            |row| {
                Ok(OauthSession {
                    session_id: row.get(0)?,
                    client_id: row.get(1)?,
                    scope: row.get(2)?,
                    dpop_jwk_thumbprint: row.get(3)?,
                    refresh_token: row.get(4)?,
                    refresh_token_expires_date: row.get(5)?,
                    created_date: row.get(6)?,
                    ip_address: row.get(7)?,
                    auth_type: row.get(8)?,
                })
            },
        );

        match result {
            Ok(session) => Ok(Some(session)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Get all OAuth sessions.
    pub fn get_all_oauth_sessions(&self) -> Result<Vec<OauthSession>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt = conn.prepare(
            "SELECT SessionId, ClientId, Scope, DpopJwkThumbprint, RefreshToken, RefreshTokenExpiresDate, CreatedDate, IpAddress, AuthType
             FROM OauthSession",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(OauthSession {
                session_id: row.get(0)?,
                client_id: row.get(1)?,
                scope: row.get(2)?,
                dpop_jwk_thumbprint: row.get(3)?,
                refresh_token: row.get(4)?,
                refresh_token_expires_date: row.get(5)?,
                created_date: row.get(6)?,
                ip_address: row.get(7)?,
                auth_type: row.get(8)?,
            })
        })?;

        let mut sessions = Vec::new();
        for row in rows {
            sessions.push(row?);
        }
        Ok(sessions)
    }

    /// Delete old OAuth sessions.
    pub fn delete_old_oauth_sessions(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        let right_now = get_current_datetime_for_db();
        conn.execute(
            "DELETE FROM OauthSession WHERE RefreshTokenExpiresDate < ?1",
            [right_now],
        )?;
        Ok(())
    }

    /// Delete all OAuth sessions.
    pub fn delete_all_oauth_sessions(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM OauthSession", [])?;
        Ok(())
    }

    // =========================================================================
    // LEGACY SESSION
    // =========================================================================

    /// Create the LegacySession table.
    pub fn create_table_legacy_session(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: LegacySession");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS LegacySession (
                CreatedDate TEXT NOT NULL,
                AccessJwt TEXT PRIMARY KEY,
                RefreshJwt TEXT NOT NULL,
                IpAddress TEXT NOT NULL,
                UserAgent TEXT NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Create a legacy session.
    pub fn create_legacy_session(&self, session: &LegacySession) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO LegacySession (CreatedDate, AccessJwt, RefreshJwt, IpAddress, UserAgent)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                session.created_date,
                session.access_jwt,
                session.refresh_jwt,
                session.ip_address,
                session.user_agent
            ],
        )?;
        Ok(())
    }

    /// Check if a legacy session exists by access JWT.
    pub fn legacy_session_exists_for_access_jwt(&self, access_jwt: &str) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM LegacySession WHERE AccessJwt = ?1",
            [access_jwt],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Check if a legacy session exists by refresh JWT.
    pub fn legacy_session_exists_for_refresh_jwt(&self, refresh_jwt: &str) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM LegacySession WHERE RefreshJwt = ?1",
            [refresh_jwt],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Delete a legacy session by refresh JWT.
    pub fn delete_legacy_session_for_refresh_jwt(&self, refresh_jwt: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM LegacySession WHERE RefreshJwt = ?1",
            [refresh_jwt],
        )?;
        Ok(())
    }

    /// Delete all legacy sessions.
    pub fn delete_all_legacy_sessions(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM LegacySession", [])?;
        Ok(())
    }

    /// Get all legacy sessions.
    pub fn get_all_legacy_sessions(&self) -> Result<Vec<LegacySession>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt = conn.prepare(
            "SELECT CreatedDate, AccessJwt, RefreshJwt, IpAddress, UserAgent FROM LegacySession",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(LegacySession {
                created_date: row.get(0)?,
                access_jwt: row.get(1)?,
                refresh_jwt: row.get(2)?,
                ip_address: row.get(3)?,
                user_agent: row.get(4)?,
            })
        })?;

        let mut sessions = Vec::new();
        for row in rows {
            sessions.push(row?);
        }
        Ok(sessions)
    }

    // =========================================================================
    // ADMIN SESSION
    // =========================================================================

    /// Create the AdminSession table.
    pub fn create_table_admin_session(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: AdminSession");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS AdminSession (
                SessionId TEXT PRIMARY KEY,
                CreatedDate TEXT NOT NULL,
                IpAddress TEXT NOT NULL,
                UserAgent TEXT NOT NULL,
                AuthType TEXT NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Insert an admin session.
    pub fn insert_admin_session(&self, session: &AdminSession) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO AdminSession (SessionId, CreatedDate, IpAddress, UserAgent, AuthType)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                session.session_id,
                session.created_date,
                session.ip_address,
                session.user_agent,
                session.auth_type
            ],
        )?;
        Ok(())
    }

    /// Get a valid admin session.
    pub fn get_valid_admin_session(
        &self,
        session_id: &str,
        ip_address: &str,
        timeout_minutes: i32,
    ) -> Result<Option<AdminSession>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let cutoff_date = format_datetime_for_db(
            Utc::now() - chrono::Duration::minutes(timeout_minutes as i64),
        );
        let result = conn.query_row(
            "SELECT SessionId, CreatedDate, IpAddress, UserAgent, AuthType
             FROM AdminSession WHERE SessionId = ?1 AND IpAddress = ?2 AND CreatedDate > ?3",
            rusqlite::params![session_id, ip_address, cutoff_date],
            |row| {
                Ok(AdminSession {
                    session_id: row.get(0)?,
                    created_date: row.get(1)?,
                    ip_address: row.get(2)?,
                    user_agent: row.get(3)?,
                    auth_type: row.get(4)?,
                })
            },
        );

        match result {
            Ok(session) => Ok(Some(session)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Get a valid admin session without IP address check.
    /// 
    /// Useful for development/testing where IP may not be consistent.
    pub fn get_valid_admin_session_any_ip(
        &self,
        session_id: &str,
        timeout_minutes: i32,
    ) -> Result<Option<AdminSession>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let cutoff_date = format_datetime_for_db(
            Utc::now() - chrono::Duration::minutes(timeout_minutes as i64),
        );
        let result = conn.query_row(
            "SELECT SessionId, CreatedDate, IpAddress, UserAgent, AuthType
             FROM AdminSession WHERE SessionId = ?1 AND CreatedDate > ?2",
            rusqlite::params![session_id, cutoff_date],
            |row| {
                Ok(AdminSession {
                    session_id: row.get(0)?,
                    created_date: row.get(1)?,
                    ip_address: row.get(2)?,
                    user_agent: row.get(3)?,
                    auth_type: row.get(4)?,
                })
            },
        );

        match result {
            Ok(session) => Ok(Some(session)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Get all admin sessions.
    pub fn get_all_admin_sessions(&self) -> Result<Vec<AdminSession>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt = conn.prepare(
            "SELECT SessionId, CreatedDate, IpAddress, UserAgent, AuthType FROM AdminSession",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(AdminSession {
                session_id: row.get(0)?,
                created_date: row.get(1)?,
                ip_address: row.get(2)?,
                user_agent: row.get(3)?,
                auth_type: row.get(4)?,
            })
        })?;

        let mut sessions = Vec::new();
        for row in rows {
            sessions.push(row?);
        }
        Ok(sessions)
    }

    /// Delete stale admin sessions.
    pub fn delete_stale_admin_sessions(&self, timeout_minutes: i32) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        let cutoff_date = format_datetime_for_db(
            Utc::now() - chrono::Duration::minutes(timeout_minutes as i64),
        );
        conn.execute(
            "DELETE FROM AdminSession WHERE CreatedDate <= ?1",
            [cutoff_date],
        )?;
        Ok(())
    }

    /// Delete an admin session.
    pub fn delete_admin_session(&self, session_id: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM AdminSession WHERE SessionId = ?1",
            [session_id],
        )?;
        Ok(())
    }

    /// Delete all admin sessions.
    pub fn delete_all_admin_sessions(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM AdminSession", [])?;
        Ok(())
    }

    // =========================================================================
    // PASSKEY
    // =========================================================================

    /// Create the Passkey table.
    pub fn create_table_passkey(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: Passkey");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS Passkey (
                Name TEXT PRIMARY KEY,
                CreatedDate TEXT NOT NULL,
                CredentialId TEXT NOT NULL,
                PublicKey TEXT NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Insert a passkey.
    pub fn insert_passkey(&self, passkey: &Passkey) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO Passkey (Name, CreatedDate, CredentialId, PublicKey)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                passkey.name,
                passkey.created_date,
                passkey.credential_id,
                passkey.public_key
            ],
        )?;
        Ok(())
    }

    /// Get all passkeys.
    pub fn get_all_passkeys(&self) -> Result<Vec<Passkey>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt = conn.prepare(
            "SELECT Name, CreatedDate, CredentialId, PublicKey FROM Passkey",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Passkey {
                name: row.get(0)?,
                created_date: row.get(1)?,
                credential_id: row.get(2)?,
                public_key: row.get(3)?,
            })
        })?;

        let mut passkeys = Vec::new();
        for row in rows {
            passkeys.push(row?);
        }
        Ok(passkeys)
    }

    /// Delete all passkeys.
    pub fn delete_all_passkeys(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM Passkey", [])?;
        Ok(())
    }

    /// Get a passkey by credential ID.
    pub fn get_passkey_by_credential_id(&self, credential_id: &str) -> Result<Passkey, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result = conn.query_row(
            "SELECT Name, CreatedDate, CredentialId, PublicKey FROM Passkey WHERE CredentialId = ?1",
            [credential_id],
            |row| {
                Ok(Passkey {
                    name: row.get(0)?,
                    created_date: row.get(1)?,
                    credential_id: row.get(2)?,
                    public_key: row.get(3)?,
                })
            },
        );

        match result {
            Ok(passkey) => Ok(passkey),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(PdsDbError::PasskeyNotFound(credential_id.to_string()))
            }
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Delete a passkey by name.
    pub fn delete_passkey_by_name(&self, name: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM Passkey WHERE Name = ?1", [name])?;
        Ok(())
    }

    // =========================================================================
    // PASSKEY CHALLENGE
    // =========================================================================

    /// Create the PasskeyChallenge table.
    pub fn create_table_passkey_challenge(
        conn: &Connection,
        log: &Logger,
    ) -> Result<(), PdsDbError> {
        log.info("table: PasskeyChallenge");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS PasskeyChallenge (
                Challenge TEXT PRIMARY KEY,
                CreatedDate TEXT NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    /// Insert a passkey challenge.
    pub fn insert_passkey_challenge(&self, challenge: &PasskeyChallenge) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO PasskeyChallenge (Challenge, CreatedDate) VALUES (?1, ?2)",
            rusqlite::params![challenge.challenge, challenge.created_date],
        )?;
        Ok(())
    }

    /// Get a passkey challenge.
    pub fn get_passkey_challenge(&self, challenge: &str) -> Result<Option<PasskeyChallenge>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result = conn.query_row(
            "SELECT Challenge, CreatedDate FROM PasskeyChallenge WHERE Challenge = ?1",
            [challenge],
            |row| {
                Ok(PasskeyChallenge {
                    challenge: row.get(0)?,
                    created_date: row.get(1)?,
                })
            },
        );

        match result {
            Ok(c) => Ok(Some(c)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Delete a passkey challenge.
    pub fn delete_passkey_challenge(&self, challenge: &str) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM PasskeyChallenge WHERE Challenge = ?1",
            [challenge],
        )?;
        Ok(())
    }

    /// Delete all passkey challenges.
    pub fn delete_all_passkey_challenges(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM PasskeyChallenge", [])?;
        Ok(())
    }

    /// Get all passkey challenges.
    pub fn get_all_passkey_challenges(&self) -> Result<Vec<PasskeyChallenge>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt = conn.prepare("SELECT Challenge, CreatedDate FROM PasskeyChallenge")?;
        let rows = stmt.query_map([], |row| {
            Ok(PasskeyChallenge {
                challenge: row.get(0)?,
                created_date: row.get(1)?,
            })
        })?;

        let mut challenges = Vec::new();
        for row in rows {
            challenges.push(row?);
        }
        Ok(challenges)
    }

    // =========================================================================
    // STATISTIC
    // =========================================================================

    /// Create the Statistic table.
    pub fn create_table_statistic(conn: &Connection, log: &Logger) -> Result<(), PdsDbError> {
        log.info("table: Statistic");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS Statistic (
                Name TEXT NOT NULL,
                IpAddress TEXT NOT NULL,
                UserAgent TEXT NOT NULL,
                Value INTEGER NOT NULL,
                LastUpdatedDate TEXT NOT NULL
            )",
            [],
        )?;
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS IX_Statistic_Name_UserKey
             ON Statistic (Name, IpAddress, UserAgent)",
            [],
        )?;
        Ok(())
    }

    /// Increment a statistic.
    pub fn increment_statistic(&self, key: &StatisticKey) -> Result<(), PdsDbError> {
        if self.statistic_exists(key)? {
            let current = self.get_statistic_value(key)?;
            self.update_statistic(key, current + 1)
        } else {
            self.insert_statistic(key, 1)
        }
    }

    /// Get a statistic value.
    pub fn get_statistic_value(&self, key: &StatisticKey) -> Result<i64, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let result: Result<i64, rusqlite::Error> = conn.query_row(
            "SELECT Value FROM Statistic WHERE Name = ?1 AND IpAddress = ?2 AND UserAgent = ?3",
            rusqlite::params![key.name, key.ip_address, key.user_agent],
            |row| row.get(0),
        );

        match result {
            Ok(value) => Ok(value),
            Err(rusqlite::Error::QueryReturnedNoRows) => Err(PdsDbError::StatisticNotFound),
            Err(e) => Err(PdsDbError::SqliteError(e)),
        }
    }

    /// Check if a statistic exists.
    pub fn statistic_exists(&self, key: &StatisticKey) -> Result<bool, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(1) FROM Statistic WHERE Name = ?1 AND IpAddress = ?2 AND UserAgent = ?3",
            rusqlite::params![key.name, key.ip_address, key.user_agent],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    fn insert_statistic(&self, key: &StatisticKey, value: i64) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT INTO Statistic (Name, IpAddress, UserAgent, Value, LastUpdatedDate)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                key.name,
                key.ip_address,
                key.user_agent,
                value,
                get_current_datetime_for_db()
            ],
        )?;
        Ok(())
    }

    fn update_statistic(&self, key: &StatisticKey, value: i64) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "UPDATE Statistic SET Value = ?1, LastUpdatedDate = ?2
             WHERE Name = ?3 AND IpAddress = ?4 AND UserAgent = ?5",
            rusqlite::params![
                value,
                get_current_datetime_for_db(),
                key.name,
                key.ip_address,
                key.user_agent
            ],
        )?;
        Ok(())
    }

    /// Delete all statistics.
    pub fn delete_all_statistics(&self) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute("DELETE FROM Statistic", [])?;
        Ok(())
    }

    /// Delete old statistics.
    pub fn delete_old_statistics(&self, hours_lookback: i64) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        let cutoff_date = format_datetime_for_db(Utc::now() - chrono::Duration::hours(hours_lookback));
        conn.execute(
            "DELETE FROM Statistic WHERE LastUpdatedDate < ?1",
            [cutoff_date],
        )?;
        Ok(())
    }

    /// Delete a statistic by key.
    pub fn delete_statistic_by_key(&self, key: &StatisticKey) -> Result<(), PdsDbError> {
        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM Statistic WHERE Name = ?1 AND IpAddress = ?2 AND UserAgent = ?3",
            rusqlite::params![key.name, key.ip_address, key.user_agent],
        )?;
        Ok(())
    }

    /// Get all statistics.
    pub fn get_all_statistics(&self) -> Result<Vec<Statistic>, PdsDbError> {
        let conn = self.get_connection_read_only()?;
        let mut stmt = conn.prepare(
            "SELECT Name, IpAddress, UserAgent, Value, LastUpdatedDate FROM Statistic",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(Statistic {
                name: row.get(0)?,
                ip_address: row.get(1)?,
                user_agent: row.get(2)?,
                value: row.get(3)?,
                last_updated_date: row.get(4)?,
            })
        })?;

        let mut stats = Vec::new();
        for row in rows {
            stats.push(row?);
        }
        Ok(stats)
    }
}
