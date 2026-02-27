//! PDS Database operations.
//!
//! This module provides the main database operations for the PDS,
//! including table creation, configuration storage, and data access.

use std::path::PathBuf;

use rusqlite::Connection;
use thiserror::Error;

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

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

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

        // Index for authentication lookups by DPoP thumbprint
        conn.execute(
            "CREATE INDEX IF NOT EXISTS IX_OauthSession_DpopJwkThumbprint 
             ON OauthSession(DpopJwkThumbprint)",
            [],
        )?;

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
}
