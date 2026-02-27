//! PDS Installer module.
//!
//! This module provides installation functions for the PDS,
//! including database schema creation and initial configuration.

use std::fs;

use thiserror::Error;

use super::db::{PdsDb, PdsDbError, SqliteDb};
use crate::fs::LocalFileSystem;
use crate::log::Logger;

/// Errors that can occur during PDS installation.
#[derive(Error, Debug)]
pub enum InstallerError {
    #[error("Database directory does not exist: {0}")]
    DbDirNotFound(String),

    #[error("Database error: {0}")]
    DbError(#[from] PdsDbError),

    #[error("SQLite error: {0}")]
    SqliteError(#[from] rusqlite::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// PDS Installer - handles database and configuration installation.
///
/// Available methods (run in order):
///
/// 1. `install_db` - Creates database schema
/// 2. `install_config` - Sets up server configuration (scheme, host, port, etc.)
/// 3. `install_repo` - Creates fresh repo for user (not yet implemented)
///
/// Run the methods in order.
pub struct Installer;

impl Installer {
    // =========================================================================
    // DATABASE INSTALLATION
    // =========================================================================

    /// Install the database schema.
    ///
    /// Creates all necessary tables in the PDS database. If `delete_existing_db`
    /// is true, will delete any existing database file before installing.
    /// This method is re-runnable for schema updates (uses CREATE TABLE IF NOT EXISTS).
    ///
    /// # Arguments
    ///
    /// * `lfs` - LocalFileSystem instance
    /// * `log` - Logger instance
    /// * `delete_existing_db` - If true, delete existing database before creating
    ///
    /// # Returns
    ///
    /// Ok(()) on success, or an error if installation fails.
    pub fn install_db(
        lfs: &LocalFileSystem,
        log: &Logger,
        delete_existing_db: bool,
    ) -> Result<(), InstallerError> {
        // Paths
        let db_dir = lfs.get_data_dir().join("pds");
        let db_file_path = lfs.get_path_pds_db();

        // Check that the pds folder exists
        if !db_dir.exists() {
            log.error(&format!(
                "PDS database directory does not exist: {}",
                db_dir.display()
            ));
            return Err(InstallerError::DbDirNotFound(
                db_dir.to_string_lossy().to_string(),
            ));
        }

        // Check if they want to delete existing
        let db_exists = db_file_path.exists();
        if db_exists && delete_existing_db {
            log.info("Deleting existing PDS database file.");
            fs::remove_file(&db_file_path)?;
        } else if db_exists {
            log.info("PDS database file already exists. Will NOT delete.");
        }

        // Run create table commands
        log.info("Creating PDS database tables (if not exist).");
        let conn = SqliteDb::get_connection_create(&db_file_path)?;

        PdsDb::create_table_blob(&conn, log)?;
        PdsDb::create_table_preferences(&conn, log)?;
        PdsDb::create_table_repo_header(&conn, log)?;
        PdsDb::create_table_repo_commit(&conn, log)?;
        PdsDb::create_table_repo_record(&conn, log)?;
        PdsDb::create_table_sequence_number(&conn, log)?;
        PdsDb::create_table_firehose_event(&conn, log)?;
        PdsDb::create_table_log_level(&conn, log)?;
        PdsDb::create_table_oauth_request(&conn, log)?;
        PdsDb::create_table_oauth_session(&conn, log)?;
        PdsDb::create_table_legacy_session(&conn, log)?;
        PdsDb::create_table_admin_session(&conn, log)?;
        PdsDb::create_table_passkey(&conn, log)?;
        PdsDb::create_table_passkey_challenge(&conn, log)?;
        PdsDb::create_table_statistic(&conn, log)?;
        PdsDb::create_table_config_property(&conn, log)?;

        log.info("Database installation complete.");
        Ok(())
    }

    // =========================================================================
    // CONFIGURATION INSTALLATION
    // =========================================================================

    /// Install the server configuration.
    ///
    /// Sets up initial configuration properties including:
    /// - Admin password (generated)
    /// - JWT secret (generated)
    /// - Server listen settings (scheme, host, port)
    /// - Feature flags (defaults)
    ///
    /// # Arguments
    ///
    /// * `lfs` - LocalFileSystem instance
    /// * `log` - Logger instance
    /// * `listen_scheme` - Server scheme (http/https)
    /// * `listen_host` - Server hostname
    /// * `listen_port` - Server port number
    ///
    /// # Returns
    ///
    /// Ok(()) on success, or an error if configuration fails.
    pub fn install_config(
        lfs: &LocalFileSystem,
        log: &Logger,
        listen_scheme: &str,
        listen_host: &str,
        listen_port: i32,
    ) -> Result<(), InstallerError> {
        let db = PdsDb::connect(lfs)?;

        // Admin password
        let admin_password = Self::generate_random_password(16);
        let hashed_password = Self::hash_password(&admin_password);
        db.set_config_property("AdminHashedPassword", &hashed_password)?;
        log.info("admin username: admin");
        log.info(&format!("admin password: {}", admin_password));

        // JWT secret
        let jwt_secret = Self::generate_jwt_secret();
        db.set_config_property("JwtSecret", &jwt_secret)?;
        log.info(&format!("JwtSecret: {}", jwt_secret));

        // Server listen config
        db.set_config_property("ServerListenScheme", listen_scheme)?;
        db.set_config_property("ServerListenHost", listen_host)?;
        db.set_config_property_int("ServerListenPort", listen_port)?;

        // Feature flags
        db.set_config_property_bool("FeatureEnabled_AdminDashboard", true)?;
        db.set_config_property_bool("FeatureEnabled_Oauth", false)?;
        db.set_config_property_bool("FeatureEnabled_RequestCrawl", false)?;
        db.set_config_property_bool("FeatureEnabled_Passkeys", false)?;
        db.set_config_property_int("LogRetentionDays", 10)?;
        db.set_config_property("PdsCrawlers", "bsky.network")?;

        // Security
        db.set_config_property(
            "AtprotoProxyAllowedDids",
            "did:web:api.bsky.app,did:web:api.bsky.chat",
        )?;

        log.info("Configuration installation complete.");
        Ok(())
    }

    // =========================================================================
    // HELPER FUNCTIONS
    // =========================================================================

    /// Generate a random password with the given length.
    fn generate_random_password(length: usize) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        // Simple pseudo-random generator using system time
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let mut result = String::with_capacity(length);
        let mut state = seed;

        for _ in 0..length {
            state = state.wrapping_mul(1103515245).wrapping_add(12345);
            let idx = (state as usize) % CHARSET.len();
            result.push(CHARSET[idx] as char);
        }

        result
    }

    /// Generate a JWT secret (256-bit random base64 string).
    fn generate_jwt_secret() -> String {
        use sha2::{Digest, Sha256};
        use std::time::{SystemTime, UNIX_EPOCH};

        // Generate multiple time-based seeds and hash them
        let seed1 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        // Add some variation
        std::thread::sleep(std::time::Duration::from_nanos(1));

        let seed2 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let combined = format!("{}-{}-jwt-secret", seed1, seed2);

        let mut hasher = Sha256::new();
        hasher.update(combined.as_bytes());
        let result = hasher.finalize();

        Self::base64_encode(&result)
    }

    /// Simple base64 encoding.
    fn base64_encode(data: &[u8]) -> String {
        const ALPHABET: &[u8] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        let mut result = String::new();
        let mut i = 0;

        while i < data.len() {
            let b0 = data[i] as usize;
            let b1 = if i + 1 < data.len() {
                data[i + 1] as usize
            } else {
                0
            };
            let b2 = if i + 2 < data.len() {
                data[i + 2] as usize
            } else {
                0
            };

            result.push(ALPHABET[b0 >> 2] as char);
            result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

            if i + 1 < data.len() {
                result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
            } else {
                result.push('=');
            }

            if i + 2 < data.len() {
                result.push(ALPHABET[b2 & 0x3f] as char);
            } else {
                result.push('=');
            }

            i += 3;
        }

        result
    }

    /// Hash a password using PBKDF2-SHA256 (matches dnproto's PasswordHasher).
    /// Returns base64-encoded salt+hash.
    fn hash_password(password: &str) -> String {
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
        use pbkdf2::pbkdf2_hmac;
        use rand::RngCore;
        use sha2::Sha256;

        const SALT_SIZE: usize = 16;
        const HASH_SIZE: usize = 32;
        const ITERATIONS: u32 = 100_000;

        // Generate random salt
        let mut salt = [0u8; SALT_SIZE];
        rand::thread_rng().fill_bytes(&mut salt);

        // Compute PBKDF2 hash
        let mut hash = [0u8; HASH_SIZE];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, ITERATIONS, &mut hash);

        // Combine salt + hash and encode as base64
        let mut combined = Vec::with_capacity(SALT_SIZE + HASH_SIZE);
        combined.extend_from_slice(&salt);
        combined.extend_from_slice(&hash);

        BASE64.encode(&combined)
    }
}
