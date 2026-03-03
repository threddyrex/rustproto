//! PDS Installer module.
//!
//! This module provides installation functions for the PDS,
//! including database schema creation and initial configuration.

use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use p256::ecdsa::{signature::hazmat::PrehashSigner, Signature, SigningKey};
use rand::Rng;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::db::{DbRepoCommit, DbRepoHeader, PdsDb, PdsDbError, SqliteDb};
use super::user_repo::{ApplyWritesOperation, UserRepo, parse_json_to_dag_cbor, write_type};
use crate::fs::LocalFileSystem;
use crate::log::Logger;
use crate::mst::{Mst, MstNode};
use crate::repo::{CidV1, DagCborObject, RepoMst};

/// Last timestamp for monotonic TID generation (module level static)
static LAST_TIMESTAMP: AtomicI64 = AtomicI64::new(0);

/// TID alphabet (base32-sortable)
const TID_ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";

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

    #[error("Signing error: {0}")]
    SigningError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// PDS Installer - handles database and configuration installation.
///
/// Available methods (run in order):
///
/// 1. `install_db` - Creates database schema
/// 2. `install_config` - Sets up server configuration (scheme, host, port, etc.)
/// 3. `install_repo` - Creates fresh repo for user
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
    // REPOSITORY INSTALLATION
    // =========================================================================

    /// Install a fresh user repository.
    ///
    /// Creates an empty MST, a signed repo commit, repo header, and initial preferences.
    /// This will delete any existing repo data.
    ///
    /// # Arguments
    ///
    /// * `lfs` - LocalFileSystem instance
    /// * `log` - Logger instance
    /// * `private_key_multibase` - User's private key in multibase format
    /// * `public_key_multibase` - User's public key in multibase format
    ///
    /// # Returns
    ///
    /// Ok(()) on success, or an error if installation fails.
    pub fn install_repo(
        lfs: &LocalFileSystem,
        log: &Logger,
        private_key_multibase: &str,
        public_key_multibase: &str,
    ) -> Result<(), InstallerError> {
        let db = PdsDb::connect(lfs)?;

        // Get user DID from config
        let user_did = db
            .get_config_property("UserDid")
            .map_err(|e| InstallerError::ConfigError(format!("UserDid not configured: {}", e)))?;

        log.info("Deleting existing repo data (if any).");

        // Delete existing repo data
        db.delete_repo_commit().ok();
        db.delete_all_repo_records().ok();
        db.delete_repo_header().ok();
        db.delete_all_firehose_events().ok();
        db.delete_preferences().ok();

        // Increment sequence number for firehose
        let _ = db.get_new_sequence_number_for_firehose();

        // Create empty MST
        log.info("Creating empty MST.");
        let empty_root = MstNode::new(0);
        let empty_mst = Mst { root: empty_root };

        // Convert MST to DAG-CBOR and get root CID
        let mst_cache = RepoMst::convert_mst_to_dag_cbor(&empty_mst)
            .map_err(|e| InstallerError::IoError(e))?;

        // Get root node CID
        let root_key = crate::repo::MstNodeKey::from_node(&empty_mst.root);
        let (root_cid, _root_dag_cbor) = mst_cache
            .get(&root_key)
            .ok_or_else(|| InstallerError::SigningError("Root MST node not in cache".to_string()))?;

        // Generate revision TID
        let rev = Self::generate_tid();

        // Create unsigned commit DAG-CBOR
        log.info("Creating and signing repo commit.");
        let unsigned_commit = Self::create_commit_dag_cbor(
            &user_did,
            3, // version
            root_cid,
            &rev,
            None, // no prev
            None, // no signature yet
        )?;

        // Hash the unsigned commit
        let unsigned_bytes = unsigned_commit.to_bytes()
            .map_err(|e| InstallerError::IoError(e))?;
        let mut hasher = Sha256::new();
        hasher.update(&unsigned_bytes);
        let hash: [u8; 32] = hasher.finalize().into();

        // Sign the hash
        let signature = Self::sign_commit_hash(
            &hash,
            private_key_multibase,
            public_key_multibase,
        )?;

        // Create signed commit DAG-CBOR
        let signed_commit = Self::create_commit_dag_cbor(
            &user_did,
            3, // version
            root_cid,
            &rev,
            None, // no prev
            Some(&signature),
        )?;

        // Compute CID of signed commit
        let commit_cid = CidV1::compute_cid_for_dag_cbor(&signed_commit)
            .map_err(|e| InstallerError::IoError(e))?;

        // Create repo header
        let repo_header = DbRepoHeader {
            repo_commit_cid: commit_cid.base32.clone(),
            version: 1,
        };

        // Create repo commit
        let db_repo_commit = DbRepoCommit {
            version: 3,
            cid: commit_cid.base32.clone(),
            root_mst_node_cid: root_cid.base32.clone(),
            rev: rev.clone(),
            prev_mst_node_cid: None,
            signature: signature.clone(),
        };

        // Create preferences JSON
        let prefs_tid = Self::generate_tid();
        let prefs_json = format!(
            r#"{{"preferences":[{{"$type":"app.bsky.actor.defs#savedFeedsPrefV2","items":[{{"id":"{}","type":"timeline","value":"following","pinned":true}}]}},{{"$type":"app.bsky.actor.defs#personalDetailsPref","birthDate":"1991-06-03T00:00:00.000Z"}}]}}"
"#,
            prefs_tid
        );

        // Insert everything into the database
        log.info("Inserting initial repo data into database.");
        db.insert_update_repo_commit(&db_repo_commit)?;
        db.insert_update_repo_header(&repo_header)?;
        db.insert_preferences(&prefs_json)?;

        // Add a Bluesky profile record
        log.info("Creating initial Bluesky profile record in the repo.");
        let user_handle = db.get_config_property("UserHandle")
            .unwrap_or_else(|_| "User".to_string());
        let profile_json = serde_json::json!({
            "displayName": user_handle,
            "description": "This is my Bluesky profile."
        });
        let profile_record = parse_json_to_dag_cbor(&profile_json)
            .map_err(|e| InstallerError::ConfigError(format!("Failed to create profile record: {}", e)))?;

        let user_repo = UserRepo::new(&db)
            .map_err(|e| InstallerError::ConfigError(format!("Failed to connect user repo: {}", e)))?;

        let operation = ApplyWritesOperation {
            op_type: write_type::CREATE.to_string(),
            collection: "app.bsky.actor.profile".to_string(),
            rkey: "self".to_string(),
            record: Some(profile_record),
        };

        user_repo.apply_writes(vec![operation], "127.0.0.1", "installer")
            .map_err(|e| InstallerError::ConfigError(format!("Failed to create profile record: {}", e)))?;

        log.info("Repository installation complete.");
        Ok(())
    }

    /// Create a repo commit DAG-CBOR object.
    fn create_commit_dag_cbor(
        did: &str,
        version: i64,
        data_cid: &CidV1,
        rev: &str,
        prev_cid: Option<&CidV1>,
        signature: Option<&[u8]>,
    ) -> Result<DagCborObject, InstallerError> {
        let mut commit_map: HashMap<String, DagCborObject> = HashMap::new();

        // "did" - user DID
        commit_map.insert("did".to_string(), DagCborObject::new_text(did.to_string()));

        // "version" - commit version
        commit_map.insert("version".to_string(), DagCborObject::new_unsigned_int(version));

        // "data" - root MST node CID
        commit_map.insert("data".to_string(), DagCborObject::new_cid(data_cid.clone()));

        // "rev" - revision string (TID)
        commit_map.insert("rev".to_string(), DagCborObject::new_text(rev.to_string()));

        // "prev" - previous commit CID or null
        if let Some(cid) = prev_cid {
            commit_map.insert("prev".to_string(), DagCborObject::new_cid(cid.clone()));
        } else {
            commit_map.insert("prev".to_string(), DagCborObject::new_null());
        }

        // "sig" - signature (only if provided)
        if let Some(sig) = signature {
            commit_map.insert("sig".to_string(), DagCborObject::new_byte_string(sig.to_vec()));
        }

        Ok(DagCborObject::new_map(commit_map))
    }

    /// Sign a commit hash using the private key.
    fn sign_commit_hash(
        hash: &[u8; 32],
        private_key_multibase: &str,
        _public_key_multibase: &str,
    ) -> Result<Vec<u8>, InstallerError> {
        // Decode the multibase private key (z prefix = base58btc)
        if !private_key_multibase.starts_with('z') {
            return Err(InstallerError::SigningError(
                "Private key must be multibase (base58btc, z prefix)".to_string(),
            ));
        }

        let private_key_with_prefix = bs58::decode(&private_key_multibase[1..])
            .into_vec()
            .map_err(|e| InstallerError::SigningError(format!("Invalid base58: {}", e)))?;

        // Check for P-256 private key prefix (0x86 0x26)
        if private_key_with_prefix.len() < 34 {
            return Err(InstallerError::SigningError(
                "Private key too short".to_string(),
            ));
        }

        if private_key_with_prefix[0] != 0x86 || private_key_with_prefix[1] != 0x26 {
            return Err(InstallerError::SigningError(format!(
                "Expected P-256 private key prefix (0x86 0x26), got 0x{:02X} 0x{:02X}",
                private_key_with_prefix[0], private_key_with_prefix[1]
            )));
        }

        let private_key_bytes = &private_key_with_prefix[2..];
        if private_key_bytes.len() != 32 {
            return Err(InstallerError::SigningError(format!(
                "Expected 32-byte private key, got {} bytes",
                private_key_bytes.len()
            )));
        }

        // Create signing key
        let signing_key = SigningKey::from_slice(private_key_bytes)
            .map_err(|e| InstallerError::SigningError(format!("Invalid P-256 key: {}", e)))?;

        // Sign the hash (prehashed)
        let signature: Signature = signing_key
            .sign_prehash(hash)
            .map_err(|e| InstallerError::SigningError(format!("Signing failed: {}", e)))?;

        // Get r and s values (IEEE P1363 format: r || s)
        let signature_bytes = signature.to_bytes();

        // Normalize to low-S form (required by AT Protocol)
        let normalized = Self::normalize_low_s(&signature_bytes);

        Ok(normalized)
    }

    /// Normalize ECDSA signature to low-S form (BIP-62 compliance).
    fn normalize_low_s(signature: &[u8]) -> Vec<u8> {
        if signature.len() != 64 {
            return signature.to_vec();
        }

        let r = &signature[0..32];
        let s = &signature[32..64];

        // P-256 curve order
        // n = FFFFFFFF 00000000 FFFFFFFF FFFFFFFF BCE6FAAD A7179E84 F3B9CAC2 FC632551
        let order: [u8; 32] = [
            0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xBC, 0xE6, 0xFA, 0xAD, 0xA7, 0x17, 0x9E, 0x84,
            0xF3, 0xB9, 0xCA, 0xC2, 0xFC, 0x63, 0x25, 0x51,
        ];

        // half_order = order / 2
        let half_order = Self::div_by_2(&order);

        // Check if s > half_order (need to normalize)
        if Self::compare_be(s, &half_order) > 0 {
            // s = order - s
            let normalized_s = Self::subtract_be(&order, s);
            let mut result = Vec::with_capacity(64);
            result.extend_from_slice(r);
            result.extend_from_slice(&normalized_s);
            result
        } else {
            signature.to_vec()
        }
    }

    /// Compare two big-endian byte arrays.
    fn compare_be(a: &[u8], b: &[u8]) -> i32 {
        for i in 0..a.len().min(b.len()) {
            if a[i] > b[i] {
                return 1;
            }
            if a[i] < b[i] {
                return -1;
            }
        }
        0
    }

    /// Divide a big-endian number by 2.
    fn div_by_2(n: &[u8]) -> Vec<u8> {
        let mut result = vec![0u8; n.len()];
        let mut carry = 0u8;
        for i in 0..n.len() {
            let val = (carry << 7) | (n[i] >> 1);
            carry = n[i] & 1;
            result[i] = val;
        }
        result
    }

    /// Subtract two big-endian numbers (a - b), assuming a >= b.
    fn subtract_be(a: &[u8], b: &[u8]) -> Vec<u8> {
        let mut result = vec![0u8; a.len()];
        let mut borrow = 0i16;

        for i in (0..a.len()).rev() {
            let diff = a[i] as i16 - b[i] as i16 - borrow;
            if diff < 0 {
                result[i] = (diff + 256) as u8;
                borrow = 1;
            } else {
                result[i] = diff as u8;
                borrow = 0;
            }
        }

        result
    }

    // =========================================================================
    // TID GENERATION
    // =========================================================================

    /// Generate a new TID (Timestamp Identifier).
    ///
    /// TIDs are 64-bit integers encoded as 13-character base32-sortable strings.
    /// Layout:
    /// - Top 1 bit: always 0
    /// - Next 53 bits: microseconds since UNIX epoch
    /// - Final 10 bits: random clock identifier
    pub fn generate_tid() -> String {
        // Get clock identifier (random 10-bit value)
        let clock_id: u16 = rand::thread_rng().gen_range(0..1024);

        // Get current timestamp in microseconds
        let mut ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        // Ensure monotonic increase
        let last = LAST_TIMESTAMP.load(Ordering::SeqCst);
        if ts <= last {
            ts = last + 1;
        }
        LAST_TIMESTAMP.store(ts, Ordering::SeqCst);

        // Mask to 53 bits
        ts &= 0x1FFFFFFFFFFFFF;

        // Build 64-bit TID value: (timestamp << 10) | clock_id
        let tid_value = (ts << 10) | (clock_id as i64);

        // Encode as base32-sortable (13 characters)
        Self::encode_base32_sortable(tid_value)
    }

    /// Encode a 64-bit integer as a 13-character base32-sortable string.
    fn encode_base32_sortable(mut value: i64) -> String {
        let mut chars = [0u8; 13];

        // Process from right to left (least significant to most significant)
        for i in (0..13).rev() {
            let idx = (value & 0x1F) as usize;
            chars[i] = TID_ALPHABET[idx];
            value >>= 5;
        }

        String::from_utf8(chars.to_vec()).unwrap()
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
