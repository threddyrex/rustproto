//! User repository operations module.
//!
//! This module handles all write operations to the user's repository,
//! including creating, updating, and deleting records. It manages:
//!
//! - Record storage in the database
//! - MST tree reconstruction
//! - Commit signing
//! - Firehose event generation
//!
//! This is the Rust equivalent of dnproto's UserRepo class.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Mutex;

use p256::ecdsa::{signature::hazmat::PrehashSigner, Signature, SigningKey};
use sha2::{Digest, Sha256};

use crate::mst::{Mst, MstItem};
use crate::pds::db::{DbRepoCommit, FirehoseEvent, PdsDb, PdsDbError};
use crate::repo::{CidV1, DagCborMajorType, DagCborObject, DagCborType, DagCborValue, RepoMst, MstNodeKey};

/// Global lock for repository write operations.
/// This ensures atomic updates to the repo state.
static REPO_LOCK: Mutex<()> = Mutex::new(());

/// Types of write operations.
pub mod write_type {
    pub const CREATE: &str = "com.atproto.repo.applyWrites#create";
    pub const UPDATE: &str = "com.atproto.repo.applyWrites#update";
    pub const DELETE: &str = "com.atproto.repo.applyWrites#delete";
}

/// Result types for write operations.
pub mod result_type {
    pub const CREATE_RESULT: &str = "com.atproto.repo.applyWrites#createResult";
    pub const UPDATE_RESULT: &str = "com.atproto.repo.applyWrites#updateResult";
    pub const DELETE_RESULT: &str = "com.atproto.repo.applyWrites#deleteResult";
}

/// A single write operation to apply to the repository.
#[derive(Debug, Clone)]
pub struct ApplyWritesOperation {
    /// Operation type (create, update, delete).
    pub op_type: String,
    /// Collection NSID.
    pub collection: String,
    /// Record key.
    pub rkey: String,
    /// Record data (required for create/update, None for delete).
    pub record: Option<DagCborObject>,
}

/// Result of a single write operation.
#[derive(Debug, Clone)]
pub struct ApplyWritesResult {
    /// Result type (createResult, updateResult, deleteResult).
    pub result_type: String,
    /// AT URI of the record.
    pub uri: Option<String>,
    /// CID of the record (None for deletes).
    pub cid: Option<CidV1>,
    /// Validation status.
    pub validation_status: Option<String>,
}

/// Error type for user repo operations.
#[derive(Debug)]
pub enum UserRepoError {
    DatabaseError(PdsDbError),
    InvalidOperation(String),
    SigningError(String),
    IoError(std::io::Error),
}

impl std::fmt::Display for UserRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UserRepoError::DatabaseError(e) => write!(f, "Database error: {}", e),
            UserRepoError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            UserRepoError::SigningError(msg) => write!(f, "Signing error: {}", msg),
            UserRepoError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for UserRepoError {}

impl From<PdsDbError> for UserRepoError {
    fn from(e: PdsDbError) -> Self {
        UserRepoError::DatabaseError(e)
    }
}

impl From<std::io::Error> for UserRepoError {
    fn from(e: std::io::Error) -> Self {
        UserRepoError::IoError(e)
    }
}

/// User repository manager.
///
/// Handles all write operations to the user's AT Protocol repository.
pub struct UserRepo<'a> {
    db: &'a PdsDb,
    user_did: String,
    private_key_multibase: String,
    _public_key_multibase: String,
}

impl<'a> UserRepo<'a> {
    /// Create a new UserRepo instance.
    pub fn new(db: &'a PdsDb) -> Result<Self, UserRepoError> {
        let user_did = db.get_config_property("UserDid")?;
        let private_key_multibase = db.get_config_property("UserPrivateKeyMultibase")?;
        let public_key_multibase = db.get_config_property("UserPublicKeyMultibase")?;

        Ok(Self {
            db,
            user_did,
            private_key_multibase,
            _public_key_multibase: public_key_multibase,
        })
    }

    /// Apply a list of write operations to the repository.
    ///
    /// This is the main entry point for making changes to the repo.
    /// It handles:
    /// 1. Record storage
    /// 2. MST reconstruction
    /// 3. Commit signing
    /// 4. Firehose event generation
    pub fn apply_writes(
        &self,
        writes: Vec<ApplyWritesOperation>,
        _ip_address: &str,
        _user_agent: &str,
    ) -> Result<Vec<ApplyWritesResult>, UserRepoError> {
        // Acquire global lock
        let _lock = REPO_LOCK.lock().unwrap();

        let mut results = Vec::new();

        // Get state before changes (for firehose)
        let before_commit = self.db.get_repo_commit()?;
        let mut firehose_ops: Vec<serde_json::Value> = Vec::new();

        // Process each write operation
        for write in &writes {
            let uri = format!("at://{}/{}/{}", self.user_did, write.collection, write.rkey);
            let full_key = format!("{}/{}", write.collection, write.rkey);

            match write.op_type.as_str() {
                write_type::CREATE | write_type::UPDATE => {
                    let record = write.record.as_ref().ok_or_else(|| {
                        UserRepoError::InvalidOperation(
                            "Record required for create/update operations".to_string(),
                        )
                    })?;

                    // Create a mutable copy and set $type
                    let mut record_with_type = record.clone();
                    self.set_type_field(&mut record_with_type, &write.collection);

                    // Compute CID for the record
                    let record_cid = CidV1::compute_cid_for_dag_cbor(&record_with_type)
                        .map_err(|e| UserRepoError::IoError(e))?;

                    // Delete existing record if updating
                    if write.op_type == write_type::UPDATE {
                        let _ = self.db.delete_repo_record(&write.collection, &write.rkey);
                    }

                    // Store the record
                    let record_bytes = record_with_type.to_bytes()?;
                    self.db.insert_repo_record(
                        &write.collection,
                        &write.rkey,
                        &record_cid.base32,
                        &record_bytes,
                    )?;

                    // Add to results
                    let result_type = if write.op_type == write_type::CREATE {
                        result_type::CREATE_RESULT
                    } else {
                        result_type::UPDATE_RESULT
                    };

                    results.push(ApplyWritesResult {
                        result_type: result_type.to_string(),
                        uri: Some(uri),
                        cid: Some(record_cid.clone()),
                        validation_status: Some("valid".to_string()),
                    });

                    // Add to firehose ops
                    let action = if write.op_type == write_type::CREATE {
                        "create"
                    } else {
                        "update"
                    };
                    firehose_ops.push(serde_json::json!({
                        "cid": record_cid.base32,
                        "path": full_key,
                        "action": action
                    }));
                }

                write_type::DELETE => {
                    // Check if record exists
                    if !self.db.record_exists(&write.collection, &write.rkey)? {
                        // Skip if doesn't exist
                        continue;
                    }

                    // Get original record CID for firehose
                    let original_record = self.db.get_repo_record(&write.collection, &write.rkey)?;
                    let original_cid = original_record.cid.clone();

                    // Delete the record
                    self.db.delete_repo_record(&write.collection, &write.rkey)?;

                    // Add to results
                    results.push(ApplyWritesResult {
                        result_type: result_type::DELETE_RESULT.to_string(),
                        uri: Some(uri),
                        cid: None,
                        validation_status: None,
                    });

                    // Add to firehose ops
                    firehose_ops.push(serde_json::json!({
                        "cid": serde_json::Value::Null,
                        "path": full_key,
                        "prev": original_cid,
                        "action": "delete"
                    }));
                }

                _ => {
                    return Err(UserRepoError::InvalidOperation(format!(
                        "Unknown operation type: {}",
                        write.op_type
                    )));
                }
            }
        }

        if results.is_empty() {
            return Ok(results);
        }

        // Rebuild MST from all records
        let all_records = self.db.get_all_repo_records()?;
        let mst_items: Vec<MstItem> = all_records
            .iter()
            .map(|r| MstItem::new(&format!("{}/{}", r.collection, r.rkey), &r.cid))
            .collect();

        let mst = Mst::assemble_tree_from_items(&mst_items);

        // Convert entire MST to DAG-CBOR with cache
        let mst_cache = RepoMst::convert_mst_to_dag_cbor(&mst)?;

        // Find all nodes that need to be included in the commit
        let mut nodes_to_send: HashMap<MstNodeKey, (&crate::mst::MstNode, CidV1, DagCborObject)> = HashMap::new();
        
        for write in &writes {
            let full_key = format!("{}/{}", write.collection, write.rkey);
            let nodes = mst.find_nodes_for_key(&full_key);
            for node in nodes {
                let key = MstNodeKey::from_node(node);
                if !nodes_to_send.contains_key(&key) {
                    if let Some((cid, dag_cbor)) = mst_cache.get(&key) {
                        nodes_to_send.insert(key, (node, cid.clone(), dag_cbor.clone()));
                    }
                }
            }
        }

        // Get root node CID
        let root_key = MstNodeKey::from_node(&mst.root);
        let root_cid = if let Some((_, cid, _)) = nodes_to_send.get(&root_key) {
            cid.clone()
        } else if let Some((cid, _)) = mst_cache.get(&root_key) {
            cid.clone()
        } else {
            return Err(UserRepoError::InvalidOperation("Root MST node not found".to_string()));
        };

        // Generate new revision TID
        let rev = Self::generate_tid();

        // Create and sign the commit
        let (_signed_commit, commit_cid, signature) = self.create_and_sign_commit(
            &root_cid,
            &rev,
            Some(&before_commit.root_mst_node_cid),
        )?;

        // Update repo commit in database
        let db_commit = DbRepoCommit {
            version: 3,
            cid: commit_cid.base32.clone(),
            root_mst_node_cid: root_cid.base32.clone(),
            rev: rev.clone(),
            prev_mst_node_cid: Some(before_commit.root_mst_node_cid.clone()),
            signature,
        };
        self.db.update_repo_commit(&db_commit)?;

        // Update repo header
        let header = crate::pds::db::DbRepoHeader {
            repo_commit_cid: commit_cid.base32.clone(),
            version: 1,
        };
        self.db.insert_update_repo_header(&header)?;

        // Generate firehose event
        self.generate_firehose_commit_event(
            &before_commit,
            &db_commit,
            &mst,
            &nodes_to_send,
            &writes,
            firehose_ops,
        )?;

        Ok(results)
    }

    /// Set the $type field on a record.
    fn set_type_field(&self, record: &mut DagCborObject, collection: &str) {
        if let DagCborValue::Map(ref mut map) = record.value {
            map.insert(
                "$type".to_string(),
                DagCborObject::new_text(collection.to_string()),
            );
        }
    }

    /// Generate a TID (Timestamp ID) for revisions and record keys.
    pub fn generate_tid() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();

        // TID is microseconds since epoch, encoded in base32-sortable
        let microseconds = now.as_micros() as u64;

        // Convert to base32-sortable (using custom alphabet)
        const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
        let mut result = String::with_capacity(13);
        let mut value = microseconds;

        for _ in 0..13 {
            let idx = (value & 0x1F) as usize;
            result.insert(0, ALPHABET[idx] as char);
            value >>= 5;
        }

        result
    }

    /// Create and sign a new commit.
    fn create_and_sign_commit(
        &self,
        root_cid: &CidV1,
        rev: &str,
        prev_mst_cid: Option<&str>,
    ) -> Result<(DagCborObject, CidV1, Vec<u8>), UserRepoError> {
        // Parse prev CID if provided
        let prev_cid = if let Some(cid_str) = prev_mst_cid {
            CidV1::from_base32(cid_str).ok()
        } else {
            None
        };

        // Create unsigned commit
        let unsigned_commit = self.create_commit_dag_cbor(
            3, // version
            root_cid,
            rev,
            prev_cid.as_ref(),
            None, // no signature yet
        )?;

        // Hash the unsigned commit
        let unsigned_bytes = unsigned_commit.to_bytes()?;
        let mut hasher = Sha256::new();
        hasher.update(&unsigned_bytes);
        let hash: [u8; 32] = hasher.finalize().into();

        // Sign the hash
        let signature = self.sign_commit_hash(&hash)?;

        // Create signed commit
        let signed_commit = self.create_commit_dag_cbor(
            3,
            root_cid,
            rev,
            prev_cid.as_ref(),
            Some(&signature),
        )?;

        // Compute CID of signed commit
        let commit_cid = CidV1::compute_cid_for_dag_cbor(&signed_commit)?;

        Ok((signed_commit, commit_cid, signature))
    }

    /// Create a commit DAG-CBOR object.
    fn create_commit_dag_cbor(
        &self,
        version: i64,
        data_cid: &CidV1,
        rev: &str,
        prev_cid: Option<&CidV1>,
        signature: Option<&[u8]>,
    ) -> Result<DagCborObject, UserRepoError> {
        let mut commit_map: HashMap<String, DagCborObject> = HashMap::new();

        commit_map.insert("did".to_string(), DagCborObject::new_text(self.user_did.clone()));
        commit_map.insert("version".to_string(), DagCborObject::new_unsigned_int(version));
        commit_map.insert("data".to_string(), DagCborObject::new_cid(data_cid.clone()));
        commit_map.insert("rev".to_string(), DagCborObject::new_text(rev.to_string()));

        if let Some(cid) = prev_cid {
            commit_map.insert("prev".to_string(), DagCborObject::new_cid(cid.clone()));
        } else {
            commit_map.insert("prev".to_string(), DagCborObject::new_null());
        }

        if let Some(sig) = signature {
            commit_map.insert("sig".to_string(), DagCborObject::new_byte_string(sig.to_vec()));
        }

        Ok(DagCborObject::new_map(commit_map))
    }

    /// Sign a commit hash using the private key.
    fn sign_commit_hash(&self, hash: &[u8; 32]) -> Result<Vec<u8>, UserRepoError> {
        // Decode the multibase private key (z prefix = base58btc)
        if !self.private_key_multibase.starts_with('z') {
            return Err(UserRepoError::SigningError(
                "Private key must be multibase (base58btc, z prefix)".to_string(),
            ));
        }

        let private_key_with_prefix = bs58::decode(&self.private_key_multibase[1..])
            .into_vec()
            .map_err(|e| UserRepoError::SigningError(format!("Invalid base58: {}", e)))?;

        // Check for P-256 private key prefix (0x86 0x26)
        if private_key_with_prefix.len() < 34 {
            return Err(UserRepoError::SigningError("Private key too short".to_string()));
        }

        if private_key_with_prefix[0] != 0x86 || private_key_with_prefix[1] != 0x26 {
            return Err(UserRepoError::SigningError(format!(
                "Expected P-256 private key prefix (0x86 0x26), got 0x{:02X} 0x{:02X}",
                private_key_with_prefix[0], private_key_with_prefix[1]
            )));
        }

        let private_key_bytes = &private_key_with_prefix[2..];
        if private_key_bytes.len() != 32 {
            return Err(UserRepoError::SigningError(format!(
                "Expected 32-byte private key, got {} bytes",
                private_key_bytes.len()
            )));
        }

        // Create signing key
        let signing_key = SigningKey::from_slice(private_key_bytes)
            .map_err(|e| UserRepoError::SigningError(format!("Invalid P-256 key: {}", e)))?;

        // Sign the hash (prehashed)
        let signature: Signature = signing_key
            .sign_prehash(hash)
            .map_err(|e| UserRepoError::SigningError(format!("Signing failed: {}", e)))?;

        // Get r and s values (IEEE P1363 format: r || s)
        let signature_bytes = signature.to_bytes();

        // Normalize to low-S form
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
        let order: [u8; 32] = [
            0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xBC, 0xE6, 0xFA, 0xAD, 0xA7, 0x17, 0x9E, 0x84,
            0xF3, 0xB9, 0xCA, 0xC2, 0xFC, 0x63, 0x25, 0x51,
        ];

        // half_order = order / 2
        let half_order: [u8; 32] = [
            0x7F, 0xFF, 0xFF, 0xFF, 0x80, 0x00, 0x00, 0x00,
            0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xDE, 0x73, 0x7D, 0x56, 0xD3, 0x8B, 0xCF, 0x42,
            0x79, 0xDC, 0xE5, 0x61, 0x7E, 0x31, 0x92, 0xA8,
        ];

        // Check if s > half_order
        let s_high = Self::compare_bytes(s, &half_order) > 0;

        if s_high {
            // s = order - s
            let new_s = Self::subtract_bytes(&order, s);
            let mut result = Vec::with_capacity(64);
            result.extend_from_slice(r);
            result.extend_from_slice(&new_s);
            result
        } else {
            signature.to_vec()
        }
    }

    fn compare_bytes(a: &[u8], b: &[u8]) -> i32 {
        for (x, y) in a.iter().zip(b.iter()) {
            if x > y {
                return 1;
            }
            if x < y {
                return -1;
            }
        }
        0
    }

    fn subtract_bytes(a: &[u8; 32], b: &[u8]) -> [u8; 32] {
        let mut result = [0u8; 32];
        let mut borrow: i16 = 0;

        for i in (0..32).rev() {
            let diff = (a[i] as i16) - (b[i] as i16) - borrow;
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

    /// Generate a firehose #commit event.
    fn generate_firehose_commit_event(
        &self,
        before_commit: &DbRepoCommit,
        new_commit: &DbRepoCommit,
        _mst: &Mst,
        nodes_to_send: &HashMap<MstNodeKey, (&crate::mst::MstNode, CidV1, DagCborObject)>,
        writes: &[ApplyWritesOperation],
        firehose_ops: Vec<serde_json::Value>,
    ) -> Result<(), UserRepoError> {
        let sequence_number = self.db.get_new_sequence_number_for_firehose()?;
        let created_date = FirehoseEvent::get_new_created_date();

        // Create header DAG-CBOR
        let mut header_map: HashMap<String, DagCborObject> = HashMap::new();
        header_map.insert("t".to_string(), DagCborObject::new_text("#commit".to_string()));
        header_map.insert("op".to_string(), DagCborObject::new_unsigned_int(1));
        let header = DagCborObject::new_map(header_map);

        // Build blocks (CAR-like stream)
        let mut block_stream: Vec<u8> = Vec::new();

        // Write repo header
        let repo_header = self.db.get_repo_header()?;
        self.write_car_header(&mut block_stream, &repo_header)?;

        // Write MST nodes (sorted by depth, root first)
        let mut sorted_nodes: Vec<_> = nodes_to_send.values().collect();
        sorted_nodes.sort_by(|a, b| b.0.key_depth.cmp(&a.0.key_depth));
        
        for (_, cid, dag_cbor) in sorted_nodes {
            self.write_car_block(&mut block_stream, cid, dag_cbor)?;
        }

        // Write records
        for write in writes {
            if self.db.record_exists(&write.collection, &write.rkey)? {
                let record = self.db.get_repo_record(&write.collection, &write.rkey)?;
                let record_dag_cbor = DagCborObject::from_bytes(&record.dag_cbor_bytes)?;
                let record_cid = CidV1::from_base32(&record.cid)
                    .map_err(|e| UserRepoError::InvalidOperation(format!("Invalid CID: {}", e)))?;
                self.write_car_block(&mut block_stream, &record_cid, &record_dag_cbor)?;
            }
        }

        // Write commit
        let commit_cid = CidV1::from_base32(&new_commit.cid)
            .map_err(|e| UserRepoError::InvalidOperation(format!("Invalid commit CID: {}", e)))?;
        let commit_dag_cbor = self.create_commit_dag_cbor(
            new_commit.version as i64,
            &CidV1::from_base32(&new_commit.root_mst_node_cid)
                .map_err(|e| UserRepoError::InvalidOperation(format!("Invalid root CID: {}", e)))?,
            &new_commit.rev,
            new_commit.prev_mst_node_cid.as_ref()
                .and_then(|s| CidV1::from_base32(s).ok())
                .as_ref(),
            Some(&new_commit.signature),
        )?;
        self.write_car_block(&mut block_stream, &commit_cid, &commit_dag_cbor)?;

        // Create body DAG-CBOR
        let ops_array: Vec<DagCborObject> = firehose_ops
            .iter()
            .map(|op| self.json_to_dag_cbor(op))
            .collect();

        let mut body_map: HashMap<String, DagCborObject> = HashMap::new();
        body_map.insert("ops".to_string(), DagCborObject::new_array(ops_array));
        body_map.insert("rev".to_string(), DagCborObject::new_text(new_commit.rev.clone()));
        body_map.insert("seq".to_string(), DagCborObject::new_unsigned_int(sequence_number));
        body_map.insert("repo".to_string(), DagCborObject::new_text(self.user_did.clone()));
        body_map.insert("time".to_string(), DagCborObject::new_text(created_date.clone()));
        body_map.insert("blobs".to_string(), DagCborObject::new_array(vec![]));
        body_map.insert("since".to_string(), DagCborObject::new_text(before_commit.rev.clone()));
        body_map.insert("blocks".to_string(), DagCborObject::new_byte_string(block_stream));
        body_map.insert("commit".to_string(), DagCborObject::new_cid(commit_cid));
        body_map.insert("rebase".to_string(), new_bool_dag_cbor(false));
        body_map.insert("tooBig".to_string(), new_bool_dag_cbor(false));
        
        let prev_data_cid = CidV1::from_base32(&before_commit.root_mst_node_cid)
            .map_err(|e| UserRepoError::InvalidOperation(format!("Invalid prev data CID: {}", e)))?;
        body_map.insert("prevData".to_string(), DagCborObject::new_cid(prev_data_cid));

        let body = DagCborObject::new_map(body_map);

        // Serialize and store
        let header_bytes = header.to_bytes()?;
        let body_bytes = body.to_bytes()?;

        let event = FirehoseEvent {
            sequence_number,
            created_date,
            header_op: 1,
            header_t: Some("#commit".to_string()),
            header_dag_cbor_bytes: header_bytes,
            body_dag_cbor_bytes: body_bytes,
        };

        self.db.insert_firehose_event(&event)?;

        Ok(())
    }

    /// Write a CAR header to a stream.
    fn write_car_header<W: Write>(&self, writer: &mut W, header: &crate::pds::db::DbRepoHeader) -> Result<(), UserRepoError> {
        use crate::repo::VarInt;

        // CAR header format: varint(header_length) | dag-cbor(header)
        let mut header_map: HashMap<String, DagCborObject> = HashMap::new();
        header_map.insert("version".to_string(), DagCborObject::new_unsigned_int(header.version as i64));

        // roots array with the commit CID
        let root_cid = CidV1::from_base32(&header.repo_commit_cid)
            .map_err(|e| UserRepoError::InvalidOperation(format!("Invalid header CID: {}", e)))?;
        header_map.insert("roots".to_string(), DagCborObject::new_array(vec![
            DagCborObject::new_cid(root_cid),
        ]));

        let header_dag_cbor = DagCborObject::new_map(header_map);
        let header_bytes = header_dag_cbor.to_bytes()?;

        // Write length as varint
        let length_varint = VarInt::from_long(header_bytes.len() as i64);
        length_varint.write_varint(writer)?;
        writer.write_all(&header_bytes)?;

        Ok(())
    }

    /// Write a CAR block to a stream.
    fn write_car_block<W: Write>(&self, writer: &mut W, cid: &CidV1, dag_cbor: &DagCborObject) -> Result<(), UserRepoError> {
        use crate::repo::VarInt;

        let data_bytes = dag_cbor.to_bytes()?;
        let cid_bytes = &cid.all_bytes;

        // Block format: varint(cid_length + data_length) | cid | data
        let total_length = cid_bytes.len() + data_bytes.len();
        let length_varint = VarInt::from_long(total_length as i64);

        length_varint.write_varint(writer)?;
        writer.write_all(cid_bytes)?;
        writer.write_all(&data_bytes)?;

        Ok(())
    }

    /// Convert a serde_json::Value to DagCborObject.
    fn json_to_dag_cbor(&self, value: &serde_json::Value) -> DagCborObject {
        match value {
            serde_json::Value::Null => DagCborObject::new_null(),
            serde_json::Value::Bool(b) => new_bool_dag_cbor(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    DagCborObject::new_unsigned_int(i)
                } else if let Some(f) = n.as_f64() {
                    // DAG-CBOR doesn't support floats in the same way, use unsigned int
                    DagCborObject::new_unsigned_int(f as i64)
                } else {
                    DagCborObject::new_unsigned_int(0)
                }
            }
            serde_json::Value::String(s) => DagCborObject::new_text(s.clone()),
            serde_json::Value::Array(arr) => {
                let items: Vec<DagCborObject> = arr.iter().map(|v| self.json_to_dag_cbor(v)).collect();
                DagCborObject::new_array(items)
            }
            serde_json::Value::Object(obj) => {
                let mut map: HashMap<String, DagCborObject> = HashMap::new();
                for (k, v) in obj {
                    map.insert(k.clone(), self.json_to_dag_cbor(v));
                }
                DagCborObject::new_map(map)
            }
        }
    }
}

/// Helper function to create a boolean DagCborObject.
fn new_bool_dag_cbor(value: bool) -> DagCborObject {
    DagCborObject {
        cbor_type: DagCborType {
            major_type: DagCborMajorType::SimpleValue,
            additional_info: if value { 0x15 } else { 0x14 },
            original_byte: 0,
        },
        value: DagCborValue::Bool(value),
    }
}

/// Parse a JSON value into a DagCborObject.
/// This handles the AT Protocol conventions like $link for CIDs.
pub fn parse_json_to_dag_cbor(value: &serde_json::Value) -> Result<DagCborObject, UserRepoError> {
    match value {
        serde_json::Value::Null => Ok(DagCborObject::new_null()),
        serde_json::Value::Bool(b) => Ok(new_bool_dag_cbor(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(DagCborObject::new_unsigned_int(i))
            } else {
                Ok(DagCborObject::new_unsigned_int(0))
            }
        }
        serde_json::Value::String(s) => Ok(DagCborObject::new_text(s.clone())),
        serde_json::Value::Array(arr) => {
            let items: Result<Vec<DagCborObject>, UserRepoError> = arr
                .iter()
                .map(|v| parse_json_to_dag_cbor(v))
                .collect();
            Ok(DagCborObject::new_array(items?))
        }
        serde_json::Value::Object(obj) => {
            // Check if this is a $link (CID reference)
            if let Some(link) = obj.get("$link") {
                if let Some(link_str) = link.as_str() {
                    let cid = CidV1::from_base32(link_str)
                        .map_err(|e| UserRepoError::InvalidOperation(format!("Invalid CID in $link: {}", e)))?;
                    return Ok(DagCborObject::new_cid(cid));
                }
            }

            // Check if this is a $bytes (byte string)
            if let Some(bytes) = obj.get("$bytes") {
                if let Some(bytes_str) = bytes.as_str() {
                    use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
                    let decoded = BASE64.decode(bytes_str)
                        .map_err(|e| UserRepoError::InvalidOperation(format!("Invalid base64 in $bytes: {}", e)))?;
                    return Ok(DagCborObject::new_byte_string(decoded));
                }
            }

            // Regular object
            let mut map: HashMap<String, DagCborObject> = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), parse_json_to_dag_cbor(v)?);
            }
            Ok(DagCborObject::new_map(map))
        }
    }
}
