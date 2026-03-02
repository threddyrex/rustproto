//! Firehose event generator.
//!
//! Generates AT Protocol firehose events (#account, #identity, #sync, #commit)
//! and stores them in the database for streaming to subscribers.

use std::collections::HashMap;
use std::io::Write;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};

use crate::pds::db::{DbRepoCommit, DbRepoHeader, FirehoseEvent, PdsDb};
use crate::repo::{
    CidV1, DagCborMajorType, DagCborObject, DagCborType, DagCborValue, RepoHeader, VarInt,
};

/// Firehose event generator for creating AT Protocol event stream frames.
pub struct FirehoseEventGenerator<'a> {
    db: &'a PdsDb,
}

impl<'a> FirehoseEventGenerator<'a> {
    /// Create a new FirehoseEventGenerator.
    pub fn new(db: &'a PdsDb) -> Self {
        Self { db }
    }

    /// Generate firehose events for account activation.
    /// This creates #account, #identity, and #sync events.
    pub fn generate_activation_events(&self, active: bool) -> Result<(), String> {
        let user_did = self.db.get_config_property("UserDid")
            .map_err(|e| format!("Failed to get UserDid: {}", e))?;
        let user_handle = self.db.get_config_property("UserHandle")
            .map_err(|e| format!("Failed to get UserHandle: {}", e))?;

        // Generate #account event
        self.generate_account_event(&user_did, active, None)?;

        // Generate #identity event
        self.generate_identity_event(&user_did, &user_handle)?;

        // Generate #sync event
        self.generate_sync_event(&user_did)?;

        Ok(())
    }

    /// Generate firehose events for account deactivation.
    /// This creates only #account event with status "deactivated".
    pub fn generate_deactivation_events(&self) -> Result<(), String> {
        let user_did = self.db.get_config_property("UserDid")
            .map_err(|e| format!("Failed to get UserDid: {}", e))?;

        // Generate #account event with status
        self.generate_account_event(&user_did, false, Some("deactivated"))?;

        Ok(())
    }

    /// Generate a #account firehose event.
    pub fn generate_account_event(
        &self,
        did: &str,
        active: bool,
        status: Option<&str>,
    ) -> Result<(), String> {
        let sequence_number = self.db.get_new_sequence_number_for_firehose()
            .map_err(|e| format!("Failed to get sequence number: {}", e))?;
        let created_date = FirehoseEvent::get_new_created_date();

        // Create header: {"t": "#account", "op": 1}
        let mut header_map: HashMap<String, DagCborObject> = HashMap::new();
        header_map.insert("t".to_string(), DagCborObject::new_text("#account".to_string()));
        header_map.insert("op".to_string(), DagCborObject::new_unsigned_int(1));
        let header = DagCborObject::new_map(header_map);

        // Create body: {"did": ..., "active": ..., "seq": ..., "time": ...}
        let mut body_map: HashMap<String, DagCborObject> = HashMap::new();
        body_map.insert("did".to_string(), DagCborObject::new_text(did.to_string()));
        body_map.insert("active".to_string(), new_bool_dag_cbor(active));
        body_map.insert("seq".to_string(), DagCborObject::new_unsigned_int(sequence_number));
        body_map.insert("time".to_string(), DagCborObject::new_text(created_date.clone()));

        if let Some(s) = status {
            body_map.insert("status".to_string(), DagCborObject::new_text(s.to_string()));
        }

        let body = DagCborObject::new_map(body_map);

        let header_bytes = header.to_bytes()
            .map_err(|e| format!("Failed to encode header: {}", e))?;
        let body_bytes = body.to_bytes()
            .map_err(|e| format!("Failed to encode body: {}", e))?;

        let event = FirehoseEvent {
            sequence_number,
            created_date,
            header_op: 1,
            header_t: Some("#account".to_string()),
            header_dag_cbor_bytes: header_bytes,
            body_dag_cbor_bytes: body_bytes,
        };

        self.db.insert_firehose_event(&event)
            .map_err(|e| format!("Failed to insert firehose event: {}", e))?;

        Ok(())
    }

    /// Generate a #identity firehose event.
    pub fn generate_identity_event(&self, did: &str, handle: &str) -> Result<(), String> {
        let sequence_number = self.db.get_new_sequence_number_for_firehose()
            .map_err(|e| format!("Failed to get sequence number: {}", e))?;
        let created_date = FirehoseEvent::get_new_created_date();

        // Create header: {"t": "#identity", "op": 1}
        let mut header_map: HashMap<String, DagCborObject> = HashMap::new();
        header_map.insert("t".to_string(), DagCborObject::new_text("#identity".to_string()));
        header_map.insert("op".to_string(), DagCborObject::new_unsigned_int(1));
        let header = DagCborObject::new_map(header_map);

        // Create body: {"did": ..., "handle": ..., "seq": ..., "time": ...}
        let mut body_map: HashMap<String, DagCborObject> = HashMap::new();
        body_map.insert("did".to_string(), DagCborObject::new_text(did.to_string()));
        body_map.insert("handle".to_string(), DagCborObject::new_text(handle.to_string()));
        body_map.insert("seq".to_string(), DagCborObject::new_unsigned_int(sequence_number));
        body_map.insert("time".to_string(), DagCborObject::new_text(created_date.clone()));

        let body = DagCborObject::new_map(body_map);

        let header_bytes = header.to_bytes()
            .map_err(|e| format!("Failed to encode header: {}", e))?;
        let body_bytes = body.to_bytes()
            .map_err(|e| format!("Failed to encode body: {}", e))?;

        let event = FirehoseEvent {
            sequence_number,
            created_date,
            header_op: 1,
            header_t: Some("#identity".to_string()),
            header_dag_cbor_bytes: header_bytes,
            body_dag_cbor_bytes: body_bytes,
        };

        self.db.insert_firehose_event(&event)
            .map_err(|e| format!("Failed to insert firehose event: {}", e))?;

        Ok(())
    }

    /// Generate a #sync firehose event with repo blocks.
    pub fn generate_sync_event(&self, did: &str) -> Result<(), String> {
        let sequence_number = self.db.get_new_sequence_number_for_firehose()
            .map_err(|e| format!("Failed to get sequence number: {}", e))?;
        let created_date = FirehoseEvent::get_new_created_date();

        // Get repo commit and header
        let repo_commit = self.db.get_repo_commit()
            .map_err(|e| format!("Failed to get repo commit: {}", e))?;
        let repo_header = self.db.get_repo_header()
            .map_err(|e| format!("Failed to get repo header: {}", e))?;

        // Create header: {"t": "#sync", "op": 1}
        let mut header_map: HashMap<String, DagCborObject> = HashMap::new();
        header_map.insert("t".to_string(), DagCborObject::new_text("#sync".to_string()));
        header_map.insert("op".to_string(), DagCborObject::new_unsigned_int(1));
        let header = DagCborObject::new_map(header_map);

        // Build the blocks (repo header + commit DAG-CBOR)
        let blocks = build_sync_blocks(&repo_header, &repo_commit)?;

        // Create body: {"did": ..., "rev": ..., "seq": ..., "time": ..., "blocks": ...}
        let mut body_map: HashMap<String, DagCborObject> = HashMap::new();
        body_map.insert("did".to_string(), DagCborObject::new_text(did.to_string()));
        body_map.insert("rev".to_string(), DagCborObject::new_text(repo_commit.rev.clone()));
        body_map.insert("seq".to_string(), DagCborObject::new_unsigned_int(sequence_number));
        body_map.insert("time".to_string(), DagCborObject::new_text(created_date.clone()));
        body_map.insert("blocks".to_string(), DagCborObject::new_byte_string(blocks));

        let body = DagCborObject::new_map(body_map);

        let header_bytes = header.to_bytes()
            .map_err(|e| format!("Failed to encode header: {}", e))?;
        let body_bytes = body.to_bytes()
            .map_err(|e| format!("Failed to encode body: {}", e))?;

        let event = FirehoseEvent {
            sequence_number,
            created_date,
            header_op: 1,
            header_t: Some("#sync".to_string()),
            header_dag_cbor_bytes: header_bytes,
            body_dag_cbor_bytes: body_bytes,
        };

        self.db.insert_firehose_event(&event)
            .map_err(|e| format!("Failed to insert firehose event: {}", e))?;

        Ok(())
    }
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Creates a new DagCborObject containing a boolean.
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

/// Build the blocks byte array for a #sync event.
fn build_sync_blocks(repo_header: &DbRepoHeader, repo_commit: &DbRepoCommit) -> Result<Vec<u8>, String> {
    let mut blocks = Vec::new();

    // Parse repo commit CID
    let repo_commit_cid = CidV1::from_base32(&repo_header.repo_commit_cid)
        .map_err(|e| format!("Invalid CID: {}", e))?;

    // Write repo header (CAR format header)
    let header = RepoHeader {
        version: repo_header.version as i64,
        repo_commit_cid: repo_commit_cid.clone(),
    };
    header.write_to_stream(&mut blocks)
        .map_err(|e| format!("Failed to write header: {}", e))?;

    // Build commit DAG-CBOR
    let root_cid = CidV1::from_base32(&repo_commit.root_mst_node_cid)
        .map_err(|e| format!("Invalid root CID: {}", e))?;
    let commit_cid = CidV1::from_base32(&repo_commit.cid)
        .map_err(|e| format!("Invalid commit CID: {}", e))?;

    // Decode signature from base64
    let signature_bytes = BASE64.decode(&repo_commit.signature)
        .map_err(|e| format!("Invalid signature base64: {}", e))?;

    // Create commit object (version, data, rev, prev, sig)
    let mut commit_map: HashMap<String, DagCborObject> = HashMap::new();
    commit_map.insert("version".to_string(), DagCborObject::new_unsigned_int(repo_commit.version as i64));
    commit_map.insert("data".to_string(), DagCborObject::new_cid(root_cid));
    commit_map.insert("rev".to_string(), DagCborObject::new_text(repo_commit.rev.clone()));

    if let Some(prev) = &repo_commit.prev_mst_node_cid {
        let prev_cid = CidV1::from_base32(prev)
            .map_err(|e| format!("Invalid prev CID: {}", e))?;
        commit_map.insert("prev".to_string(), DagCborObject::new_cid(prev_cid));
    } else {
        commit_map.insert("prev".to_string(), DagCborObject::new_null());
    }

    commit_map.insert("sig".to_string(), DagCborObject::new_byte_string(signature_bytes));

    let commit_obj = DagCborObject::new_map(commit_map);

    // Write commit block to stream (CAR block format: varint_length | CID | dag_cbor_bytes)
    let mut cid_bytes = Vec::new();
    commit_cid.write_cid(&mut cid_bytes)
        .map_err(|e| format!("Failed to write CID: {}", e))?;

    let commit_bytes = commit_obj.to_bytes()
        .map_err(|e| format!("Failed to serialize commit: {}", e))?;

    let block_length = cid_bytes.len() + commit_bytes.len();
    let block_length_varint = VarInt::from_long(block_length as i64);

    block_length_varint.write_varint(&mut blocks)
        .map_err(|e| format!("Failed to write block length: {}", e))?;
    blocks.write_all(&cid_bytes)
        .map_err(|e| format!("Failed to write CID bytes: {}", e))?;
    blocks.write_all(&commit_bytes)
        .map_err(|e| format!("Failed to write commit bytes: {}", e))?;

    Ok(blocks)
}
