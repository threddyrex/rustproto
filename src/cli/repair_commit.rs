

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;


pub fn cmd_repair_commit(args: &HashMap<String, String>) {
    use crate::mst::{Mst, MstItem};
    use crate::pds::db::{DbRepoCommit, PdsDb};
    use crate::repo::{CidV1, DagCborObject, RepoMst, MstNodeKey};
    use sha2::{Digest, Sha256};

    let log = logger();

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command RepairCommit /dataDir <path>");
            return;
        }
    };

    let lfs = match LocalFileSystem::initialize(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };

    let db = match PdsDb::connect(&lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to connect to database: {}", e));
            return;
        }
    };

    // Get existing commit
    let old_commit = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit: {}", e));
            return;
        }
    };

    log.info(&format!("Current commit CID: {}", old_commit.cid));
    log.info(&format!("Current root MST CID: {}", old_commit.root_mst_node_cid));
    log.info(&format!("Current rev: {}", old_commit.rev));

    // Get all repo records and rebuild MST
    let all_records = match db.get_all_repo_records() {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("Failed to get repo records: {}", e));
            return;
        }
    };

    log.info(&format!("Found {} repo records", all_records.len()));

    let mst_items: Vec<MstItem> = all_records
        .iter()
        .map(|r| MstItem::new(&format!("{}/{}", r.collection, r.rkey), &r.cid))
        .collect();

    let mst = Mst::assemble_tree_from_items(&mst_items);

    // Convert entire MST to DAG-CBOR
    let mst_cache = match RepoMst::convert_mst_to_dag_cbor(&mst) {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to convert MST to DAG-CBOR: {}", e));
            return;
        }
    };

    // Get root node CID
    let root_key = MstNodeKey::from_node(&mst.root);
    let root_cid = match mst_cache.get(&root_key) {
        Some((cid, _)) => cid.clone(),
        None => {
            log.error("Root MST node not found in cache");
            return;
        }
    };

    log.info(&format!("Computed new root MST CID: {}", root_cid.base32));

    if root_cid.base32 == old_commit.root_mst_node_cid {
        log.info("Root MST CID matches existing commit - no repair needed.");
        return;
    }

    log.info("Root MST CID differs - re-signing commit...");

    // Get signing keys
    let private_key_multibase = match db.get_config_property("UserPrivateKeyMultibase") {
        Ok(k) => k,
        Err(e) => {
            log.error(&format!("Failed to get private key: {}", e));
            return;
        }
    };

    let user_did = match db.get_config_property("UserDid") {
        Ok(d) => d,
        Err(e) => {
            log.error(&format!("Failed to get user DID: {}", e));
            return;
        }
    };

    // Create unsigned commit
    let mut commit_map: std::collections::HashMap<String, DagCborObject> = std::collections::HashMap::new();
    commit_map.insert("did".to_string(), DagCborObject::new_text(user_did.clone()));
    commit_map.insert("version".to_string(), DagCborObject::new_unsigned_int(3));
    commit_map.insert("data".to_string(), DagCborObject::new_cid(root_cid.clone()));
    commit_map.insert("rev".to_string(), DagCborObject::new_text(old_commit.rev.clone()));
    commit_map.insert("prev".to_string(), DagCborObject::new_null());

    let unsigned_commit = DagCborObject::new_map(commit_map.clone());

    // Hash the unsigned commit
    let unsigned_bytes = match unsigned_commit.to_bytes() {
        Ok(b) => b,
        Err(e) => {
            log.error(&format!("Failed to serialize unsigned commit: {}", e));
            return;
        }
    };

    let mut hasher = Sha256::new();
    hasher.update(&unsigned_bytes);
    let hash: [u8; 32] = hasher.finalize().into();

    // Sign the hash
    let signature = match sign_commit_hash(&hash, &private_key_multibase) {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to sign commit: {}", e));
            return;
        }
    };

    // Create signed commit
    commit_map.insert("sig".to_string(), DagCborObject::new_byte_string(signature.clone()));
    let signed_commit = DagCborObject::new_map(commit_map);

    // Compute CID of signed commit
    let commit_cid = match CidV1::compute_cid_for_dag_cbor(&signed_commit) {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to compute commit CID: {}", e));
            return;
        }
    };

    log.info(&format!("New commit CID: {}", commit_cid.base32));

    // Update database
    let new_commit = DbRepoCommit {
        version: 3,
        cid: commit_cid.base32.clone(),
        root_mst_node_cid: root_cid.base32.clone(),
        rev: old_commit.rev.clone(),
        prev_mst_node_cid: None,
        signature: signature.clone(),
    };

    if let Err(e) = db.update_repo_commit(&new_commit) {
        log.error(&format!("Failed to update repo commit: {}", e));
        return;
    }

    // Update repo header
    let header = crate::pds::db::DbRepoHeader {
        repo_commit_cid: commit_cid.base32.clone(),
        version: 1,
    };

    if let Err(e) = db.insert_update_repo_header(&header) {
        log.error(&format!("Failed to update repo header: {}", e));
        return;
    }

    log.info("Commit repaired successfully!");
    log.info(&format!("Old root MST CID: {}", old_commit.root_mst_node_cid));
    log.info(&format!("New root MST CID: {}", root_cid.base32));
    log.info(&format!("New commit CID: {}", commit_cid.base32));
}

/// Sign a commit hash using the private key (helper for cmd_repair_commit).
fn sign_commit_hash(hash: &[u8; 32], private_key_multibase: &str) -> Result<Vec<u8>, String> {
    use p256::ecdsa::{signature::hazmat::PrehashSigner, Signature, SigningKey};

    // Decode the multibase private key (z prefix = base58btc)
    if !private_key_multibase.starts_with('z') {
        return Err("Private key must be multibase (base58btc, z prefix)".to_string());
    }

    let private_key_with_prefix = bs58::decode(&private_key_multibase[1..])
        .into_vec()
        .map_err(|e| format!("Invalid base58: {}", e))?;

    // Check for P-256 private key prefix (0x86 0x26)
    if private_key_with_prefix.len() < 34 {
        return Err("Private key too short".to_string());
    }

    if private_key_with_prefix[0] != 0x86 || private_key_with_prefix[1] != 0x26 {
        return Err(format!(
            "Expected P-256 private key prefix (0x86 0x26), got 0x{:02X} 0x{:02X}",
            private_key_with_prefix[0], private_key_with_prefix[1]
        ));
    }

    let private_key_bytes = &private_key_with_prefix[2..];
    if private_key_bytes.len() != 32 {
        return Err(format!(
            "Expected 32-byte private key, got {} bytes",
            private_key_bytes.len()
        ));
    }

    // Create signing key
    let signing_key = SigningKey::from_slice(private_key_bytes)
        .map_err(|e| format!("Invalid P-256 key: {}", e))?;

    // Sign the hash (prehashed)
    let signature: Signature = signing_key
        .sign_prehash(hash)
        .map_err(|e| format!("Signing failed: {}", e))?;

    // Get r and s values (IEEE P1363 format: r || s)
    let signature_bytes = signature.to_bytes();

    // Normalize to low-S form
    Ok(normalize_low_s(&signature_bytes))
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
    let s_high = compare_bytes(s, &half_order) > 0;

    if s_high {
        // s = order - s
        let new_s = subtract_bytes(&order, s);
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


