//! sync_get_record client command.
//!
//! Calls com.atproto.sync.getRecord on a remote PDS and verifies the
//! returned CAR file contains a valid proof chain for the requested record.
//!
//! Verification is based on the XRPC server implementation in
//! `pds/xrpc/sync_get_record.rs`, which builds the CAR as:
//!   1. CAR header (version=1, roots=[commit CID])
//!   2. Commit block (did, version, data, rev, prev, sig)
//!   3. MST proof chain nodes (root → leaf)
//!   4. Record block

use std::collections::HashMap;
use std::io::Cursor;

use crate::cli::get_arg;
use crate::log::logger;
use crate::repo::{CidV1, RepoRecord, Repo};
use crate::ws::{BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};


/// Run the sync_get_record client command.
pub async fn cmd_sync_get_record(args: &HashMap<String, String>) {
    let log = logger();

    // Parse arguments
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command SyncGetRecord /actor <handle_or_did> /collection <nsid> /rkey <rkey>");
            return;
        }
    };

    let collection = match get_arg(args, "collection") {
        Some(c) => c,
        None => {
            log.error("missing /collection argument");
            log.error("Usage: rustproto /command SyncGetRecord /actor <handle_or_did> /collection <nsid> /rkey <rkey>");
            return;
        }
    };

    let rkey = match get_arg(args, "rkey") {
        Some(r) => r,
        None => {
            log.error("missing /rkey argument");
            log.error("Usage: rustproto /command SyncGetRecord /actor <handle_or_did> /collection <nsid> /rkey <rkey>");
            return;
        }
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    // Resolve actor to DID + PDS
    log.info(&format!("Resolving actor: {}", actor));
    let info = match client.resolve_actor_info(actor, None).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Error resolving actor: {}", e));
            return;
        }
    };

    let did = match &info.did {
        Some(d) => d.clone(),
        None => {
            log.error("Could not resolve DID for actor");
            return;
        }
    };

    let pds = match &info.pds {
        Some(p) => p.clone(),
        None => {
            log.error("Could not resolve PDS for actor");
            return;
        }
    };

    log.info(&format!("DID: {}", did));
    log.info(&format!("PDS: {}", pds));

    // Fetch the record CAR from the PDS
    log.info(&format!(
        "Fetching record: at://{}/{}/{}",
        did, collection, rkey
    ));

    let car_bytes = match client.sync_get_record(&pds, &did, collection, rkey).await {
        Ok(bytes) => bytes,
        Err(e) => {
            log.error(&format!("Error fetching record: {}", e));
            return;
        }
    };

    log.info(&format!("Received {} bytes", car_bytes.len()));

    // Parse and verify the CAR
    log.info("");
    log.info("=== VERIFYING CAR RESPONSE ===");
    log.info("");

    match verify_record_car(&car_bytes, &did, collection, rkey) {
        Ok(result) => {
            log.info("--- CAR HEADER ---");
            log.info(&format!("  Version:  {}", result.header_version));
            log.info(&format!("  Root CID: {}", result.root_cid));
            log.info("");

            log.info("--- COMMIT BLOCK ---");
            log.info(&format!("  CID:     {}", result.commit_cid));
            log.info(&format!("  DID:     {}", result.commit_did));
            log.info(&format!("  Version: {}", result.commit_version));
            log.info(&format!("  Rev:     {}", result.commit_rev));
            log.info(&format!("  Data:    {}", result.commit_data_cid));
            log.info("");

            log.info(&format!(
                "--- MST PROOF CHAIN ({} nodes) ---",
                result.proof_node_count
            ));
            for (i, node_info) in result.proof_nodes.iter().enumerate() {
                log.info(&format!(
                    "  Node {}: CID={} entries={}",
                    i, node_info.cid, node_info.entry_count
                ));
            }
            log.info("");

            log.info("--- RECORD BLOCK ---");
            log.info(&format!("  CID:   {}", result.record_cid));
            log.info(&format!("  $type: {}", result.record_type));
            log.info("");

            log.info("--- VERIFICATION RESULTS ---");
            let mut all_passed = true;
            for check in &result.checks {
                if check.passed {
                    log.info(&format!("  [PASS] {}", check.description));
                } else {
                    all_passed = false;
                    log.error(&format!("  [FAIL] {}", check.description));
                    if let Some(ref detail) = check.detail {
                        log.error(&format!("         {}", detail));
                    }
                }
            }
            log.info("");

            if all_passed {
                log.info(&format!(
                    "ALL {} CHECKS PASSED - CAR is valid",
                    result.checks.len()
                ));
            } else {
                let failed = result.checks.iter().filter(|c| !c.passed).count();
                log.error(&format!(
                    "{} of {} checks FAILED",
                    failed,
                    result.checks.len()
                ));
            }
        }
        Err(e) => {
            log.error(&format!("CAR verification failed: {}", e));
        }
    }
}


// =====================================================================
// Verification types
// =====================================================================

struct VerificationResult {
    header_version: i64,
    root_cid: String,
    commit_cid: String,
    commit_did: String,
    commit_version: i64,
    commit_rev: String,
    commit_data_cid: String,
    proof_node_count: usize,
    proof_nodes: Vec<ProofNodeInfo>,
    record_cid: String,
    record_type: String,
    checks: Vec<Check>,
}

struct ProofNodeInfo {
    cid: String,
    entry_count: usize,
}

struct Check {
    passed: bool,
    description: String,
    detail: Option<String>,
}

impl Check {
    fn pass(description: impl Into<String>) -> Self {
        Check {
            passed: true,
            description: description.into(),
            detail: None,
        }
    }

    fn fail(description: impl Into<String>, detail: impl Into<String>) -> Self {
        Check {
            passed: false,
            description: description.into(),
            detail: Some(detail.into()),
        }
    }
}


// =====================================================================
// Verification logic
// =====================================================================

/// Verify a CAR file returned by com.atproto.sync.getRecord.
///
/// The expected structure (per the XRPC implementation) is:
///   - CAR header: { version: 1, roots: [commit_cid] }
///   - Block 0: commit (has did, version, data, rev, sig)
///   - Blocks 1..N-1: MST proof chain nodes
///   - Block N: the record itself
fn verify_record_car(
    car_bytes: &[u8],
    expected_did: &str,
    expected_collection: &str,
    expected_rkey: &str,
) -> Result<VerificationResult, String> {
    let mut checks: Vec<Check> = Vec::new();

    // Parse the CAR
    let cursor = Cursor::new(car_bytes);
    let (header, records) = Repo::read_repo(cursor)
        .map_err(|e| format!("Failed to parse CAR: {}", e))?;

    // --- CHECK: CAR header version ---
    if header.version == 1 {
        checks.push(Check::pass("CAR header version is 1"));
    } else {
        checks.push(Check::fail(
            "CAR header version is 1",
            format!("Got version {}", header.version),
        ));
    }

    let root_cid_base32 = header.repo_commit_cid.base32.clone();

    // --- CHECK: At least 3 blocks (commit + at least 1 MST node + record) ---
    if records.len() >= 3 {
        checks.push(Check::pass(format!(
            "CAR contains at least 3 blocks (got {})",
            records.len()
        )));
    } else {
        checks.push(Check::fail(
            "CAR contains at least 3 blocks",
            format!("Got {} blocks", records.len()),
        ));
    }

    if records.is_empty() {
        return Err("CAR contains no blocks".to_string());
    }

    // ---- COMMIT BLOCK (first block) ----
    let commit_record = &records[0];
    let commit_cid_base32 = commit_record.cid.base32.clone();

    // --- CHECK: Root CID matches commit CID ---
    if root_cid_base32 == commit_cid_base32 {
        checks.push(Check::pass("CAR root CID matches commit block CID"));
    } else {
        checks.push(Check::fail(
            "CAR root CID matches commit block CID",
            format!(
                "Root={}, Commit={}",
                root_cid_base32, commit_cid_base32
            ),
        ));
    }

    // --- CHECK: Commit CID integrity (recompute from DAG-CBOR) ---
    verify_block_cid(&commit_record, &mut checks, "Commit");

    // --- CHECK: Commit has required fields ---
    let commit = &commit_record.data_block;
    let commit_did = commit.select_string(&["did"]).unwrap_or_default();
    let commit_version = commit.select_int(&["version"]).unwrap_or(-1);
    let commit_rev = commit.select_string(&["rev"]).unwrap_or_default();
    let commit_data_cid = commit.select_cid(&["data"]);

    let has_did = !commit_did.is_empty();
    let has_version = commit_version > 0;
    let has_rev = !commit_rev.is_empty();
    let has_data = commit_data_cid.is_some();
    let has_sig = commit.select_bytes(&["sig"]).is_some();

    if has_did && has_version && has_rev && has_data && has_sig {
        checks.push(Check::pass(
            "Commit block has required fields (did, version, rev, data, sig)",
        ));
    } else {
        let mut missing = Vec::new();
        if !has_did { missing.push("did"); }
        if !has_version { missing.push("version"); }
        if !has_rev { missing.push("rev"); }
        if !has_data { missing.push("data"); }
        if !has_sig { missing.push("sig"); }
        checks.push(Check::fail(
            "Commit block has required fields (did, version, rev, data, sig)",
            format!("Missing: {}", missing.join(", ")),
        ));
    }

    // --- CHECK: Commit DID matches expected DID ---
    if commit_did == expected_did {
        checks.push(Check::pass("Commit DID matches expected actor DID"));
    } else {
        checks.push(Check::fail(
            "Commit DID matches expected actor DID",
            format!("Expected={}, Got={}", expected_did, commit_did),
        ));
    }

    // --- CHECK: Commit version is 3 ---
    if commit_version == 3 {
        checks.push(Check::pass("Commit version is 3"));
    } else {
        checks.push(Check::fail(
            "Commit version is 3",
            format!("Got {}", commit_version),
        ));
    }

    // ---- MST PROOF CHAIN + RECORD (remaining blocks) ----
    // Last block should be the record, blocks in between are MST proof nodes
    let last_idx = records.len() - 1;
    let record_block = &records[last_idx];
    let mst_proof_blocks: Vec<&RepoRecord> = records[1..last_idx].iter().collect();

    // --- CHECK: Record CID integrity ---
    verify_block_cid(record_block, &mut checks, "Record");

    // --- CHECK: Record has $type field ---
    let record_type = record_block
        .data_block
        .select_string(&["$type"])
        .unwrap_or_default();
    if !record_type.is_empty() {
        checks.push(Check::pass(format!(
            "Record has $type field ({})",
            record_type
        )));
    } else {
        checks.push(Check::fail("Record has $type field", "No $type found"));
    }

    // --- CHECK: MST proof node CID integrity ---
    let mut proof_nodes_info = Vec::new();
    for (i, mst_block) in mst_proof_blocks.iter().enumerate() {
        verify_block_cid(mst_block, &mut checks, &format!("MST node {}", i));

        let entry_count = mst_block
            .data_block
            .select_array(&["e"])
            .map(|arr| arr.len())
            .unwrap_or(0);

        proof_nodes_info.push(ProofNodeInfo {
            cid: mst_block.cid.base32.clone(),
            entry_count,
        });
    }

    // --- CHECK: MST proof nodes are valid MST nodes (have "e" array) ---
    let all_valid_mst = mst_proof_blocks.iter().all(|b| {
        b.data_block.select_array(&["e"]).is_some()
    });
    if all_valid_mst {
        checks.push(Check::pass("All MST proof blocks have 'e' (entries) array"));
    } else {
        checks.push(Check::fail(
            "All MST proof blocks have 'e' (entries) array",
            "One or more MST blocks missing 'e' field",
        ));
    }

    // --- CHECK: Commit data CID points to first MST node ---
    if let Some(data_cid) = commit_data_cid {
        if !mst_proof_blocks.is_empty() {
            let first_mst_cid = &mst_proof_blocks[0].cid.base32;
            if data_cid.base32 == *first_mst_cid {
                checks.push(Check::pass(
                    "Commit 'data' CID matches first MST proof node CID",
                ));
            } else {
                checks.push(Check::fail(
                    "Commit 'data' CID matches first MST proof node CID",
                    format!(
                        "data={}, first MST={}",
                        data_cid.base32, first_mst_cid
                    ),
                ));
            }
        }
    }

    // --- CHECK: MST proof chain links are valid (each node points to next via subtree) ---
    verify_proof_chain_links(&mst_proof_blocks, &mut checks);

    // --- CHECK: Final MST node contains entry with record CID ---
    let record_cid_base32 = record_block.cid.base32.clone();
    let full_key = format!("{}/{}", expected_collection, expected_rkey);
    verify_record_in_mst_leaf(
        &mst_proof_blocks,
        &record_cid_base32,
        &full_key,
        &mut checks,
    );

    let commit_data_str = commit_data_cid
        .map(|c| c.base32.clone())
        .unwrap_or_else(|| "<missing>".to_string());

    Ok(VerificationResult {
        header_version: header.version,
        root_cid: root_cid_base32,
        commit_cid: commit_cid_base32,
        commit_did,
        commit_version,
        commit_rev,
        commit_data_cid: commit_data_str,
        proof_node_count: mst_proof_blocks.len(),
        proof_nodes: proof_nodes_info,
        record_cid: record_cid_base32,
        record_type,
        checks,
    })
}


/// Verify that a block's CID matches the SHA-256 hash of its DAG-CBOR serialization.
fn verify_block_cid(record: &RepoRecord, checks: &mut Vec<Check>, label: &str) {
    match record.data_block.to_bytes() {
        Ok(serialized) => match CidV1::compute_cid_for_dag_cbor_bytes(&serialized) {
            Ok(computed_cid) => {
                if computed_cid.base32 == record.cid.base32 {
                    checks.push(Check::pass(format!(
                        "{} block CID integrity (SHA-256 matches)",
                        label
                    )));
                } else {
                    checks.push(Check::fail(
                        format!("{} block CID integrity (SHA-256 matches)", label),
                        format!(
                            "Claimed={}, Computed={}",
                            record.cid.base32, computed_cid.base32
                        ),
                    ));
                }
            }
            Err(e) => {
                checks.push(Check::fail(
                    format!("{} block CID integrity", label),
                    format!("Failed to compute CID: {}", e),
                ));
            }
        },
        Err(e) => {
            checks.push(Check::fail(
                format!("{} block CID integrity", label),
                format!("Failed to serialize DAG-CBOR: {}", e),
            ));
        }
    }
}


/// Verify that each MST proof node links to the next via subtree pointers (l or t).
///
/// The proof chain goes from root to the leaf that contains the record key.
/// Each node should contain a subtree pointer (left link or entry right-tree)
/// that matches the CID of the next node in the chain.
fn verify_proof_chain_links(mst_blocks: &[&RepoRecord], checks: &mut Vec<Check>) {
    if mst_blocks.len() < 2 {
        // Single MST node means the tree has one level; nothing to chain-verify.
        return;
    }

    for i in 0..mst_blocks.len() - 1 {
        let current = &mst_blocks[i].data_block;
        let next_cid = &mst_blocks[i + 1].cid.base32;

        let mut found_link = false;

        // Check "l" (left subtree) link
        if let Some(left_cid) = current.select_cid(&["l"]) {
            if left_cid.base32 == *next_cid {
                found_link = true;
            }
        }

        // Check "t" (right subtree) links in entries
        if !found_link {
            if let Some(entries) = current.select_array(&["e"]) {
                for entry in entries {
                    if let Some(tree_cid) = entry.select_cid(&["t"]) {
                        if tree_cid.base32 == *next_cid {
                            found_link = true;
                            break;
                        }
                    }
                }
            }
        }

        if found_link {
            checks.push(Check::pass(format!(
                "MST proof chain link {} → {} is valid",
                i,
                i + 1
            )));
        } else {
            checks.push(Check::fail(
                format!("MST proof chain link {} → {} is valid", i, i + 1),
                format!(
                    "Node {} does not contain subtree pointer to node {} (CID={})",
                    i,
                    i + 1,
                    next_cid
                ),
            ));
        }
    }
}


/// Verify that the last MST proof node contains an entry whose value CID
/// matches the record CID and whose key matches the expected collection/rkey.
fn verify_record_in_mst_leaf(
    mst_blocks: &[&RepoRecord],
    record_cid: &str,
    full_key: &str,
    checks: &mut Vec<Check>,
) {
    if mst_blocks.is_empty() {
        checks.push(Check::fail(
            "MST leaf contains record entry",
            "No MST proof nodes present",
        ));
        return;
    }

    let leaf = mst_blocks.last().unwrap();
    let entries = match leaf.data_block.select_array(&["e"]) {
        Some(e) => e,
        None => {
            checks.push(Check::fail(
                "MST leaf contains record entry",
                "Leaf node has no 'e' array",
            ));
            return;
        }
    };

    // Reconstruct full keys from prefix-compressed entries (same logic as repo_mst.rs)
    let mut full_keys: Vec<String> = Vec::new();
    let mut found_record = false;

    for (i, entry) in entries.iter().enumerate() {
        let prefix_length = entry.select_int(&["p"]).unwrap_or(0) as usize;
        let key_suffix = match entry.select_bytes(&["k"]) {
            Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
            None => continue,
        };

        let reconstructed_key = if i == 0 {
            key_suffix.clone()
        } else {
            let prev_key = &full_keys[i - 1];
            let prefix = &prev_key[..prefix_length.min(prev_key.len())];
            format!("{}{}", prefix, key_suffix)
        };

        full_keys.push(reconstructed_key.clone());

        // Check if this entry matches our record
        if let Some(value_cid) = entry.select_cid(&["v"]) {
            if value_cid.base32 == record_cid && reconstructed_key == full_key {
                found_record = true;
            }
        }
    }

    if found_record {
        checks.push(Check::pass(format!(
            "MST leaf contains entry for '{}' pointing to record CID",
            full_key
        )));
    } else {
        // Provide more detail about what we did find
        let found_keys: Vec<String> = full_keys.clone();
        let found_cid_match = entries.iter().any(|e| {
            e.select_cid(&["v"])
                .map(|c| c.base32 == record_cid)
                .unwrap_or(false)
        });

        let detail = if found_cid_match {
            format!(
                "CID match found but key mismatch. Expected key '{}'. Found keys: {:?}",
                full_key, found_keys
            )
        } else {
            format!(
                "No entry with matching CID. Expected key='{}', CID={}. Found keys: {:?}",
                full_key, record_cid, found_keys
            )
        };

        checks.push(Check::fail(
            format!(
                "MST leaf contains entry for '{}' pointing to record CID",
                full_key
            ),
            detail,
        ));
    }
}
