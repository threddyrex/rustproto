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
use crate::log::{logger, LogLevel};
use crate::mst::Mst;
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

    // At trace level, dump every block in the CAR the same way PrintRepoRecords does
    trace_dump_car(&car_bytes);

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
                "--- MST PROOF CHAIN ({} nodes, key depth {}) ---",
                result.proof_node_count, result.record_key_depth
            ));
            log.info(&format!("  Key:   {}", result.record_key));
            log.info(&format!("  Depth: {} (SHA-256 leading zeros / 2-bit chunks)", result.record_key_depth));
            for (i, node_info) in result.proof_nodes.iter().enumerate() {
                let layer_str = match node_info.layer {
                    Some(d) => format!(" layer={}", d),
                    None => String::new(),
                };
                log.info(&format!(
                    "  Node {}: CID={} entries={}{}",
                    i, node_info.cid, node_info.entry_count, layer_str
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
// Trace-level CAR dump (same format as PrintRepoRecords)
// =====================================================================

/// If the log level is trace, parse and print every block in the CAR.
fn trace_dump_car(car_bytes: &[u8]) {
    let log = logger();
    if log.level() > LogLevel::Trace {
        return;
    }

    let cursor = Cursor::new(car_bytes);
    let result = Repo::walk_repo(
        cursor,
        |header| {
            log.trace("");
            log.trace("REPO HEADER:");
            log.trace(&format!("   roots: {}", header.repo_commit_cid.get_base32()));
            log.trace(&format!("   version: {}", header.version));
            true
        },
        |record: &RepoRecord| {
            let record_type_str = record.get_record_type_string();
            log.trace("");
            log.trace(&format!("{}:", record_type_str));
            log.trace(&format!("  cid: {}", record.cid.get_base32()));
            log.trace(&format!("  blockJson:\n {}", record.json_string));
            true
        },
    );

    if let Err(e) = result {
        log.trace(&format!("Error walking CAR for trace dump: {}", e));
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
    record_key: String,
    record_key_depth: i32,
    record_cid: String,
    record_type: String,
    checks: Vec<Check>,
}

struct ProofNodeInfo {
    cid: String,
    entry_count: usize,
    layer: Option<i32>,
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
/// Blocks are classified using the same detection logic as `RepoRecord`:
///   - `is_repo_commit()` → commit block (has did, data, rev, version)
///   - `is_mst_node()` → MST proof chain node (has "e" or "l")
///   - `is_at_proto_record()` → the record itself (has "$type")
///
/// The root CID in the CAR header identifies the commit block.
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

    // ---- CLASSIFY BLOCKS by type, not position ----
    let mut commit_blocks: Vec<&RepoRecord> = Vec::new();
    let mut mst_blocks: Vec<&RepoRecord> = Vec::new();
    let mut record_blocks: Vec<&RepoRecord> = Vec::new();
    let mut unclassified_blocks: Vec<&RepoRecord> = Vec::new();

    for record in &records {
        if record.is_repo_commit() {
            commit_blocks.push(record);
        } else if record.is_mst_node() {
            mst_blocks.push(record);
        } else if record.is_at_proto_record() {
            record_blocks.push(record);
        } else {
            unclassified_blocks.push(record);
        }
    }

    // --- CHECK: Exactly one commit block ---
    if commit_blocks.len() == 1 {
        checks.push(Check::pass("CAR contains exactly 1 commit block"));
    } else {
        checks.push(Check::fail(
            "CAR contains exactly 1 commit block",
            format!("Found {} commit blocks", commit_blocks.len()),
        ));
    }

    // --- CHECK: At least one MST node ---
    if !mst_blocks.is_empty() {
        checks.push(Check::pass(format!(
            "CAR contains MST proof nodes (got {})",
            mst_blocks.len()
        )));
    } else {
        checks.push(Check::fail(
            "CAR contains MST proof nodes",
            "No MST nodes found",
        ));
    }

    // --- CHECK: Exactly one record block ---
    if record_blocks.len() == 1 {
        checks.push(Check::pass("CAR contains exactly 1 record block"));
    } else {
        checks.push(Check::fail(
            "CAR contains exactly 1 record block",
            format!("Found {} record blocks", record_blocks.len()),
        ));
    }

    // --- CHECK: No unclassified blocks ---
    if unclassified_blocks.is_empty() {
        checks.push(Check::pass("All blocks are classifiable (commit, MST, or record)"));
    } else {
        checks.push(Check::fail(
            "All blocks are classifiable (commit, MST, or record)",
            format!("{} unclassified blocks", unclassified_blocks.len()),
        ));
    }

    // Use first found of each type (gracefully handle missing)
    let commit_record = match commit_blocks.first() {
        Some(c) => *c,
        None => return Err("No commit block found in CAR".to_string()),
    };
    let record_block = match record_blocks.first() {
        Some(r) => *r,
        None => return Err("No record block found in CAR".to_string()),
    };

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
    verify_block_cid(commit_record, &mut checks, "Commit");

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
    for (i, mst_block) in mst_blocks.iter().enumerate() {
        verify_block_cid(mst_block, &mut checks, &format!("MST node {}", i));

        let entries = mst_block.data_block.select_array(&["e"]);
        let entry_count = entries.map(|arr| arr.len()).unwrap_or(0);

        // Compute node layer from the depth of its first entry's key
        let layer = entries.and_then(|arr| {
            let first = arr.first()?;
            let key_bytes = first.select_bytes(&["k"])?;
            let key = String::from_utf8_lossy(key_bytes);
            Some(Mst::get_key_depth_str(&key))
        });

        proof_nodes_info.push(ProofNodeInfo {
            cid: mst_block.cid.base32.clone(),
            entry_count,
            layer,
        });
    }

    // --- CHECK: MST proof nodes are valid MST nodes (have "e" array) ---
    let all_valid_mst = mst_blocks.iter().all(|b| {
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

    // --- CHECK: Commit data CID points to an MST node ---
    // The commit's "data" field is the root MST node CID. It should match
    // one of the MST proof nodes (specifically the root of the proof chain).
    if let Some(data_cid) = commit_data_cid {
        let root_mst_match = mst_blocks.iter().any(|b| b.cid.base32 == data_cid.base32);
        if root_mst_match {
            checks.push(Check::pass(
                "Commit 'data' CID matches an MST proof node CID (root of proof chain)",
            ));
        } else {
            checks.push(Check::fail(
                "Commit 'data' CID matches an MST proof node CID (root of proof chain)",
                format!(
                    "data={}, not found among {} MST nodes",
                    data_cid.base32, mst_blocks.len()
                ),
            ));
        }
    }

    // --- CHECK: MST proof chain links are valid (each node points to next via subtree) ---
    // Order the MST blocks into a chain starting from the commit's data CID,
    // using the record's key to route through the correct subtrees
    let full_key = format!("{}/{}", expected_collection, expected_rkey);
    let key_depth = Mst::get_key_depth_str(&full_key);
    let ordered_mst = order_proof_chain(commit_data_cid, &mst_blocks, &full_key);
    verify_proof_chain_links(&ordered_mst, &mut checks);

    // --- CHECK: Proof chain depth matches key depth ---
    // The proof chain should contain (root_depth - key_depth + 1) nodes,
    // i.e. one node per layer from the root down to the layer at key_depth.
    // We can verify that the ordered chain length is at least key_depth + 1
    // (root at some depth >= key_depth, descending to key_depth).
    // More precisely: the record entry lives at the node whose layer == key_depth.
    // The chain must reach that depth, so it needs at least (root_depth - key_depth + 1) nodes.
    // Since we don't know root_depth from the CAR alone, we check that the chain
    // has at least 1 node (root) and that the entry is found (checked next).
    {
        let chain_len = ordered_mst.len();
        let total_mst = mst_blocks.len();
        if chain_len == total_mst {
            checks.push(Check::pass(format!(
                "MST proof chain depth: {} nodes (key depth={}, all MST blocks on path)",
                chain_len, key_depth
            )));
        } else if chain_len > 0 {
            checks.push(Check::fail(
                format!(
                    "MST proof chain depth: routed through {} of {} MST blocks (key depth={})",
                    chain_len, total_mst, key_depth
                ),
                format!(
                    "{} MST block(s) not reachable via key routing — may indicate incorrect proof nodes",
                    total_mst - chain_len
                ),
            ));
        } else {
            checks.push(Check::fail(
                "MST proof chain depth",
                "Could not route from root — commit data CID may not match any MST node",
            ));
        }
    }

    // --- CHECK: MST proof contains entry with record CID ---
    let record_cid_base32 = record_block.cid.base32.clone();
    verify_record_in_mst_proof(
        &ordered_mst,
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
        proof_node_count: mst_blocks.len(),
        proof_nodes: proof_nodes_info,
        record_key: full_key,
        record_key_depth: key_depth,
        record_cid: record_cid_base32,
        record_type,
        checks,
    })
}


/// Order MST proof blocks into a chain by routing through the tree using
/// the record's key and lexicographic position.
///
/// Starts from the block whose CID matches `root_cid` (the commit's "data" field)
/// and walks down the tree the same way the MST routes a key:
///   1. At each node, reconstruct compressed keys and find the lexicographic
///      insertion point for the target key
///   2. If insertion point is 0 → follow "l" (left subtree)
///      Otherwise → follow entries[insertion_point - 1]'s "t" (right subtree)
///   3. Stop when no more proof nodes match the next CID
fn order_proof_chain<'a>(
    root_cid: Option<&CidV1>,
    mst_blocks: &[&'a RepoRecord],
    target_key: &str,
) -> Vec<&'a RepoRecord> {
    let root_cid = match root_cid {
        Some(c) => c,
        None => return mst_blocks.to_vec(),
    };

    // Build a lookup from CID (owned String) → block
    let mut by_cid: HashMap<String, &'a RepoRecord> = HashMap::new();
    for block in mst_blocks {
        by_cid.insert(block.cid.base32.clone(), *block);
    }

    let mut ordered = Vec::new();
    let mut current_cid = root_cid.base32.clone();

    while let Some(block) = by_cid.remove(&current_cid) {
        ordered.push(block);

        // Reconstruct full keys from prefix-compressed entries
        let entries = block.data_block.select_array(&["e"]);
        let empty_entries = Vec::new();
        let entries = entries.unwrap_or(&empty_entries);
        let mut node_keys: Vec<String> = Vec::new();
        for (i, entry) in entries.iter().enumerate() {
            let prefix_length = entry.select_int(&["p"]).unwrap_or(0) as usize;
            let key_suffix = match entry.select_bytes(&["k"]) {
                Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
                None => String::new(),
            };
            let reconstructed = if i == 0 {
                key_suffix
            } else if let Some(prev) = node_keys.last() {
                let prefix = &prev[..prefix_length.min(prev.len())];
                format!("{}{}", prefix, key_suffix)
            } else {
                key_suffix
            };
            node_keys.push(reconstructed);
        }

        // Find the lexicographic insertion point for the target key.
        // This mirrors Mst::assemble_item: scan entries until we find one
        // where target_key < entry_key.
        let mut insert_index = 0;
        for key in &node_keys {
            if target_key < key.as_str() {
                break;
            }
            insert_index += 1;
        }

        // Route to the correct subtree
        let next_cid = if insert_index == 0 {
            // Target key is before all entries → go left ("l")
            block.data_block.select_cid(&["l"])
                .map(|c| c.base32.clone())
        } else {
            // Target key is after entries[insert_index - 1] → follow its "t" subtree
            entries.get(insert_index - 1)
                .and_then(|e| e.select_cid(&["t"]))
                .map(|c| c.base32.clone())
        };

        match next_cid {
            Some(cid) if by_cid.contains_key(&cid) => {
                current_cid = cid;
            }
            _ => break,
        }
    }

    ordered
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


/// Verify that one of the MST proof nodes contains an entry whose value CID
/// matches the record CID and whose key matches the expected collection/rkey.
///
/// The record's entry lives at whichever MST depth its key hashes to,
/// which may be any node in the proof chain — not necessarily the last.
fn verify_record_in_mst_proof(
    mst_blocks: &[&RepoRecord],
    record_cid: &str,
    full_key: &str,
    checks: &mut Vec<Check>,
) {
    if mst_blocks.is_empty() {
        checks.push(Check::fail(
            "MST proof contains record entry",
            "No MST proof nodes present",
        ));
        return;
    }

    let mut all_found_keys: Vec<String> = Vec::new();
    let mut found_record = false;

    for block in mst_blocks {
        let entries = match block.data_block.select_array(&["e"]) {
            Some(e) => e,
            None => continue,
        };

        // Reconstruct full keys from prefix-compressed entries (same logic as repo_mst.rs)
        let mut node_keys: Vec<String> = Vec::new();

        for (i, entry) in entries.iter().enumerate() {
            let prefix_length = entry.select_int(&["p"]).unwrap_or(0) as usize;
            let key_suffix = match entry.select_bytes(&["k"]) {
                Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
                None => continue,
            };

            let reconstructed_key = if i == 0 {
                key_suffix.clone()
            } else if let Some(prev_key) = node_keys.last() {
                let prefix = &prev_key[..prefix_length.min(prev_key.len())];
                format!("{}{}", prefix, key_suffix)
            } else {
                key_suffix.clone()
            };

            node_keys.push(reconstructed_key.clone());

            // Check if this entry matches our record
            if let Some(value_cid) = entry.select_cid(&["v"]) {
                if value_cid.base32 == record_cid && reconstructed_key == full_key {
                    found_record = true;
                }
            }
        }

        all_found_keys.extend(node_keys);
    }

    if found_record {
        checks.push(Check::pass(format!(
            "MST proof contains entry for '{}' pointing to record CID",
            full_key
        )));
    } else {
        // Provide more detail about what we did find
        let found_cid_match = mst_blocks.iter().any(|block| {
            block.data_block.select_array(&["e"]).map_or(false, |entries| {
                entries.iter().any(|e| {
                    e.select_cid(&["v"])
                        .map(|c| c.base32 == record_cid)
                        .unwrap_or(false)
                })
            })
        });

        let detail = if found_cid_match {
            format!(
                "CID match found but key mismatch. Expected key '{}'. Found keys: {:?}",
                full_key, all_found_keys
            )
        } else {
            format!(
                "No entry with matching CID across {} proof nodes. Expected key='{}', CID={}. Found keys: {:?}",
                mst_blocks.len(), full_key, record_cid, all_found_keys
            )
        };

        checks.push(Check::fail(
            format!(
                "MST proof contains entry for '{}' pointing to record CID",
                full_key
            ),
            detail,
        ));
    }
}
