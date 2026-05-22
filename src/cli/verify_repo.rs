//! verify_repo client command.
//!
//! Performs extensive verification of a full repo CAR file stored on disk
//! for the given actor. Verifies the CAR structure, the commit block,
//! every block's CID integrity, the full MST tree connectivity, and that
//! every MST record entry resolves to a record block (and vice-versa).

use std::collections::{HashMap, HashSet};

use crate::cli::{get_arg, resolve_repo_file};
use crate::cli::verify_common::{
    check_block_classification, check_car_header_version, classify_blocks,
    log_checks_summary, verify_block_cid, verify_commit_block, Check, CommitInfo,
};
use crate::log::logger;
use crate::repo::{Repo, RepoRecord};


/// Run the verify_repo client command.
pub async fn cmd_verify_repo(args: &HashMap<String, String>) {
    let log = logger();

    let repo_file = match resolve_repo_file(args).await {
        Some(path) => path,
        None => {
            log.error("Could not resolve repo file. Use /repoFile <path> or /actor <handle> /dataDir <path>");
            return;
        }
    };

    log.info(&format!("Verifying repo file: {}", repo_file.display()));

    let expected_did = get_arg(args, "did")
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let (header, records) = match Repo::read_repo_file(&repo_file) {
        Ok(pair) => pair,
        Err(e) => {
            log.error(&format!("Failed to read repo file: {}", e));
            return;
        }
    };

    log.info(&format!("Loaded {} blocks", records.len()));
    log.info("");
    log.info("=== VERIFYING FULL REPO CAR ===");
    log.info("");

    let result = verify_full_repo(&header, &records, expected_did.as_deref());

    log.info("--- CAR HEADER ---");
    log.info(&format!("  Version:  {}", header.version));
    log.info(&format!("  Root CID: {}", header.repo_commit_cid.base32));
    log.info("");

    if let Some(ref commit) = result.commit_info {
        log.info("--- COMMIT BLOCK ---");
        log.info(&format!("  CID:     {}", commit.cid));
        log.info(&format!("  DID:     {}", commit.did));
        log.info(&format!("  Version: {}", commit.version));
        log.info(&format!("  Rev:     {}", commit.rev));
        log.info(&format!("  Data:    {}", commit.data_cid_str()));
        log.info("");
    }

    log.info(&format!(
        "--- BLOCK COUNTS: commit={}, mst={}, records={}, unclassified={} ---",
        result.commit_block_count,
        result.mst_block_count,
        result.record_block_count,
        result.unclassified_block_count
    ));
    log.info("");

    log.info("--- VERIFICATION RESULTS ---");
    log_checks_summary(&result.checks);
}


struct FullRepoVerificationResult {
    commit_info: Option<CommitInfo>,
    commit_block_count: usize,
    mst_block_count: usize,
    record_block_count: usize,
    unclassified_block_count: usize,
    checks: Vec<Check>,
}


/// Verify a full on-disk repo CAR file.
fn verify_full_repo(
    header: &crate::repo::RepoHeader,
    records: &[RepoRecord],
    expected_did: Option<&str>,
) -> FullRepoVerificationResult {
    let mut checks: Vec<Check> = Vec::new();

    // --- CHECK: CAR header version ---
    check_car_header_version(header, &mut checks);

    // ---- CLASSIFY BLOCKS ----
    let classified = classify_blocks(records);
    let commit_blocks = &classified.commit;
    let mst_blocks = &classified.mst;
    let record_blocks = &classified.record;

    // --- CHECKS: exactly 1 commit + all classifiable ---
    check_block_classification(&classified, &mut checks);

    let root_cid_base32 = header.repo_commit_cid.base32.clone();
    let commit_record = match commit_blocks.first().copied() {
        Some(c) => c,
        None => {
            return FullRepoVerificationResult {
                commit_info: None,
                commit_block_count: commit_blocks.len(),
                mst_block_count: mst_blocks.len(),
                record_block_count: record_blocks.len(),
                unclassified_block_count: classified.unclassified.len(),
                checks,
            };
        }
    };

    // --- CHECKS: root↔commit, commit CID, required fields, version=3, DID ---
    let commit_info = verify_commit_block(
        commit_record,
        &root_cid_base32,
        expected_did,
        &mut checks,
    );
    let commit_data_cid = commit_info.data_cid.clone();

    // --- CHECK: At least one MST node present ---
    if !mst_blocks.is_empty() {
        checks.push(Check::pass(format!(
            "CAR contains MST nodes (got {})",
            mst_blocks.len()
        )));
    } else {
        checks.push(Check::fail("CAR contains MST nodes", "No MST nodes found"));
    }

    // --- CHECK: All MST blocks have 'e' (entries) array ---
    let all_have_entries = mst_blocks
        .iter()
        .all(|b| b.data_block.select_array(&["e"]).is_some());
    if all_have_entries {
        checks.push(Check::pass("All MST blocks have 'e' (entries) array"));
    } else {
        checks.push(Check::fail(
            "All MST blocks have 'e' (entries) array",
            "One or more MST blocks missing 'e' field",
        ));
    }

    // --- CHECK: All record blocks have $type field ---
    let all_have_type = record_blocks.iter().all(|b| {
        b.data_block
            .select_string(&["$type"])
            .map(|s| !s.is_empty())
            .unwrap_or(false)
    });
    if all_have_type {
        checks.push(Check::pass(format!(
            "All {} record blocks have $type field",
            record_blocks.len()
        )));
    } else {
        checks.push(Check::fail(
            "All record blocks have $type field",
            "One or more record blocks missing $type",
        ));
    }

    // --- CHECK: MST node CID integrity for every node ---
    let mut mst_by_cid: HashMap<String, &RepoRecord> = HashMap::new();
    let mut mst_cid_integrity_failures = 0;
    for (i, block) in mst_blocks.iter().enumerate() {
        let mut local_checks = Vec::new();
        verify_block_cid(block, &mut local_checks, &format!("MST node {}", i));
        for c in &local_checks {
            if !c.passed { mst_cid_integrity_failures += 1; }
        }
        mst_by_cid.insert(block.cid.base32.clone(), *block);
    }
    if mst_cid_integrity_failures == 0 {
        checks.push(Check::pass(format!(
            "All {} MST node CIDs match their DAG-CBOR SHA-256",
            mst_blocks.len()
        )));
    } else {
        checks.push(Check::fail(
            format!(
                "All {} MST node CIDs match their DAG-CBOR SHA-256",
                mst_blocks.len()
            ),
            format!("{} MST node(s) failed CID integrity", mst_cid_integrity_failures),
        ));
    }

    // --- CHECK: Record block CID integrity for every record ---
    let mut record_by_cid: HashMap<String, &RepoRecord> = HashMap::new();
    let mut record_cid_integrity_failures = 0;
    for (i, block) in record_blocks.iter().enumerate() {
        let mut local_checks = Vec::new();
        verify_block_cid(block, &mut local_checks, &format!("Record {}", i));
        for c in &local_checks {
            if !c.passed { record_cid_integrity_failures += 1; }
        }
        record_by_cid.insert(block.cid.base32.clone(), *block);
    }
    if record_cid_integrity_failures == 0 {
        checks.push(Check::pass(format!(
            "All {} record block CIDs match their DAG-CBOR SHA-256",
            record_blocks.len()
        )));
    } else {
        checks.push(Check::fail(
            format!(
                "All {} record block CIDs match their DAG-CBOR SHA-256",
                record_blocks.len()
            ),
            format!("{} record block(s) failed CID integrity", record_cid_integrity_failures),
        ));
    }

    // --- CHECK: Commit data CID points to an MST node ---
    if let Some(ref data_cid) = commit_data_cid {
        if mst_by_cid.contains_key(&data_cid.base32) {
            checks.push(Check::pass(
                "Commit 'data' CID points to an MST node in the CAR (root of tree)",
            ));
        } else {
            checks.push(Check::fail(
                "Commit 'data' CID points to an MST node in the CAR (root of tree)",
                format!("data={} not found among MST nodes", data_cid.base32),
            ));
        }
    }

    // --- CHECK: MST subtree pointers resolve, and entry 'v' CIDs resolve ---
    let mut dangling_subtree = 0;
    let mut dangling_value = 0;
    let mut referenced_records: HashSet<String> = HashSet::new();

    for block in mst_blocks {
        // 'l' (left subtree) pointer
        if let Some(left_cid) = block.data_block.select_cid(&["l"]) {
            if !mst_by_cid.contains_key(&left_cid.base32) {
                dangling_subtree += 1;
            }
        }

        if let Some(entries) = block.data_block.select_array(&["e"]) {
            for entry in entries {
                // 't' (right subtree) pointer (optional)
                if let Some(tree_cid) = entry.select_cid(&["t"]) {
                    if !mst_by_cid.contains_key(&tree_cid.base32) {
                        dangling_subtree += 1;
                    }
                }
                // 'v' (record value) pointer
                if let Some(value_cid) = entry.select_cid(&["v"]) {
                    if record_by_cid.contains_key(&value_cid.base32) {
                        referenced_records.insert(value_cid.base32.clone());
                    } else {
                        dangling_value += 1;
                    }
                }
            }
        }
    }

    if dangling_subtree == 0 {
        checks.push(Check::pass(
            "All MST subtree pointers ('l' and entry 't') resolve to MST nodes in the CAR",
        ));
    } else {
        checks.push(Check::fail(
            "All MST subtree pointers ('l' and entry 't') resolve to MST nodes in the CAR",
            format!("{} dangling subtree pointer(s)", dangling_subtree),
        ));
    }

    if dangling_value == 0 {
        checks.push(Check::pass(
            "All MST entry 'v' (record) CIDs resolve to record blocks in the CAR",
        ));
    } else {
        checks.push(Check::fail(
            "All MST entry 'v' (record) CIDs resolve to record blocks in the CAR",
            format!("{} MST entry value CID(s) point to missing record blocks", dangling_value),
        ));
    }

    // --- CHECK: MST is fully connected (every MST node reachable from root) ---
    if let Some(ref data_cid) = commit_data_cid {
        let mut reachable: HashSet<String> = HashSet::new();
        let mut stack: Vec<String> = Vec::new();
        if mst_by_cid.contains_key(&data_cid.base32) {
            stack.push(data_cid.base32.clone());
        }
        while let Some(cid) = stack.pop() {
            if !reachable.insert(cid.clone()) {
                continue;
            }
            let block = match mst_by_cid.get(&cid) {
                Some(b) => *b,
                None => continue,
            };
            if let Some(left_cid) = block.data_block.select_cid(&["l"]) {
                if mst_by_cid.contains_key(&left_cid.base32)
                    && !reachable.contains(&left_cid.base32)
                {
                    stack.push(left_cid.base32.clone());
                }
            }
            if let Some(entries) = block.data_block.select_array(&["e"]) {
                for entry in entries {
                    if let Some(tree_cid) = entry.select_cid(&["t"]) {
                        if mst_by_cid.contains_key(&tree_cid.base32)
                            && !reachable.contains(&tree_cid.base32)
                        {
                            stack.push(tree_cid.base32.clone());
                        }
                    }
                }
            }
        }

        if reachable.len() == mst_blocks.len() {
            checks.push(Check::pass(format!(
                "MST is fully connected: all {} MST nodes reachable from root",
                mst_blocks.len()
            )));
        } else {
            checks.push(Check::fail(
                "MST is fully connected: all MST nodes reachable from root",
                format!(
                    "{} of {} MST nodes reachable from root ({} unreachable)",
                    reachable.len(),
                    mst_blocks.len(),
                    mst_blocks.len() - reachable.len()
                ),
            ));
        }
    }

    // --- CHECK: Every record block is referenced by some MST entry ---
    let orphaned_records: Vec<&String> = record_by_cid
        .keys()
        .filter(|cid| !referenced_records.contains(*cid))
        .collect();
    if orphaned_records.is_empty() {
        checks.push(Check::pass(format!(
            "All {} record blocks are referenced by an MST entry (no orphans)",
            record_blocks.len()
        )));
    } else {
        checks.push(Check::fail(
            "All record blocks are referenced by an MST entry (no orphans)",
            format!("{} orphaned record block(s) not referenced by any MST entry", orphaned_records.len()),
        ));
    }

    FullRepoVerificationResult {
        commit_info: Some(commit_info),
        commit_block_count: commit_blocks.len(),
        mst_block_count: mst_blocks.len(),
        record_block_count: record_blocks.len(),
        unclassified_block_count: classified.unclassified.len(),
        checks,
    }
}
