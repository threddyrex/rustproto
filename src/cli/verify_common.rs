//! Common verification primitives shared by CAR verification commands.
//!
//! Used by `sync_get_record` (verifies a proof-chain CAR returned by
//! com.atproto.sync.getRecord) and `verify_repo` (verifies a full on-disk
//! repo CAR).

use crate::log::logger;
use crate::repo::{CidV1, RepoHeader, RepoRecord};


/// A single verification check (pass/fail with optional detail).
pub struct Check {
    pub passed: bool,
    pub description: String,
    pub detail: Option<String>,
}

impl Check {
    pub fn pass(description: impl Into<String>) -> Self {
        Check {
            passed: true,
            description: description.into(),
            detail: None,
        }
    }

    pub fn fail(description: impl Into<String>, detail: impl Into<String>) -> Self {
        Check {
            passed: false,
            description: description.into(),
            detail: Some(detail.into()),
        }
    }
}


/// Blocks in a CAR, classified by `is_repo_commit` / `is_mst_node` /
/// `is_at_proto_record` (the same logic used by `RepoRecord`).
pub struct ClassifiedBlocks<'a> {
    pub commit: Vec<&'a RepoRecord>,
    pub mst: Vec<&'a RepoRecord>,
    pub record: Vec<&'a RepoRecord>,
    pub unclassified: Vec<&'a RepoRecord>,
}


/// Fields extracted from a commit block.
pub struct CommitInfo {
    pub cid: String,
    pub did: String,
    pub version: i64,
    pub rev: String,
    pub data_cid: Option<CidV1>,
}

impl CommitInfo {
    pub fn data_cid_str(&self) -> String {
        self.data_cid
            .as_ref()
            .map(|c| c.base32.clone())
            .unwrap_or_else(|| "<missing>".to_string())
    }
}


/// Classify the blocks in a CAR by inspecting each `RepoRecord`.
pub fn classify_blocks(records: &[RepoRecord]) -> ClassifiedBlocks<'_> {
    let mut classified = ClassifiedBlocks {
        commit: Vec::new(),
        mst: Vec::new(),
        record: Vec::new(),
        unclassified: Vec::new(),
    };
    for record in records {
        if record.is_repo_commit() {
            classified.commit.push(record);
        } else if record.is_mst_node() {
            classified.mst.push(record);
        } else if record.is_at_proto_record() {
            classified.record.push(record);
        } else {
            classified.unclassified.push(record);
        }
    }
    classified
}


/// Push the standard CAR header check: version is 1.
pub fn check_car_header_version(header: &RepoHeader, checks: &mut Vec<Check>) {
    if header.version == 1 {
        checks.push(Check::pass("CAR header version is 1"));
    } else {
        checks.push(Check::fail(
            "CAR header version is 1",
            format!("Got version {}", header.version),
        ));
    }
}


/// Push the standard "exactly 1 commit block" + "all blocks classifiable" checks.
pub fn check_block_classification(classified: &ClassifiedBlocks<'_>, checks: &mut Vec<Check>) {
    if classified.commit.len() == 1 {
        checks.push(Check::pass("CAR contains exactly 1 commit block"));
    } else {
        checks.push(Check::fail(
            "CAR contains exactly 1 commit block",
            format!("Found {} commit blocks", classified.commit.len()),
        ));
    }

    if classified.unclassified.is_empty() {
        checks.push(Check::pass(
            "All blocks are classifiable (commit, MST, or record)",
        ));
    } else {
        checks.push(Check::fail(
            "All blocks are classifiable (commit, MST, or record)",
            format!("{} unclassified blocks", classified.unclassified.len()),
        ));
    }
}


/// Verify a commit block:
///   - root CID matches commit CID
///   - commit CID integrity (SHA-256 of DAG-CBOR)
///   - required fields present (did, version, rev, data, sig)
///   - version is 3
///   - (optional) DID matches `expected_did`
/// Returns extracted commit fields.
pub fn verify_commit_block(
    commit_record: &RepoRecord,
    root_cid_base32: &str,
    expected_did: Option<&str>,
    checks: &mut Vec<Check>,
) -> CommitInfo {
    let commit_cid_base32 = commit_record.cid.base32.clone();

    // --- CHECK: Root CID matches commit CID ---
    if root_cid_base32 == commit_cid_base32 {
        checks.push(Check::pass("CAR root CID matches commit block CID"));
    } else {
        checks.push(Check::fail(
            "CAR root CID matches commit block CID",
            format!("Root={}, Commit={}", root_cid_base32, commit_cid_base32),
        ));
    }

    // --- CHECK: Commit CID integrity ---
    verify_block_cid(commit_record, checks, "Commit");

    // --- CHECK: Commit has required fields ---
    let commit = &commit_record.data_block;
    let commit_did = commit.select_string(&["did"]).unwrap_or_default();
    let commit_version = commit.select_int(&["version"]).unwrap_or(-1);
    let commit_rev = commit.select_string(&["rev"]).unwrap_or_default();
    let commit_data_cid = commit.select_cid(&["data"]).cloned();

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

    // --- CHECK: Commit version is 3 ---
    if commit_version == 3 {
        checks.push(Check::pass("Commit version is 3"));
    } else {
        checks.push(Check::fail(
            "Commit version is 3",
            format!("Got {}", commit_version),
        ));
    }

    // --- CHECK: Commit DID matches expected (if provided) ---
    if let Some(expected) = expected_did {
        if commit_did == expected {
            checks.push(Check::pass("Commit DID matches expected actor DID"));
        } else {
            checks.push(Check::fail(
                "Commit DID matches expected actor DID",
                format!("Expected={}, Got={}", expected, commit_did),
            ));
        }
    }

    CommitInfo {
        cid: commit_cid_base32,
        did: commit_did,
        version: commit_version,
        rev: commit_rev,
        data_cid: commit_data_cid,
    }
}


/// Verify that a block's CID matches the SHA-256 hash of its DAG-CBOR serialization.
pub fn verify_block_cid(record: &RepoRecord, checks: &mut Vec<Check>, label: &str) {
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


/// Print a checks list and a pass/fail summary line to the logger.
/// Returns true if every check passed.
pub fn log_checks_summary(checks: &[Check]) -> bool {
    let log = logger();
    let mut all_passed = true;
    for check in checks {
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
        log.info(&format!("ALL {} CHECKS PASSED", checks.len()));
    } else {
        let failed = checks.iter().filter(|c| !c.passed).count();
        log.error(&format!("{} of {} checks FAILED", failed, checks.len()));
    }
    all_passed
}
