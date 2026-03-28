

use std::collections::HashMap;
use super::{build_commit_dag_cbor_local, hex_encode};
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;
use crate::mst::{Mst, MstItem};
use crate::pds::db::PdsDb;
use crate::repo::{CidV1, DagCborObject, MstNodeKey, RepoMst};


/// Get a record directly from the local pds.db and print its details.
pub fn cmd_sync_get_record_local(args: &HashMap<String, String>) {

    let log = logger();

    // Get required arguments
    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command SyncGetRecordLocal /dataDir <path> /collection <nsid> /rkey <rkey> [/format dagcbor|json|raw]");
            return;
        }
    };

    let collection = match get_arg(args, "collection") {
        Some(c) => c,
        None => {
            log.error("missing /collection argument");
            log.error("Usage: rustproto /command SyncGetRecordLocal /dataDir <path> /collection <nsid> /rkey <rkey> [/format dagcbor|json|raw]");
            return;
        }
    };

    let rkey = match get_arg(args, "rkey") {
        Some(r) => r,
        None => {
            log.error("missing /rkey argument");
            log.error("Usage: rustproto /command SyncGetRecordLocal /dataDir <path> /collection <nsid> /rkey <rkey> [/format dagcbor|json|raw]");
            return;
        }
    };

    let format = get_arg(args, "format").unwrap_or("dagcbor");
    let full_key = format!("{}/{}", collection, rkey);

    // Initialize file system
    let lfs = match LocalFileSystem::initialize(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };

    // Connect to database
    let db = match PdsDb::connect(&lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to connect to PDS database: {}", e));
            return;
        }
    };

    // Get repo header and commit
    let repo_header = match db.get_repo_header() {
        Ok(h) => h,
        Err(e) => {
            log.error(&format!("Failed to get repo header: {}", e));
            return;
        }
    };

    let repo_commit = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit: {}", e));
            return;
        }
    };

    // Get record
    let record = match db.get_repo_record(collection, rkey) {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("Record not found: {}/{}", collection, rkey));
            log.trace(&format!("Error: {}", e));
            return;
        }
    };

    // Build MST from all records
    let all_records = match db.get_all_repo_records() {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("Failed to get all records: {}", e));
            return;
        }
    };

    let mst_items: Vec<MstItem> = all_records
        .iter()
        .map(|r| MstItem::new(&format!("{}/{}", r.collection, r.rkey), &r.cid))
        .collect();

    let mst = Mst::assemble_tree_from_items(&mst_items);

    // Find nodes on the path to the record (proof chain)
    let proof_nodes = mst.find_nodes_for_key(&full_key);

    // Convert ALL MST nodes to DAG-CBOR first (so CIDs are computed correctly)
    let mst_cache = match RepoMst::convert_mst_to_dag_cbor(&mst) {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to convert MST: {}", e));
            return;
        }
    };

    // Get user DID for AT URI
    let user_did = db.get_config_property("UserDid").unwrap_or_else(|_| "<unknown>".to_string());

    // Parse record DAG-CBOR
    let record_dag_cbor = match DagCborObject::from_bytes(&record.dag_cbor_bytes) {
        Ok(obj) => obj,
        Err(e) => {
            log.error(&format!("Failed to parse record DAG-CBOR: {}", e));
            return;
        }
    };

    let at_proto_type = record_dag_cbor.select_string(&["$type"]).unwrap_or_else(|| "<null>".to_string());

    // Print based on format
    match format.to_lowercase().as_str() {
        "dagcbor" => {
            log.info("");
            log.info("=== SYNC GET RECORD (DAG-CBOR FORMAT) ===");
            log.info(&format!("AT URI: at://{}/{}/{}", user_did, collection, rkey));
            log.info("");

            // CAR Header
            log.info("--- BLOCK 1: CAR HEADER ---");
            let mut header_map: std::collections::HashMap<String, DagCborObject> = std::collections::HashMap::new();
            header_map.insert("version".to_string(), DagCborObject::new_unsigned_int(1));
            if let Ok(root_cid) = CidV1::from_base32(&repo_header.repo_commit_cid) {
                header_map.insert("roots".to_string(), DagCborObject::new_array(vec![
                    DagCborObject::new_cid(root_cid),
                ]));
            }
            let header_dag_cbor = DagCborObject::new_map(header_map);
            if let Ok(header_bytes) = header_dag_cbor.to_bytes() {
                log.info(&format!("CID:    {} (root reference)", repo_header.repo_commit_cid));
                log.info(&format!("Length: {} bytes", header_bytes.len()));
                log.info(&format!("Hex:    {}", hex_encode(&header_bytes)));
            }
            log.info("");

            // Repo Commit
            log.info("--- BLOCK 2: REPO COMMIT ---");
            if let Ok(commit_dag_cbor) = build_commit_dag_cbor_local(&db, &repo_commit) {
                if let Ok(commit_bytes) = commit_dag_cbor.to_bytes() {
                    log.info(&format!("CID:    {}", repo_commit.cid));
                    log.info(&format!("Length: {} bytes", commit_bytes.len()));
                    log.info(&format!("Hex:    {}", hex_encode(&commit_bytes)));
                }
            }
            log.info("");

            // MST Nodes (proof chain)
            let block_start = 3;
            log.info(&format!("--- BLOCKS {}-{}: MST NODES (PROOF CHAIN) ---", block_start, block_start + proof_nodes.len() - 1));
            log.info(&format!("Total MST nodes in proof chain: {}", proof_nodes.len()));
            log.info("");

            let mut block_num = block_start;
            for node in &proof_nodes {
                let node_key = MstNodeKey::from_node(node);
                if let Some((cid, dag_cbor)) = mst_cache.get(&node_key) {
                    if let Ok(node_bytes) = dag_cbor.to_bytes() {
                        log.info(&format!("  BLOCK {}: MST NODE", block_num));
                        log.info(&format!("  CID:    {}", cid.base32));
                        log.info(&format!("  Length: {} bytes", node_bytes.len()));
                        log.info(&format!("  Hex:    {}", hex_encode(&node_bytes)));
                        log.info("");
                    }
                }
                block_num += 1;
            }

            // Record
            log.info(&format!("--- BLOCK {}: RECORD ---", block_num));
            log.info(&format!("CID:    {}", record.cid));
            log.info(&format!("$type:  {}", at_proto_type));
            log.info(&format!("Length: {} bytes", record.dag_cbor_bytes.len()));
            log.info(&format!("Hex:    {}", hex_encode(&record.dag_cbor_bytes)));
        }
        "json" => {
            log.info("");
            log.info("=== SYNC GET RECORD (JSON FORMAT) ===");
            log.info(&format!("AT URI: at://{}/{}/{}", user_did, collection, rkey));
            log.info("");

            // CAR Header
            log.info("--- CAR HEADER ---");
            let mut header_map: std::collections::HashMap<String, DagCborObject> = std::collections::HashMap::new();
            header_map.insert("version".to_string(), DagCborObject::new_unsigned_int(1));
            if let Ok(root_cid) = CidV1::from_base32(&repo_header.repo_commit_cid) {
                header_map.insert("roots".to_string(), DagCborObject::new_array(vec![
                    DagCborObject::new_cid(root_cid),
                ]));
            }
            let header_dag_cbor = DagCborObject::new_map(header_map);
            log.info(&header_dag_cbor.to_json_string());
            log.info("");

            // Repo Commit
            log.info("--- REPO COMMIT ---");
            if let Ok(commit_dag_cbor) = build_commit_dag_cbor_local(&db, &repo_commit) {
                log.info(&commit_dag_cbor.to_json_string());
            }
            log.info("");

            // MST Nodes
            log.info(&format!("--- MST NODES (PROOF CHAIN: {} nodes) ---", proof_nodes.len()));
            let mut node_num = 1;
            for node in &proof_nodes {
                let node_key = MstNodeKey::from_node(node);
                if let Some((cid, dag_cbor)) = mst_cache.get(&node_key) {
                    log.info(&format!("MST NODE {} (CID: {}):", node_num, cid.base32));
                    log.info(&dag_cbor.to_json_string());
                    log.info("");
                }
                node_num += 1;
            }

            // Record
            log.info("--- RECORD ---");
            log.info(&format!("CID:   {}", record.cid));
            log.info(&format!("$type: {}", at_proto_type));
            log.info(&record_dag_cbor.to_json_string());
        }
        "raw" => {
            log.info("");
            log.info("=== SYNC GET RECORD (RAW FORMAT) ===");
            log.info(&format!("AT URI: at://{}/{}/{}", user_did, collection, rkey));
            log.info("");

            log.info(&format!("Record CID:        {}", record.cid));
            log.info(&format!("$type:             {}", at_proto_type));
            log.info(&format!("Commit CID:        {}", repo_commit.cid));
            log.info(&format!("Root MST Node CID: {}", repo_commit.root_mst_node_cid));
            log.info(&format!("MST Proof Chain:   {} nodes", proof_nodes.len()));
            log.info("");

            log.info(&format!("Record Length: {} bytes", record.dag_cbor_bytes.len()));
            log.info(&format!("Record Hex:    {}", hex_encode(&record.dag_cbor_bytes)));
        }
        "tree" => {
            log.info("");
            log.info("=== SYNC GET RECORD (TREE FORMAT) ===");
            log.info(&format!("AT URI: at://{}/{}/{}", user_did, collection, rkey));
            log.info("");

            // Repo Commit
            log.info("--- REPO COMMIT ---");
            log.info(&format!("CID:              {}", repo_commit.cid));
            log.info(&format!("Root MST Node:    {}", repo_commit.root_mst_node_cid));
            log.info(&format!("Rev:              {}", repo_commit.rev));
            log.info(&format!("Version:          {}", repo_commit.version));
            log.info("");

            // MST Proof Chain
            log.info("--- MST PROOF CHAIN ---");
            log.info(&format!("Total nodes in proof: {}", proof_nodes.len()));
            log.info("");

            for (node_idx, node) in proof_nodes.iter().enumerate() {
                let node_key = MstNodeKey::from_node(node);
                if let Some((cid, dag_cbor)) = mst_cache.get(&node_key) {
                    if let Ok(node_bytes) = dag_cbor.to_bytes() {
                        log.info(&format!("NODE {} (depth={})", node_idx, node.key_depth));
                        log.info(&format!("  CID: {}", cid.base32));
                        log.info(&format!("  Hex: {}", hex_encode(&node_bytes)));
                        log.info("  DAG-CBOR:");
                        log.info(&dag_cbor.get_recursive_debug_string(2));
                        log.info("");
                    }
                } else {
                    log.info(&format!("[NODE {} NOT IN CACHE]", node_idx));
                }
            }

            // Target Record
            log.info("--- TARGET RECORD ---");
            log.info(&format!("Key:   {}/{}", collection, rkey));
            log.info(&format!("CID:   {}", record.cid));
            log.info(&format!("$type: {}", at_proto_type));
            log.info(&format!("Hex:   {}", hex_encode(&record.dag_cbor_bytes)));
        }
        _ => {
            log.error(&format!("Unknown format: {}. Use 'dagcbor', 'json', 'raw', or 'tree'.", format));
        }
    }
}


