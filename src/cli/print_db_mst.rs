

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;
use crate::repo::{RepoMst, DagCborObject, CidV1, MstNodeKey};
use crate::mst::{Mst};
use super::hex_encode;

pub fn cmd_print_db_mst(args: &HashMap<String, String>) {
    use crate::mst::{MstItem, MstNode};
    use crate::pds::db::PdsDb;

    let log = logger();

    // Get arguments
    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command PrintDbMst /dataDir <path> [/format tree]");
            return;
        }
    };

    let format = get_arg(args, "format").unwrap_or("tree");

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

    // Get repo commit
    let repo_commit = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit: {}", e));
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
    let all_nodes = mst.find_all_nodes();

    // Convert all MST nodes to DAG-CBOR
    let mst_cache = match RepoMst::convert_mst_to_dag_cbor(&mst) {
        Ok(cache) => cache,
        Err(e) => {
            log.error(&format!("Failed to convert MST to DAG-CBOR: {}", e));
            return;
        }
    };

    // Compute stats
    let mut mst_entry_count = 0;
    for node in &all_nodes {
        mst_entry_count += node.entries.len();
    }

    log.info("");
    log.info("=== PRINT DB MST ===");
    log.info("");
    log.info(&format!("Commit CID:        {}", repo_commit.cid));
    log.info(&format!("Root MST Node CID: {}", repo_commit.root_mst_node_cid));
    log.info(&format!("Rev:               {}", repo_commit.rev));
    log.info(&format!("mst_items.len():   {}", mst_items.len()));
    log.info(&format!("all_nodes.len():   {}", all_nodes.len()));
    log.info(&format!("mst_entry_count:   {}", mst_entry_count));
    log.info(&format!("root depth:        {}", mst.root.key_depth));
    log.info("");

    // Print based on format
    match format.to_lowercase().as_str() {
        "tree" => {
            fn print_tree(
                log: &crate::log::Logger,
                mst_cache: &HashMap<MstNodeKey, (CidV1, DagCborObject)>,
                node: &MstNode,
                indent: usize,
                direction: &str,
            ) {
                let indent_str = " ".repeat(indent);
                let node_key = MstNodeKey::from_node(node);

                if let Some((cid, dag_cbor)) = mst_cache.get(&node_key) {
                    log.info(&format!(
                        "{}[{}] [depth={}] {}",
                        indent_str, direction, node.key_depth, cid.base32
                    ));

                    if let Ok(node_bytes) = dag_cbor.to_bytes() {
                        log.info(&format!("{}  Hex: {}", indent_str, hex_encode(&node_bytes)));
                    }

                    log.info(&format!("{}  DAG-CBOR:", indent_str));
                    log.info(&dag_cbor.get_recursive_debug_string((indent / 2) + 2));

                    for entry in &node.entries {
                        log.info(&format!("{}  {}: {}", indent_str, entry.key, entry.value));
                    }

                    log.info("");
                } else {
                    log.info(&format!(
                        "{}[{}] [depth={}] [NOT IN CACHE]",
                        indent_str, direction, node.key_depth
                    ));
                }

                if let Some(ref left) = node.left_tree {
                    print_tree(log, mst_cache, left, indent + 2, "left");
                }

                for entry in &node.entries {
                    if let Some(ref right) = entry.right_tree {
                        print_tree(log, mst_cache, right, indent + 2, "right");
                    }
                }
            }

            print_tree(log, &mst_cache, &mst.root, 0, "root");
        }
        _ => {
            log.error(&format!("Unknown format: {}. Use 'tree'.", format));
        }
    }
}

