
use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;
use crate::repo::{RepoMst, DagCborObject, CidV1, MstNodeKey};
use crate::mst::{Mst, MstNode};

pub async fn cmd_walk_mst(args: &HashMap<String, String>) {

    let log = logger();

    // Get arguments
    let actor = get_arg(args, "actor");
    let repo_file_arg = get_arg(args, "repofile");

    // Determine repo file path
    let repo_file: String = if let Some(rf) = repo_file_arg {
        rf.to_string()
    } else if let Some(act) = actor {
        let data_dir = match get_arg(args, "datadir") {
            Some(d) => d,
            None => {
                log.error("missing /dataDir argument when using /actor");
                log.error("Usage: rustproto /command WalkMst /actor <handle_or_did> /dataDir <path>");
                log.error("   or: rustproto /command WalkMst /repoFile <path>");
                return;
            }
        };

        let lfs = match LocalFileSystem::initialize(data_dir) {
            Ok(lfs) => lfs,
            Err(e) => {
                log.error(&format!("Error initializing data directory: {}", e));
                return;
            }
        };

        // Resolve actor to get DID
        let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);
        let info = match client.resolve_actor_info(act, None).await {
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

        match lfs.get_path_repo_file(&did) {
            Ok(path) => path.to_string_lossy().to_string(),
            Err(e) => {
                log.error(&format!("Error getting repo path for actor: {}", e));
                return;
            }
        }
    } else {
        log.error("missing /actor or /repoFile argument");
        log.error("Usage: rustproto /command WalkMst /actor <handle_or_did> /dataDir <path>");
        log.error("   or: rustproto /command WalkMst /repoFile <path>");
        return;
    };

    // Check if file exists
    if !std::path::Path::new(&repo_file).exists() {
        log.error(&format!("Repo file does not exist: {}", repo_file));
        return;
    }

    // Load MST items from repo
    log.info(&format!("Loading MST from: {}", repo_file));
    let mst_items = match RepoMst::load_mst_items_from_repo_file(&repo_file, log) {
        Ok(items) => items,
        Err(e) => {
            log.error(&format!("Error loading MST items: {}", e));
            return;
        }
    };

    // Assemble tree
    let mst = Mst::assemble_tree_from_items(&mst_items);
    let all_mst_nodes = mst.find_all_nodes();

    // Convert to DAG-CBOR and cache CIDs
    let mst_node_cache = match RepoMst::convert_mst_to_dag_cbor(&mst) {
        Ok(cache) => cache,
        Err(e) => {
            log.error(&format!("Error converting MST to DAG-CBOR: {}", e));
            return;
        }
    };

    // Compute stats
    let mut mst_entry_count = 0;
    for node in &all_mst_nodes {
        mst_entry_count += node.entries.len();
    }

    // Print stats
    log.info("");
    log.info(&format!("mst_items.len(): {}", mst_items.len()));
    log.info(&format!("all_mst_nodes.len(): {}", all_mst_nodes.len()));
    log.info(&format!("mst_node_cache.len(): {}", mst_node_cache.len()));
    log.info(&format!("mst_entry_count: {}", mst_entry_count));
    log.info(&format!("root depth: {}", mst.root.key_depth));
    log.info("");

    // Walk and print tree structure
    fn visit_node(
        log: &crate::log::Logger,
        mst_node_cache: &HashMap<MstNodeKey, (CidV1, DagCborObject)>,
        node: &MstNode,
        indent: usize,
        direction: &str,
    ) {
        let indent_str = " ".repeat(indent);
        let node_key = MstNodeKey::from_node(node);
        
        let cid_str = mst_node_cache
            .get(&node_key)
            .map(|(cid, _)| cid.get_base32().to_string())
            .unwrap_or_else(|| "<unknown>".to_string());

        log.trace(&format!(
            "{} [{}] [{}] {}",
            indent_str, direction, node.key_depth, cid_str
        ));

        for entry in &node.entries {
            log.trace(&format!("{} {}: {}", indent_str, entry.key, entry.value));
        }

        log.trace("");

        if let Some(ref left) = node.left_tree {
            visit_node(log, mst_node_cache, left, indent + 2, "left");
            log.trace("");
        }

        for entry in &node.entries {
            if let Some(ref right) = entry.right_tree {
                visit_node(log, mst_node_cache, right, indent + 2, "right");
            }
        }
    }

    log.trace("");
    visit_node(log, &mst_node_cache, &mst.root, 0, "root");
    log.trace("");
}

