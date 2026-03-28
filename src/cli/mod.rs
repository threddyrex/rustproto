
pub mod backup_account;
pub mod create_session;
pub mod get_pds_info;
pub mod get_plc_history;
pub mod get_post;
pub mod inspect_firehose_event;
pub mod install_config;
pub mod install_db;
pub mod print_db_mst;
pub mod print_repo_records;
pub mod print_repo_stats;
pub mod repair_commit;
pub mod resolve_actor;
pub mod run_pds;
pub mod start_firehose_consumer;
pub mod sync_get_record_local;
pub mod sync_repo;
pub mod test_apply_writes_and_log_firehose;
pub mod walk_mst;

use std::collections::HashMap;
use crate::fs::{LocalFileSystem};
use crate::log::{logger};
use crate::repo::{CidV1, DagCborObject};
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;


/// Gets an argument value or returns None.
pub fn get_arg<'a>(args: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    args.get(&key.to_lowercase()).map(|s| s.as_str())
}


/// Parses command line arguments in the format `/name1 value1 /name2 value2`.
pub fn parse_arguments(args: &[String]) -> Result<HashMap<String, String>, String> {
    if args.len() % 2 != 0 {
        return Err("Arguments must be in the format '/name1 value1 /name2 value2'".to_string());
    }

    let mut arguments = HashMap::new();

    for chunk in args.chunks(2) {
        let key = &chunk[0];
        let value = &chunk[1];

        if !key.starts_with('/') {
            return Err(format!(
                "Argument name must start with '/': {}",
                key
            ));
        }

        let key_name = key[1..].to_lowercase();
        arguments.insert(key_name, value.clone());
    }

    Ok(arguments)
}

/// Convert bytes to hex string for debugging.
pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}



/// Build commit DAG-CBOR object for local display.
pub fn build_commit_dag_cbor_local(db: &crate::pds::db::PdsDb, commit: &crate::pds::db::DbRepoCommit) -> Result<DagCborObject, String> {
    let user_did = db.get_config_property("UserDid")
        .map_err(|e| format!("Failed to get UserDid: {}", e))?;

    let root_cid = CidV1::from_base32(&commit.root_mst_node_cid)
        .map_err(|e| format!("Invalid root CID: {}", e))?;

    let mut commit_map: std::collections::HashMap<String, DagCborObject> = std::collections::HashMap::new();
    commit_map.insert("did".to_string(), DagCborObject::new_text(user_did));
    commit_map.insert("version".to_string(), DagCborObject::new_unsigned_int(commit.version as i64));
    commit_map.insert("data".to_string(), DagCborObject::new_cid(root_cid));
    commit_map.insert("rev".to_string(), DagCborObject::new_text(commit.rev.clone()));

    if let Some(ref prev_cid_str) = commit.prev_mst_node_cid {
        if let Ok(prev_cid) = CidV1::from_base32(prev_cid_str) {
            commit_map.insert("prev".to_string(), DagCborObject::new_cid(prev_cid));
        } else {
            commit_map.insert("prev".to_string(), DagCborObject::new_null());
        }
    } else {
        commit_map.insert("prev".to_string(), DagCborObject::new_null());
    }

    commit_map.insert("sig".to_string(), DagCborObject::new_byte_string(commit.signature.clone()));

    Ok(DagCborObject::new_map(commit_map))
}


/// Convert an AT URI to a bsky.app URL.
pub fn at_uri_to_bsky_url(at_uri: &str) -> Option<String> {
    // Format: at://did:plc:xxx/app.bsky.feed.post/rkey
    if !at_uri.starts_with("at://") {
        return None;
    }

    let rest = at_uri.strip_prefix("at://")?;
    let parts: Vec<&str> = rest.split('/').collect();

    if parts.len() >= 3 && parts[1] == "app.bsky.feed.post" {
        let did = parts[0];
        let rkey = parts[2];
        Some(format!("https://bsky.app/profile/{}/post/{}", did, rkey))
    } else {
        None
    }
}


/// Resolves the repo file path from arguments.
/// Supports either /repoFile directly or /actor + /dataDir combination.
/// If actor is not cached, resolves online via BlueskyClient.
pub async fn resolve_repo_file(args: &HashMap<String, String>) -> Option<std::path::PathBuf> {
    // Check for direct repoFile argument
    if let Some(repo_file) = get_arg(args, "repofile") {
        let path = std::path::PathBuf::from(repo_file);
        if path.exists() {
            return Some(path);
        } else {
            logger().error(&format!("Repo file does not exist: {}", repo_file));
            return None;
        }
    }

    // Try to resolve from actor + dataDir
    let actor = get_arg(args, "actor")?;
    let data_dir = get_arg(args, "datadir")?;

    let lfs = match LocalFileSystem::initialize(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            logger().error(&format!("Error initializing data directory: {}", e));
            return None;
        }
    };

    // Resolve actor to DID
    let did = if actor.starts_with("did:") {
        // Already a DID
        actor.to_string()
    } else {
        // Try to resolve handle from cached actor info (falls back to online)
        match lfs.resolve_actor_info(actor, None, DEFAULT_APP_VIEW_HOST_NAME).await {
            Ok(info) => {
                match info.did {
                    Some(d) => d,
                    None => {
                        logger().error("Resolved actor info does not contain a DID");
                        return None;
                    }
                }
            }
            Err(e) => {
                logger().error(&format!("Could not resolve actor: {}", e));
                return None;
            }
        }
    };
    
    match lfs.get_path_repo_file(&did) {
        Ok(path) => {
            if path.exists() {
                Some(path)
            } else {
                logger().error(&format!("Repo file does not exist: {}", path.display()));
                None
            }
        }
        Err(e) => {
            logger().error(&format!("Error getting repo file path: {}", e));
            None
        }
    }
}

