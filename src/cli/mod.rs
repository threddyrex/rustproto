
pub mod backup_account;
pub mod create_session;
pub mod get_pds_info;
pub mod get_plc_history;
pub mod inspect_firehose_event;
pub mod install_config;
pub mod install_db;
pub mod print_db_mst;
pub mod repair_commit;
pub mod resolve_actor;
pub mod run_pds;
pub mod start_firehose_consumer;
pub mod sync_get_record_local;
pub mod sync_repo;
pub mod test_apply_writes_and_log_firehose;
pub mod walk_mst;

use std::collections::HashMap;
use crate::repo::{CidV1, DagCborObject};


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

