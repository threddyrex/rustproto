//! com.atproto.sync.getBlocks endpoint.
//!
//! Returns the repository header and commit plus data blocks selected by CID.
//! The converted MST is cached in memory until the repository commit changes.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::{
    Json,
    body::Body,
    extract::{ConnectInfo, RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;

use super::get_caller_info;
use crate::mst::{Mst, MstItem};
use crate::pds::db::{DbRepoCommit, StatisticKey};
use crate::pds::server::PdsState;
use crate::pds::user_repo::REPO_LOCK;
use crate::repo::{CidV1, DagCborObject, MstNodeKey, RepoHeader, RepoMst, VarInt};

/// Query parameters for getBlocks.
pub struct GetBlocksQuery {
    /// Repository DID (required).
    did: Option<String>,
    /// CIDs of blocks to return (required).
    cids: Option<Vec<String>>,
}

/// Error response for getBlocks.
#[derive(Serialize)]
pub struct GetBlocksError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.sync.getBlocks - Return repository blocks as a CAR file.
pub async fn sync_get_blocks(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.getBlocks".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    let query = parse_get_blocks_query(raw_query.as_deref());
    let did = match query.did.as_deref() {
        Some(did) if !did.is_empty() => did,
        _ => return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", "Missing did"),
    };

    let cids = match query.cids {
        Some(cids) => cids,
        None => return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", "Missing cids"),
    };

    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                &format!("Failed to get user DID: {}", e),
            );
        }
    };
    if !did.eq_ignore_ascii_case(&user_did) {
        return error_response(StatusCode::NOT_FOUND, "RepoNotFound", "Repo not found");
    }

    let mut parsed_cids = Vec::with_capacity(cids.len());
    for cid in &cids {
        match CidV1::from_base32(cid) {
            Ok(cid) => parsed_cids.push(cid),
            Err(e) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    &format!("Invalid CID '{}': {}", cid, e),
                );
            }
        }
    }

    match build_blocks_car(&state, &parsed_cids) {
        Ok(car_bytes) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/vnd.ipld.car")
            .body(Body::from(car_bytes))
            .unwrap_or_else(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to build response",
                )
                    .into_response()
            }),
        Err(BlocksCarError::Missing(missing)) => error_response(
            StatusCode::BAD_REQUEST,
            "BlockNotFound",
            &format!("Could not find CIDs: {}", missing.join(", ")),
        ),
        Err(BlocksCarError::Internal(message)) => {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", &message)
        }
    }
}

#[derive(Debug)]
enum BlocksCarError {
    Missing(Vec<String>),
    Internal(String),
}

type MstCache = HashMap<MstNodeKey, (CidV1, DagCborObject)>;

struct CachedMst {
    commit_cid: String,
    nodes: Arc<MstCache>,
}

/// Build a CAR containing the header, commit, and requested data blocks.
fn build_blocks_car(state: &PdsState, requested_cids: &[CidV1]) -> Result<Vec<u8>, BlocksCarError> {
    static MST_CACHE: Mutex<Option<CachedMst>> = Mutex::new(None);

    build_blocks_car_with_cache(state, requested_cids, &MST_CACHE)
}

fn build_blocks_car_with_cache(
    state: &PdsState,
    requested_cids: &[CidV1],
    cache: &Mutex<Option<CachedMst>>,
) -> Result<Vec<u8>, BlocksCarError> {
    let requested: HashSet<&str> = requested_cids
        .iter()
        .map(|cid| cid.base32.as_str())
        .collect();
    let mut blocks: HashMap<String, Vec<u8>> = HashMap::new();

    let (repo_commit, all_records, mst_cache) = {
        let mut cache = cache
            .lock()
            .map_err(|e| BlocksCarError::Internal(format!("Failed to lock MST cache: {}", e)))?;
        // Writes change records before publishing the commit. Exclude them while
        // loading records and checking or rebuilding the MST.
        let _repo_lock = REPO_LOCK
            .lock()
            .map_err(|e| BlocksCarError::Internal(format!("Failed to lock repository: {}", e)))?;
        let repo_commit = state
            .db
            .get_repo_commit()
            .map_err(|e| BlocksCarError::Internal(format!("Failed to get repo commit: {}", e)))?;
        let all_records = state
            .db
            .get_all_repo_records()
            .map_err(|e| BlocksCarError::Internal(format!("Failed to get records: {}", e)))?;

        let mst_cache = match cache.as_ref() {
            Some(cached) if cached.commit_cid == repo_commit.cid => {
                Arc::clone(&cached.nodes)
            }
            _ => {
                let mst_items: Vec<MstItem> = all_records
                    .iter()
                    .map(|record| {
                        MstItem::new(
                            &format!("{}/{}", record.collection, record.rkey),
                            &record.cid,
                        )
                    })
                    .collect();
                let mst = Mst::assemble_tree_from_items(&mst_items);
                let nodes = Arc::new(RepoMst::convert_mst_to_dag_cbor(&mst).map_err(|e| {
                    BlocksCarError::Internal(format!("Failed to convert MST: {}", e))
                })?);
                *cache = Some(CachedMst {
                    commit_cid: repo_commit.cid.clone(),
                    nodes: Arc::clone(&nodes),
                });
                nodes
            }
        };
        (repo_commit, all_records, mst_cache)
    };

    for (cid, dag_cbor) in mst_cache.values() {
        if requested.contains(cid.base32.as_str()) {
            let bytes = dag_cbor.to_bytes().map_err(|e| {
                BlocksCarError::Internal(format!("Failed to encode MST block: {}", e))
            })?;
            blocks.insert(cid.base32.clone(), bytes);
        }
    }

    for record in all_records {
        if requested.contains(record.cid.as_str()) {
            blocks.insert(record.cid, record.dag_cbor_bytes);
        }
    }

    let commit_cid = CidV1::from_base32(&repo_commit.cid)
        .map_err(|e| BlocksCarError::Internal(format!("Invalid commit CID: {}", e)))?;
    let commit = build_commit_dag_cbor(state, &repo_commit)?;
    let bytes = commit
        .to_bytes()
        .map_err(|e| BlocksCarError::Internal(format!("Failed to encode commit block: {}", e)))?;
    blocks.insert(commit_cid.base32.clone(), bytes);

    let missing: Vec<String> = requested_cids
        .iter()
        .filter(|cid| !blocks.contains_key(&cid.base32))
        .map(|cid| cid.base32.clone())
        .collect();
    if !missing.is_empty() {
        return Err(BlocksCarError::Missing(missing));
    }

    let mut car_bytes = Vec::new();
    RepoHeader {
        repo_commit_cid: commit_cid.clone(),
        version: 1,
    }
    .write_to_stream(&mut car_bytes)
    .map_err(|e| BlocksCarError::Internal(format!("Failed to write CAR header: {}", e)))?;

    let mut emitted = HashSet::new();
    for cid in std::iter::once(&commit_cid).chain(requested_cids) {
        if emitted.insert(cid.base32.as_str()) {
            let data = blocks.get(&cid.base32).ok_or_else(|| {
                BlocksCarError::Internal(format!(
                    "Block {} disappeared during CAR generation",
                    cid.base32
                ))
            })?;
            write_car_block(&mut car_bytes, cid, data).map_err(|e| {
                BlocksCarError::Internal(format!("Failed to write CAR block: {}", e))
            })?;
        }
    }

    Ok(car_bytes)
}

/// Build the commit DAG-CBOR object.
fn build_commit_dag_cbor(
    state: &PdsState,
    commit: &DbRepoCommit,
) -> Result<DagCborObject, BlocksCarError> {
    let user_did = state
        .db
        .get_config_property("UserDid")
        .map_err(|e| BlocksCarError::Internal(format!("Failed to get user DID: {}", e)))?;
    let root_cid = CidV1::from_base32(&commit.root_mst_node_cid)
        .map_err(|e| BlocksCarError::Internal(format!("Invalid root CID: {}", e)))?;

    let mut commit_map = HashMap::new();
    commit_map.insert("did".to_string(), DagCborObject::new_text(user_did));
    commit_map.insert(
        "version".to_string(),
        DagCborObject::new_unsigned_int(commit.version as i64),
    );
    commit_map.insert("data".to_string(), DagCborObject::new_cid(root_cid));
    commit_map.insert(
        "rev".to_string(),
        DagCborObject::new_text(commit.rev.clone()),
    );

    if let Some(prev_cid) = &commit.prev_mst_node_cid {
        if let Ok(prev_cid) = CidV1::from_base32(prev_cid) {
            commit_map.insert("prev".to_string(), DagCborObject::new_cid(prev_cid));
        } else {
            commit_map.insert("prev".to_string(), DagCborObject::new_null());
        }
    } else {
        commit_map.insert("prev".to_string(), DagCborObject::new_null());
    }
    commit_map.insert(
        "sig".to_string(),
        DagCborObject::new_byte_string(commit.signature.clone()),
    );

    Ok(DagCborObject::new_map(commit_map))
}

/// Write a serialized block to a CAR stream.
fn write_car_block<W: Write>(writer: &mut W, cid: &CidV1, data: &[u8]) -> std::io::Result<()> {
    let total_length = cid.all_bytes.len() + data.len();
    VarInt::from_long(total_length as i64).write_varint(writer)?;
    writer.write_all(&cid.all_bytes)?;
    writer.write_all(data)
}

fn error_response(status: StatusCode, error: &str, message: &str) -> Response {
    (
        status,
        Json(GetBlocksError {
            error: error.to_string(),
            message: message.to_string(),
        }),
    )
        .into_response()
}

fn parse_get_blocks_query(raw_query: Option<&str>) -> GetBlocksQuery {
    let mut query = GetBlocksQuery {
        did: None,
        cids: None,
    };

    if let Some(raw_query) = raw_query {
        for (key, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
            match key.as_ref() {
                "did" => query.did = Some(value.into_owned()),
                "cids" => query
                    .cids
                    .get_or_insert_with(Vec::new)
                    .push(value.into_owned()),
                _ => {}
            }
        }
    }

    query
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::LocalFileSystem;
    use crate::log::Logger;
    use crate::pds::db::PdsDb;
    use crate::pds::installer::Installer;
    use crate::repo::RepoRecord;
    use std::io::Cursor;
    use std::sync::LazyLock;

    fn publish_commit(state: &PdsState, rev: &str) -> DbRepoCommit {
        let records = state.db.get_all_repo_records().unwrap();
        let items: Vec<_> = records
            .iter()
            .map(|r| MstItem::new(format!("{}/{}", r.collection, r.rkey), &r.cid))
            .collect();
        let mst = Mst::assemble_tree_from_items(&items);
        let nodes = RepoMst::convert_mst_to_dag_cbor(&mst).unwrap();
        let root_cid = &nodes.get(&MstNodeKey::from_node(&mst.root)).unwrap().0;
        let mut commit = DbRepoCommit {
            version: 3,
            cid: String::new(),
            root_mst_node_cid: root_cid.base32.clone(),
            rev: rev.to_string(),
            prev_mst_node_cid: None,
            signature: vec![1; 64],
        };
        commit.cid =
            CidV1::compute_cid_for_dag_cbor(&build_commit_dag_cbor(state, &commit).unwrap())
                .unwrap()
                .base32;
        state.db.insert_update_repo_commit(&commit).unwrap();
        commit
    }

    #[test]
    fn car_always_includes_header_and_commit_and_filters_data_blocks() {
        static LOG: LazyLock<Logger> = LazyLock::new(Logger::default_logger);
        let dir = tempfile::tempdir().unwrap();
        let lfs = LocalFileSystem::initialize_with_create(dir.path()).unwrap();
        Installer::install_db(&lfs, &LOG, false).unwrap();
        let db = PdsDb::connect(&lfs).unwrap();
        db.set_config_property("UserDid", "did:plc:example")
            .unwrap();

        let record_bytes = DagCborObject::new_text("requested record".to_string())
            .to_bytes()
            .unwrap();
        let record_cid = CidV1::compute_cid_for_dag_cbor_bytes(&record_bytes).unwrap();
        db.insert_repo_record(
            "app.bsky.feed.post",
            "one",
            &record_cid.base32,
            &record_bytes,
        )
        .unwrap();
        let other_bytes = DagCborObject::new_text("unrequested record".to_string())
            .to_bytes()
            .unwrap();
        let other_cid = CidV1::compute_cid_for_dag_cbor_bytes(&other_bytes).unwrap();
        db.insert_repo_record("app.bsky.feed.post", "two", &other_cid.base32, &other_bytes)
            .unwrap();

        let state = PdsState { log: &LOG, lfs, db };
        let commit = publish_commit(&state, "3testrevision");
        let commit_cid = CidV1::from_base32(&commit.cid).unwrap();
        let mst_cid = CidV1::from_base32(&commit.root_mst_node_cid).unwrap();

        for requested in [
            vec![],
            vec![record_cid.clone()],
            vec![mst_cid.clone()],
            vec![
                record_cid.clone(),
                mst_cid.clone(),
                commit_cid.clone(),
                record_cid.clone(),
            ],
        ] {
            let bytes = build_blocks_car(&state, &requested).unwrap();
            let mut reader = Cursor::new(&bytes);
            let header = RepoHeader::read_from_stream(&mut reader).unwrap();
            assert_eq!(header.version, 1);
            assert_eq!(header.repo_commit_cid, commit_cid);
            let first = RepoRecord::read_from_stream(&mut reader).unwrap();
            assert!(first.is_repo_commit());
            assert_eq!(first.cid, commit_cid);
            let mut returned = HashSet::from([first.cid.base32]);
            while reader.position() < bytes.len() as u64 {
                let block = RepoRecord::read_from_stream(&mut reader).unwrap();
                assert!(returned.insert(block.cid.base32.clone()), "duplicate block");
                if block.cid == record_cid {
                    assert_eq!(block.data_block.to_bytes().unwrap(), record_bytes);
                }
            }
            let expected: HashSet<String> = std::iter::once(&commit_cid)
                .chain(&requested)
                .map(|cid| cid.base32.clone())
                .collect();
            assert_eq!(returned, expected);
        }

        let missing = CidV1::compute_cid_for_dag_cbor(&DagCborObject::new_null()).unwrap();
        assert!(matches!(
            build_blocks_car(&state, &[missing.clone()]),
            Err(BlocksCarError::Missing(cids)) if cids == vec![missing.base32]
        ));
    }

    #[test]
    fn cache_reuses_only_mst_and_refreshes_after_create_update_delete() {
        static LOG: LazyLock<Logger> = LazyLock::new(Logger::default_logger);
        let dir = tempfile::tempdir().unwrap();
        let lfs = LocalFileSystem::initialize_with_create(dir.path()).unwrap();
        Installer::install_db(&lfs, &LOG, false).unwrap();
        let db = PdsDb::connect(&lfs).unwrap();
        db.set_config_property("UserDid", "did:plc:blockscache")
            .unwrap();
        let state = PdsState { log: &LOG, lfs, db };
        let cache = Mutex::new(None);
        let mut previous: Option<Arc<MstCache>> = None;
        let mut previous_record: Option<CidV1> = None;

        for (rev, value) in [
            ("3blocks1", Some("created")),
            ("3blocks2", Some("updated")),
            ("3blocks3", None),
        ] {
            let (commit, record_cid) = {
                let _lock = REPO_LOCK.lock().unwrap();
                if previous_record.is_some() {
                    state
                        .db
                        .delete_repo_record("app.bsky.feed.post", "one")
                        .unwrap();
                }
                let record_cid = value.map(|value| {
                    let bytes = DagCborObject::new_text(value.to_string())
                        .to_bytes()
                        .unwrap();
                    let cid = CidV1::compute_cid_for_dag_cbor_bytes(&bytes).unwrap();
                    state
                        .db
                        .insert_repo_record("app.bsky.feed.post", "one", &cid.base32, &bytes)
                        .unwrap();
                    cid
                });
                (publish_commit(&state, rev), record_cid)
            };

            let root_cid = CidV1::from_base32(&commit.root_mst_node_cid).unwrap();
            let requested: Vec<_> = std::iter::once(root_cid)
                .chain(record_cid.clone())
                .collect();
            let bytes = build_blocks_car_with_cache(&state, &requested, &cache).unwrap();
            let nodes = Arc::clone(&cache.lock().unwrap().as_ref().unwrap().nodes);
            if let Some(previous) = &previous {
                assert!(!Arc::ptr_eq(&nodes, previous));
            }
            if let Some(old_cid) = &previous_record {
                assert!(matches!(
                    build_blocks_car_with_cache(&state, std::slice::from_ref(old_cid), &cache),
                    Err(BlocksCarError::Missing(cids)) if cids == vec![old_cid.base32.clone()]
                ));
            }

            std::thread::scope(|scope| {
                let readers: Vec<_> = (0..4)
                    .map(|_| {
                        scope.spawn(|| {
                            build_blocks_car_with_cache(&state, &requested, &cache).unwrap()
                        })
                    })
                    .collect();
                for reader in readers {
                    assert_eq!(reader.join().unwrap(), bytes);
                }
            });
            assert!(
                Arc::ptr_eq(&nodes, &cache.lock().unwrap().as_ref().unwrap().nodes),
                "cache hits must reuse the converted MST"
            );
            let mut reader = Cursor::new(&bytes);
            assert_eq!(
                RepoHeader::read_from_stream(&mut reader)
                    .unwrap()
                    .repo_commit_cid
                    .base32,
                commit.cid,
            );
            let mut returned = HashSet::new();
            while reader.position() < bytes.len() as u64 {
                let block = RepoRecord::read_from_stream(&mut reader).unwrap();
                assert_eq!(
                    CidV1::compute_cid_for_dag_cbor(&block.data_block).unwrap(),
                    block.cid
                );
                returned.insert(block.cid.base32);
            }
            let expected = std::iter::once(commit.cid.clone())
                .chain(requested.iter().map(|cid| cid.base32.clone()))
                .collect::<HashSet<_>>();
            assert_eq!(returned, expected);
            previous = Some(nodes);
            previous_record = record_cid;
        }

        let commit = {
            let _lock = REPO_LOCK.lock().unwrap();
            let commit = publish_commit(&state, "3blocks4");
            state
                .db
                .insert_repo_record("app.bsky.feed.post", "broken", "invalid", &[0])
                .unwrap();
            commit
        };
        assert!(matches!(
            build_blocks_car_with_cache(&state, &[], &cache),
            Err(BlocksCarError::Internal(_))
        ));
        assert!(Arc::ptr_eq(
            &cache.lock().unwrap().as_ref().unwrap().nodes,
            previous.as_ref().unwrap()
        ));
        {
            let _lock = REPO_LOCK.lock().unwrap();
            state
                .db
                .delete_repo_record("app.bsky.feed.post", "broken")
                .unwrap();
        }
        build_blocks_car_with_cache(&state, &[], &cache).unwrap();
        let recovered = Arc::clone(&cache.lock().unwrap().as_ref().unwrap().nodes);
        assert!(!Arc::ptr_eq(&recovered, previous.as_ref().unwrap()));

        let other_dir = tempfile::tempdir().unwrap();
        let lfs = LocalFileSystem::initialize_with_create(other_dir.path()).unwrap();
        Installer::install_db(&lfs, &LOG, false).unwrap();
        let db = PdsDb::connect(&lfs).unwrap();
        db.set_config_property("UserDid", "did:plc:blockscache")
            .unwrap();
        let other_state = PdsState { log: &LOG, lfs, db };
        let other_commit = publish_commit(&other_state, "3blocks4");
        assert_eq!(other_commit.cid, commit.cid);
        build_blocks_car_with_cache(&other_state, &[], &cache).unwrap();
        let other_nodes = Arc::clone(&cache.lock().unwrap().as_ref().unwrap().nodes);
        assert!(
            Arc::ptr_eq(&recovered, &other_nodes),
            "the same commit can reuse its MST regardless of database path"
        );
    }

    #[test]
    fn query_accepts_repeated_cids_parameters() {
        let query = parse_get_blocks_query(Some("did=did%3Aplc%3Aexample&cids=cid1&cids=cid2"));

        assert_eq!(query.did.as_deref(), Some("did:plc:example"));
        assert_eq!(
            query.cids.as_deref(),
            Some(&["cid1".to_string(), "cid2".to_string()][..])
        );
    }

    #[test]
    fn query_distinguishes_missing_cids_from_empty_list() {
        assert_eq!(
            parse_get_blocks_query(Some("did=did%3Aplc%3Aexample")).cids,
            None
        );
        assert_eq!(
            parse_get_blocks_query(Some("did=did%3Aplc%3Aexample&cids=")).cids,
            Some(vec![String::new()])
        );
    }
}
