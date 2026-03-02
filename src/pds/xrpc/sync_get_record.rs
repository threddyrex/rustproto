//! com.atproto.sync.getRecord endpoint.
//!
//! Gets a single record as a CAR file with proof chain.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::mst::{Mst, MstItem, MstNode};
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::repo::{CidV1, DagCborObject, MstNodeKey, RepoMst, VarInt};

/// Query parameters for getRecord.
#[derive(Deserialize)]
pub struct GetRecordQuery {
    /// Repository DID (required).
    #[allow(dead_code)]
    did: Option<String>,
    /// Collection NSID (required).
    collection: Option<String>,
    /// Record key (required).
    rkey: Option<String>,
}

/// Error response for getRecord.
#[derive(Serialize)]
pub struct GetRecordError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.sync.getRecord - Get a record with proof as CAR.
///
/// Returns a single record with its proof chain as a CAR file.
/// The CAR contains:
/// - CAR header with root CID
/// - Commit block
/// - All MST nodes on the path to the record (proof chain)
/// - The record block itself
///
/// # Query Parameters
///
/// * `did` - Repository DID (optional, defaults to local user)
/// * `collection` - Collection NSID (required)
/// * `rkey` - Record key (required)
///
/// # Returns
///
/// * `200 OK` with CAR file bytes (application/vnd.ipld.car)
/// * `400 Bad Request` if parameters are missing
/// * `404 Not Found` if record doesn't exist
pub async fn sync_get_record(
    State(state): State<Arc<PdsState>>,
    Query(query): Query<GetRecordQuery>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.getRecord".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Validate collection parameter
    let collection = match &query.collection {
        Some(c) if !c.is_empty() => c,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetRecordError {
                    error: "InvalidRequest".to_string(),
                    message: "Error: Param 'collection' is required.".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Validate rkey parameter
    let rkey = match &query.rkey {
        Some(r) if !r.is_empty() => r,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetRecordError {
                    error: "InvalidRequest".to_string(),
                    message: "Error: Param 'rkey' is required.".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Check if record exists
    match state.db.record_exists(collection, rkey) {
        Ok(true) => {}
        Ok(false) => {
            return (
                StatusCode::NOT_FOUND,
                Json(GetRecordError {
                    error: "NotFound".to_string(),
                    message: "Record not found".to_string(),
                }),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetRecordError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to check record: {}", e),
                }),
            )
                .into_response();
        }
    }

    // Build the CAR file with proof
    match build_record_proof_car(&state, collection, rkey) {
        Ok(car_bytes) => {
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/vnd.ipld.car")
                .body(Body::from(car_bytes))
                .unwrap_or_else(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to build response",
                    )
                        .into_response()
                })
        }
        Err(e) => {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetRecordError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to build proof: {}", e),
                }),
            )
                .into_response()
        }
    }
}

/// Build a CAR file containing a record and its proof chain.
fn build_record_proof_car(state: &PdsState, collection: &str, rkey: &str) -> Result<Vec<u8>, String> {
    let mut car_bytes: Vec<u8> = Vec::new();

    // Get repo header and commit
    let repo_header = state.db.get_repo_header()
        .map_err(|e| format!("Failed to get repo header: {}", e))?;
    let repo_commit = state.db.get_repo_commit()
        .map_err(|e| format!("Failed to get repo commit: {}", e))?;

    // Get the record
    let repo_record = state.db.get_repo_record(collection, rkey)
        .map_err(|e| format!("Failed to get record: {}", e))?;

    // Build MST from all records
    let all_records = state.db.get_all_repo_records()
        .map_err(|e| format!("Failed to get records: {}", e))?;

    let mst_items: Vec<MstItem> = all_records
        .iter()
        .map(|r| MstItem::new(&format!("{}/{}", r.collection, r.rkey), &r.cid))
        .collect();

    let mst = Mst::assemble_tree_from_items(&mst_items);

    // Find nodes on the path to the record (proof chain)
    let full_key = format!("{}/{}", collection, rkey);
    let proof_nodes: Vec<&MstNode> = mst.find_nodes_for_key(&full_key);

    // Convert ALL MST nodes to DAG-CBOR first (so CIDs are computed correctly)
    let mst_cache = RepoMst::convert_mst_to_dag_cbor(&mst)
        .map_err(|e| format!("Failed to convert MST: {}", e))?;

    // Write CAR header
    write_car_header(&mut car_bytes, &repo_header.repo_commit_cid)
        .map_err(|e| format!("Failed to write CAR header: {}", e))?;

    // Write commit block
    let commit_cid = CidV1::from_base32(&repo_commit.cid)
        .map_err(|e| format!("Invalid commit CID: {}", e))?;
    let commit_dag_cbor = build_commit_dag_cbor(state, &repo_commit)?;
    write_car_block(&mut car_bytes, &commit_cid, &commit_dag_cbor)
        .map_err(|e| format!("Failed to write commit block: {}", e))?;

    // Write only the MST nodes on the path to the record (proof chain)
    for node in proof_nodes {
        let node_key = MstNodeKey::from_node(node);
        if let Some((cid, dag_cbor)) = mst_cache.get(&node_key) {
            write_car_block(&mut car_bytes, cid, dag_cbor)
                .map_err(|e| format!("Failed to write MST node: {}", e))?;
        }
    }

    // Write the record block
    let record_cid = CidV1::from_base32(&repo_record.cid)
        .map_err(|e| format!("Invalid record CID: {}", e))?;
    let record_dag_cbor = DagCborObject::from_bytes(&repo_record.dag_cbor_bytes)
        .map_err(|e| format!("Invalid record DAG-CBOR: {}", e))?;
    write_car_block(&mut car_bytes, &record_cid, &record_dag_cbor)
        .map_err(|e| format!("Failed to write record block: {}", e))?;

    Ok(car_bytes)
}

/// Build the commit DAG-CBOR object.
fn build_commit_dag_cbor(state: &PdsState, commit: &crate::pds::db::DbRepoCommit) -> Result<DagCborObject, String> {
    let user_did = state.db.get_config_property("UserDid")
        .map_err(|e| format!("Failed to get UserDid: {}", e))?;

    let root_cid = CidV1::from_base32(&commit.root_mst_node_cid)
        .map_err(|e| format!("Invalid root CID: {}", e))?;

    let mut commit_map: HashMap<String, DagCborObject> = HashMap::new();
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

/// Write the CAR header to a stream.
fn write_car_header<W: Write>(writer: &mut W, root_cid_str: &str) -> Result<(), std::io::Error> {
    let root_cid = CidV1::from_base32(root_cid_str)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut header_map: HashMap<String, DagCborObject> = HashMap::new();
    header_map.insert("version".to_string(), DagCborObject::new_unsigned_int(1));
    header_map.insert("roots".to_string(), DagCborObject::new_array(vec![
        DagCborObject::new_cid(root_cid),
    ]));

    let header_dag_cbor = DagCborObject::new_map(header_map);
    let header_bytes = header_dag_cbor.to_bytes()?;

    // Write length as varint
    let length_varint = VarInt::from_long(header_bytes.len() as i64);
    length_varint.write_varint(writer)?;
    writer.write_all(&header_bytes)?;

    Ok(())
}

/// Write a CAR block to a stream.
fn write_car_block<W: Write>(writer: &mut W, cid: &CidV1, dag_cbor: &DagCborObject) -> Result<(), std::io::Error> {
    let data_bytes = dag_cbor.to_bytes()?;
    let cid_bytes = &cid.all_bytes;

    // Block format: varint(cid_length + data_length) | cid | data
    let total_length = cid_bytes.len() + data_bytes.len();
    let length_varint = VarInt::from_long(total_length as i64);

    length_varint.write_varint(writer)?;
    writer.write_all(cid_bytes)?;
    writer.write_all(&data_bytes)?;

    Ok(())
}
