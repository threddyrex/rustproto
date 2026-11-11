//! com.atproto.sync.getRepo endpoint.
//!
//! Exports the full repository as a CAR file.

use std::collections::HashMap;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::mst::{Mst, MstItem};
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use super::{get_caller_info};
use crate::repo::{CidV1, DagCborObject, RepoMst, VarInt};

const SLOW_GET_REPO_THRESHOLD: Duration = Duration::from_millis(1250);

#[derive(Default)]
struct CarTimings {
    load_commit_ms: Option<u128>,
    write_commit_ms: Option<u128>,
    load_records_ms: Option<u128>,
    build_mst_ms: Option<u128>,
    encode_mst_ms: Option<u128>,
    write_mst_ms: Option<u128>,
    write_records_ms: Option<u128>,
}

fn format_timing(ms: Option<u128>) -> String {
    ms.map(|ms| format!("{}ms", ms)).unwrap_or_else(|| "n/a".to_string())
}

fn lap_ms(lap_start: &mut Instant) -> u128 {
    let now = Instant::now();
    let ms = now.duration_since(*lap_start).as_millis();
    *lap_start = now;
    ms
}

/// Query parameters for getRepo.
#[derive(Deserialize)]
pub struct GetRepoQuery {
    /// Repository DID (required).
    did: Option<String>,
    /// Optional 'since' parameter for incremental sync.
    #[allow(dead_code)]
    since: Option<String>,
}

/// Error response for getRepo.
#[derive(Serialize)]
pub struct GetRepoError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.sync.getRepo - Export full repository as CAR.
///
/// Downloads the complete repository as a CAR (Content Addressable aRchive) file.
/// The CAR contains:
/// - CAR header with root CID
/// - Commit block
/// - All MST nodes
/// - All record blocks
///
/// # Query Parameters
///
/// * `did` - Repository DID (required)
/// * `since` - Optional revision to export from (incremental sync)
///
/// # Returns
///
/// * `200 OK` with CAR file bytes (application/vnd.ipld.car)
/// * `400 Bad Request` if DID is missing
/// * `404 Not Found` if repository doesn't exist
pub async fn sync_get_repo(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<GetRepoQuery>,
) -> Response {
    let request_start = Instant::now();
    let mut lap = request_start;
    let mut car_timings = CarTimings::default();
    let mut increment_stats_ms = 0;
    let mut validate_did_ms = 0;
    let mut build_car_ms = 0;
    let mut response_bytes = 0;

    let response = (|| {
        let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));
        let stat_key = StatisticKey {
            name: "xrpc/com.atproto.sync.getRepo".to_string(),
            ip_address,
            user_agent,
        };
        let _ = state.db.increment_statistic_for_endpoint(&stat_key);
        increment_stats_ms = lap_ms(&mut lap);

        let did = match &query.did {
            Some(d) if !d.is_empty() => d,
            _ => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(GetRepoError {
                        error: "InvalidRequest".to_string(),
                        message: "Missing did".to_string(),
                    }),
                )
                    .into_response();
            }
        };

        let user_did_result = state.db.get_config_property("UserDid");
        validate_did_ms = lap_ms(&mut lap);
        let user_did = match user_did_result {
            Ok(d) => d,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(GetRepoError {
                        error: "InternalError".to_string(),
                        message: format!("Failed to get user DID: {}", e),
                    }),
                )
                    .into_response();
            }
        };

        if !did.eq_ignore_ascii_case(&user_did) {
            return (
                StatusCode::NOT_FOUND,
                Json(GetRepoError {
                    error: "NotFound".to_string(),
                    message: "Repo not found".to_string(),
                }),
            )
                .into_response();
        }

        let car_result = build_repo_car(&state, &mut car_timings);
        build_car_ms = lap_ms(&mut lap);
        match car_result {
            Ok(car_bytes) => {
                response_bytes = car_bytes.len();
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
                    Json(GetRepoError {
                        error: "InternalError".to_string(),
                        message: format!("Failed to export repo: {}", e),
                    }),
                )
                    .into_response()
            }
        }
    })();

    let total = request_start.elapsed();
    if total > SLOW_GET_REPO_THRESHOLD {
        state.log.warning(&format!(
            "[GET_REPO] Slow request (status {}): total={}ms increment_stats={}ms \
             validate_did={}ms build_car={}ms (load_commit={} write_commit={} \
             load_records={} build_mst={} encode_mst={} write_mst={} \
             write_records={}) response_bytes={}",
            response.status().as_u16(),
            total.as_millis(),
            increment_stats_ms,
            validate_did_ms,
            build_car_ms,
            format_timing(car_timings.load_commit_ms),
            format_timing(car_timings.write_commit_ms),
            format_timing(car_timings.load_records_ms),
            format_timing(car_timings.build_mst_ms),
            format_timing(car_timings.encode_mst_ms),
            format_timing(car_timings.write_mst_ms),
            format_timing(car_timings.write_records_ms),
            response_bytes,
        ));
    }

    response
}

/// Build the complete repository as a CAR file.
fn build_repo_car(state: &PdsState, timings: &mut CarTimings) -> Result<Vec<u8>, String> {
    let mut car_bytes: Vec<u8> = Vec::new();
    let mut lap = Instant::now();

    // Get repo header and commit
    let repo_header = state.db.get_repo_header()
        .map_err(|e| format!("Failed to get repo header: {}", e))?;
    let repo_commit = state.db.get_repo_commit()
        .map_err(|e| format!("Failed to get repo commit: {}", e))?;
    timings.load_commit_ms = Some(lap_ms(&mut lap));

    // Write CAR header
    write_car_header(&mut car_bytes, &repo_header.repo_commit_cid)
        .map_err(|e| format!("Failed to write CAR header: {}", e))?;

    // Write commit block
    let commit_cid = CidV1::from_base32(&repo_commit.cid)
        .map_err(|e| format!("Invalid commit CID: {}", e))?;
    let commit_dag_cbor = build_commit_dag_cbor(&state, &repo_commit)?;
    write_car_block(&mut car_bytes, &commit_cid, &commit_dag_cbor)
        .map_err(|e| format!("Failed to write commit block: {}", e))?;
    timings.write_commit_ms = Some(lap_ms(&mut lap));

    // Get all records and build MST
    let all_records = state.db.get_all_repo_records()
        .map_err(|e| format!("Failed to get records: {}", e))?;
    timings.load_records_ms = Some(lap_ms(&mut lap));

    let mst_items: Vec<MstItem> = all_records
        .iter()
        .map(|r| MstItem::new(&format!("{}/{}", r.collection, r.rkey), &r.cid))
        .collect();

    let mst = Mst::assemble_tree_from_items(&mst_items);
    timings.build_mst_ms = Some(lap_ms(&mut lap));

    // Convert MST to DAG-CBOR
    let mst_cache = RepoMst::convert_mst_to_dag_cbor(&mst)
        .map_err(|e| format!("Failed to convert MST: {}", e))?;
    timings.encode_mst_ms = Some(lap_ms(&mut lap));

    // Write all MST nodes
    for (_key, (cid, dag_cbor)) in &mst_cache {
        write_car_block(&mut car_bytes, cid, dag_cbor)
            .map_err(|e| format!("Failed to write MST node: {}", e))?;
    }
    timings.write_mst_ms = Some(lap_ms(&mut lap));

    // Write all records
    for record in &all_records {
        let record_cid = CidV1::from_base32(&record.cid)
            .map_err(|e| format!("Invalid record CID: {}", e))?;
        let record_dag_cbor = DagCborObject::from_bytes(&record.dag_cbor_bytes)
            .map_err(|e| format!("Invalid record DAG-CBOR: {}", e))?;
        write_car_block(&mut car_bytes, &record_cid, &record_dag_cbor)
            .map_err(|e| format!("Failed to write record block: {}", e))?;
    }
    timings.write_records_ms = Some(lap_ms(&mut lap));

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
