//! com.atproto.repo.putRecord endpoint.
//!
//! Creates or updates a record in the repository.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::pds::user_repo::{ApplyWritesOperation, UserRepo, parse_json_to_dag_cbor, write_type};
use crate::pds::xrpc::auth_helpers::{auth_failure_response, check_legacy_auth, get_caller_info};

/// Request body for putRecord.
#[derive(Deserialize)]
pub struct PutRecordRequest {
    /// Repository DID (must match authenticated user).
    repo: String,
    /// Collection NSID.
    collection: String,
    /// Record key (required for put).
    rkey: String,
    /// The record data.
    record: serde_json::Value,
    /// Optional swap record CID for optimistic concurrency.
    #[serde(rename = "swapRecord")]
    swap_record: Option<String>,
    /// Optional swap commit CID for optimistic concurrency.
    #[serde(rename = "swapCommit")]
    swap_commit: Option<String>,
}

/// Commit information in the response.
#[derive(Serialize)]
pub struct CommitInfo {
    /// CID of the commit.
    cid: String,
    /// Revision string.
    rev: String,
}

/// Successful response for putRecord.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PutRecordResponse {
    /// AT URI of the record.
    uri: String,
    /// CID of the record.
    cid: String,
    /// Commit information.
    commit: CommitInfo,
    /// Validation status.
    validation_status: String,
}

/// Error response for putRecord.
#[derive(Serialize)]
pub struct PutRecordError {
    error: String,
    message: String,
}

/// POST /xrpc/com.atproto.repo.putRecord - Create or update a record.
///
/// Creates a new record or updates an existing one at the specified key.
///
/// # Request Body
///
/// * `repo` - Repository DID
/// * `collection` - Collection NSID
/// * `rkey` - Record key
/// * `record` - The record data
/// * `swapRecord` - Optional CID to ensure existing record matches
/// * `swapCommit` - Optional CID for optimistic concurrency
///
/// # Returns
///
/// * `200 OK` with record info
/// * `400 Bad Request` if parameters are invalid
/// * `401 Unauthorized` if not authenticated
pub async fn put_record(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<PutRecordRequest>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.repo.putRecord".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Check authentication
    let auth_result = check_legacy_auth(&state, &headers);
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Validate input
    if body.collection.is_empty() || body.repo.is_empty() || body.rkey.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(PutRecordError {
                error: "InvalidRequest".to_string(),
                message: "Error: invalid params.".to_string(),
            }),
        )
            .into_response();
    }

    // Check swapCommit if provided
    if let Some(swap_cid) = &body.swap_commit {
        match state.db.get_repo_commit() {
            Ok(current_commit) => {
                if &current_commit.cid != swap_cid {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(PutRecordError {
                            error: "InvalidSwap".to_string(),
                            message: "Commit CID mismatch.".to_string(),
                        }),
                    )
                        .into_response();
                }
            }
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(PutRecordError {
                        error: "InternalError".to_string(),
                        message: "Failed to get current commit.".to_string(),
                    }),
                )
                    .into_response();
            }
        }
    }

    // Check swapRecord if provided
    if let Some(swap_record_cid) = &body.swap_record {
        match state.db.get_repo_record(&body.collection, &body.rkey) {
            Ok(existing_record) => {
                if &existing_record.cid != swap_record_cid {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(PutRecordError {
                            error: "InvalidSwap".to_string(),
                            message: "Record CID mismatch.".to_string(),
                        }),
                    )
                        .into_response();
                }
            }
            Err(_) => {
                // If swapRecord is provided but record doesn't exist, that's an error
                return (
                    StatusCode::BAD_REQUEST,
                    Json(PutRecordError {
                        error: "InvalidSwap".to_string(),
                        message: "Record does not exist.".to_string(),
                    }),
                )
                    .into_response();
            }
        }
    }

    // Parse record JSON to DAG-CBOR
    let record = match parse_json_to_dag_cbor(&body.record) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PutRecordError {
                    error: "InvalidRequest".to_string(),
                    message: format!("Failed to parse record: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Create UserRepo and apply the write
    let user_repo = match UserRepo::new(&state.db) {
        Ok(repo) => repo,
        Err(e) => {
            state.log.error(&format!("Failed to create UserRepo: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PutRecordError {
                    error: "InternalError".to_string(),
                    message: "Failed to initialize repository".to_string(),
                }),
            )
                .into_response();
        }
    };

    let operation = ApplyWritesOperation {
        op_type: write_type::UPDATE.to_string(),
        collection: body.collection.clone(),
        rkey: body.rkey.clone(),
        record: Some(record),
    };

    let results = match user_repo.apply_writes(vec![operation], &ip_address, &user_agent) {
        Ok(results) => results,
        Err(e) => {
            state.log.error(&format!("Failed to put record: {}", e));
            return (
                StatusCode::BAD_REQUEST,
                Json(PutRecordError {
                    error: "PutRecordFailed".to_string(),
                    message: format!("Error updating record: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Get the result
    let result = match results.first() {
        Some(r) => r,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PutRecordError {
                    error: "PutRecordFailed".to_string(),
                    message: "Error updating record.".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Get updated commit info
    let commit = match state.db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            state.log.error(&format!("Failed to get repo commit: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PutRecordError {
                    error: "InternalError".to_string(),
                    message: "Failed to get commit info".to_string(),
                }),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(PutRecordResponse {
            uri: result.uri.clone().unwrap_or_default(),
            cid: result.cid.as_ref().map(|c| c.base32.clone()).unwrap_or_default(),
            commit: CommitInfo {
                cid: commit.cid,
                rev: commit.rev,
            },
            validation_status: result.validation_status.clone().unwrap_or_else(|| "valid".to_string()),
        }),
    )
        .into_response()
}
