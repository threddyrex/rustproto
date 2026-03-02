//! com.atproto.repo.applyWrites endpoint.
//!
//! Applies multiple write operations to the repository atomically.

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

/// A single write operation in the request.
#[derive(Deserialize)]
pub struct WriteOperation {
    /// Operation type.
    #[serde(rename = "$type")]
    op_type: String,
    /// Collection NSID.
    collection: String,
    /// Record key (optional for creates).
    rkey: Option<String>,
    /// Record value (for create/update).
    value: Option<serde_json::Value>,
}

/// Request body for applyWrites.
#[derive(Deserialize)]
pub struct ApplyWritesRequest {
    /// Repository DID (must match authenticated user).
    repo: String,
    /// List of write operations.
    writes: Vec<WriteOperation>,
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

/// Result of a single write operation.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteResult {
    /// Result type.
    #[serde(rename = "$type")]
    result_type: String,
    /// AT URI of the record.
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    /// CID of the record.
    #[serde(skip_serializing_if = "Option::is_none")]
    cid: Option<String>,
    /// Validation status.
    #[serde(skip_serializing_if = "Option::is_none")]
    validation_status: Option<String>,
}

/// Successful response for applyWrites.
#[derive(Serialize)]
pub struct ApplyWritesResponse {
    /// Commit information.
    commit: CommitInfo,
    /// Results for each write operation.
    results: Vec<WriteResult>,
}

/// Error response for applyWrites.
#[derive(Serialize)]
pub struct ApplyWritesError {
    error: String,
    message: String,
}

/// POST /xrpc/com.atproto.repo.applyWrites - Apply multiple write operations.
///
/// Applies a batch of create, update, and delete operations atomically.
///
/// # Request Body
///
/// * `repo` - Repository DID
/// * `writes` - List of write operations
/// * `swapCommit` - Optional CID for optimistic concurrency
///
/// # Returns
///
/// * `200 OK` with commit info and results
/// * `400 Bad Request` if parameters are invalid
/// * `401 Unauthorized` if not authenticated
pub async fn apply_writes(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<ApplyWritesRequest>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.repo.applyWrites".to_string(),
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
    if body.repo.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApplyWritesError {
                error: "InvalidRequest".to_string(),
                message: "Error: invalid params.".to_string(),
            }),
        )
            .into_response();
    }

    if body.writes.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApplyWritesError {
                error: "InvalidRequest".to_string(),
                message: "Error: writes array is required.".to_string(),
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
                        Json(ApplyWritesError {
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
                    Json(ApplyWritesError {
                        error: "InternalError".to_string(),
                        message: "Failed to get current commit.".to_string(),
                    }),
                )
                    .into_response();
            }
        }
    }

    // Parse write operations
    let mut operations = Vec::new();

    for write in &body.writes {
        if write.collection.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApplyWritesError {
                    error: "InvalidRequest".to_string(),
                    message: "Error: missing collection in write operation.".to_string(),
                }),
            )
                .into_response();
        }

        // Determine operation type
        let normalized_type = if write.op_type.contains("#create") {
            write_type::CREATE
        } else if write.op_type.contains("#update") {
            write_type::UPDATE
        } else if write.op_type.contains("#delete") {
            write_type::DELETE
        } else {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApplyWritesError {
                    error: "InvalidRequest".to_string(),
                    message: format!("Error: unknown operation type: {}", write.op_type),
                }),
            )
                .into_response();
        };

        // Generate rkey if not provided for create
        let rkey = match &write.rkey {
            Some(r) if !r.is_empty() => r.clone(),
            _ => {
                if normalized_type == write_type::CREATE {
                    UserRepo::generate_tid()
                } else {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApplyWritesError {
                            error: "InvalidRequest".to_string(),
                            message: "Error: rkey is required for update/delete operations.".to_string(),
                        }),
                    )
                        .into_response();
                }
            }
        };

        // Parse record for create/update
        let record = if normalized_type == write_type::CREATE || normalized_type == write_type::UPDATE {
            match &write.value {
                Some(v) => match parse_json_to_dag_cbor(v) {
                    Ok(r) => Some(r),
                    Err(e) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(ApplyWritesError {
                                error: "InvalidRequest".to_string(),
                                message: format!("Error: failed to parse record value: {}", e),
                            }),
                        )
                            .into_response();
                    }
                },
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApplyWritesError {
                            error: "InvalidRequest".to_string(),
                            message: "Error: value is required for create/update operations.".to_string(),
                        }),
                    )
                        .into_response();
                }
            }
        } else {
            None
        };

        operations.push(ApplyWritesOperation {
            op_type: normalized_type.to_string(),
            collection: write.collection.clone(),
            rkey,
            record,
        });
    }

    // Create UserRepo and apply the writes
    let user_repo = match UserRepo::new(&state.db) {
        Ok(repo) => repo,
        Err(e) => {
            state.log.error(&format!("Failed to create UserRepo: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApplyWritesError {
                    error: "InternalError".to_string(),
                    message: "Failed to initialize repository".to_string(),
                }),
            )
                .into_response();
        }
    };

    let results = match user_repo.apply_writes(operations, &ip_address, &user_agent) {
        Ok(results) => results,
        Err(e) => {
            state.log.error(&format!("Failed to apply writes: {}", e));
            return (
                StatusCode::BAD_REQUEST,
                Json(ApplyWritesError {
                    error: "ApplyWritesFailed".to_string(),
                    message: format!("Error applying writes: {}", e),
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
                Json(ApplyWritesError {
                    error: "InternalError".to_string(),
                    message: "Failed to get commit info".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Build response results
    let response_results: Vec<WriteResult> = results
        .iter()
        .map(|r| WriteResult {
            result_type: r.result_type.clone(),
            uri: r.uri.clone(),
            cid: r.cid.as_ref().map(|c| c.base32.clone()),
            validation_status: r.validation_status.clone(),
        })
        .collect();

    (
        StatusCode::OK,
        Json(ApplyWritesResponse {
            commit: CommitInfo {
                cid: commit.cid,
                rev: commit.rev,
            },
            results: response_results,
        }),
    )
        .into_response()
}
