//! com.atproto.repo.deleteRecord endpoint.
//!
//! Deletes a record from the repository.

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
use crate::pds::user_repo::{ApplyWritesOperation, UserRepo, write_type};
use crate::pds::xrpc::auth_helpers::{auth_failure_response, check_legacy_auth, get_caller_info};

/// Request body for deleteRecord.
#[derive(Deserialize)]
pub struct DeleteRecordRequest {
    /// Repository DID (must match authenticated user).
    #[allow(dead_code)]
    repo: String,
    /// Collection NSID.
    collection: String,
    /// Record key.
    rkey: String,
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

/// Successful response for deleteRecord.
#[derive(Serialize)]
pub struct DeleteRecordResponse {
    /// Commit information.
    commit: CommitInfo,
}

/// Error response for deleteRecord.
#[derive(Serialize)]
pub struct DeleteRecordError {
    error: String,
    message: String,
}

/// POST /xrpc/com.atproto.repo.deleteRecord - Delete a record.
///
/// Deletes a record from the repository.
///
/// # Request Body
///
/// * `repo` - Repository DID
/// * `collection` - Collection NSID
/// * `rkey` - Record key
/// * `swapRecord` - Optional CID to ensure existing record matches
/// * `swapCommit` - Optional CID for optimistic concurrency
///
/// # Returns
///
/// * `200 OK` with commit info
/// * `400 Bad Request` if parameters are invalid
/// * `401 Unauthorized` if not authenticated
pub async fn delete_record(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<DeleteRecordRequest>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.repo.deleteRecord".to_string(),
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
    if body.collection.is_empty() || body.rkey.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(DeleteRecordError {
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
                        Json(DeleteRecordError {
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
                    Json(DeleteRecordError {
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
                        Json(DeleteRecordError {
                            error: "InvalidSwap".to_string(),
                            message: "Record CID mismatch.".to_string(),
                        }),
                    )
                        .into_response();
                }
            }
            Err(_) => {
                // Record doesn't exist - that's fine for delete
            }
        }
    }

    // Create UserRepo and apply the delete
    let user_repo = match UserRepo::new(&state.db) {
        Ok(repo) => repo,
        Err(e) => {
            state.log.error(&format!("Failed to create UserRepo: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DeleteRecordError {
                    error: "InternalError".to_string(),
                    message: "Failed to initialize repository".to_string(),
                }),
            )
                .into_response();
        }
    };

    let operation = ApplyWritesOperation {
        op_type: write_type::DELETE.to_string(),
        collection: body.collection.clone(),
        rkey: body.rkey.clone(),
        record: None,
    };

    match user_repo.apply_writes(vec![operation], &ip_address, &user_agent) {
        Ok(_) => {}
        Err(e) => {
            state.log.error(&format!("Failed to delete record: {}", e));
            return (
                StatusCode::BAD_REQUEST,
                Json(DeleteRecordError {
                    error: "DeleteRecordFailed".to_string(),
                    message: format!("Error deleting record: {}", e),
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
                Json(DeleteRecordError {
                    error: "InternalError".to_string(),
                    message: "Failed to get commit info".to_string(),
                }),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(DeleteRecordResponse {
            commit: CommitInfo {
                cid: commit.cid,
                rev: commit.rev,
            },
        }),
    )
        .into_response()
}
