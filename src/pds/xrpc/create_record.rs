//! com.atproto.repo.createRecord endpoint.
//!
//! Creates a new record in the repository.

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
use crate::pds::xrpc::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info};

/// Request body for createRecord.
#[derive(Deserialize)]
pub struct CreateRecordRequest {
    /// Repository DID (must match authenticated user).
    repo: String,
    /// Collection NSID.
    collection: String,
    /// Record key (optional, will be auto-generated if not provided).
    rkey: Option<String>,
    /// The record data.
    record: serde_json::Value,
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

/// Successful response for createRecord.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRecordResponse {
    /// AT URI of the created record.
    uri: String,
    /// CID of the created record.
    cid: String,
    /// Commit information.
    commit: CommitInfo,
    /// Validation status.
    validation_status: String,
}

/// Error response for createRecord.
#[derive(Serialize)]
pub struct CreateRecordError {
    error: String,
    message: String,
}

/// POST /xrpc/com.atproto.repo.createRecord - Create a new record.
///
/// Creates a new record in the specified collection.
///
/// # Request Body
///
/// * `repo` - Repository DID
/// * `collection` - Collection NSID
/// * `rkey` - Optional record key (auto-generated if not provided)
/// * `record` - The record data
/// * `swapCommit` - Optional CID for optimistic concurrency
///
/// # Returns
///
/// * `200 OK` with created record info
/// * `400 Bad Request` if parameters are invalid
/// * `401 Unauthorized` if not authenticated
pub async fn create_record(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<CreateRecordRequest>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.repo.createRecord".to_string(),
        ip_address: ip_address.clone(),
        user_agent: user_agent.clone(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication (supports Legacy and OAuth)
    let auth_result = check_user_auth(
        &state,
        &headers,
        None,
        "POST",
        "/xrpc/com.atproto.repo.createRecord",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Validate input
    if body.collection.is_empty() || body.repo.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CreateRecordError {
                error: "InvalidRequest".to_string(),
                message: "Error: invalid params.".to_string(),
            }),
        )
            .into_response();
    }

    // Generate rkey if not provided
    let rkey = body.rkey.unwrap_or_else(|| UserRepo::generate_tid());

    // Check swapCommit if provided
    if let Some(swap_cid) = &body.swap_commit {
        match state.db.get_repo_commit() {
            Ok(current_commit) => {
                if &current_commit.cid != swap_cid {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(CreateRecordError {
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
                    Json(CreateRecordError {
                        error: "InternalError".to_string(),
                        message: "Failed to get current commit.".to_string(),
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
                Json(CreateRecordError {
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
                Json(CreateRecordError {
                    error: "InternalError".to_string(),
                    message: "Failed to initialize repository".to_string(),
                }),
            )
                .into_response();
        }
    };

    let operation = ApplyWritesOperation {
        op_type: write_type::CREATE.to_string(),
        collection: body.collection.clone(),
        rkey: rkey.clone(),
        record: Some(record),
    };

    let results = match user_repo.apply_writes(vec![operation], &ip_address, &user_agent) {
        Ok(results) => results,
        Err(e) => {
            state.log.error(&format!("Failed to create record: {}", e));
            return (
                StatusCode::BAD_REQUEST,
                Json(CreateRecordError {
                    error: "CreateRecordFailed".to_string(),
                    message: format!("Error creating record: {}", e),
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
                Json(CreateRecordError {
                    error: "CreateRecordFailed".to_string(),
                    message: "Error creating record.".to_string(),
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
                Json(CreateRecordError {
                    error: "InternalError".to_string(),
                    message: "Failed to get commit info".to_string(),
                }),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(CreateRecordResponse {
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
