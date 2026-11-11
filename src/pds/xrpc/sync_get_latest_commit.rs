//! com.atproto.sync.getLatestCommit endpoint.
//!
//! Gets the current commit CID and revision for a repository.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use super::get_caller_info;
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

/// Query parameters for getLatestCommit.
#[derive(Deserialize)]
pub struct GetLatestCommitQuery {
    /// Repository DID (required).
    did: Option<String>,
}

/// Successful response for getLatestCommit.
#[derive(Serialize)]
pub struct GetLatestCommitResponse {
    /// Current commit CID.
    cid: String,
    /// Current revision.
    rev: String,
}

/// Error response for getLatestCommit.
#[derive(Serialize)]
pub struct GetLatestCommitError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.sync.getLatestCommit - Get the current repository commit.
///
/// This endpoint does not require authentication.
///
/// # Query Parameters
///
/// * `did` - Repository DID (required)
///
/// # Returns
///
/// * `200 OK` with the current commit CID and revision
/// * `400 Bad Request` if DID is missing
/// * `404 Not Found` if the repository doesn't exist
pub async fn sync_get_latest_commit(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<GetLatestCommitQuery>,
) -> Response {
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.getLatestCommit".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    let did = match &query.did {
        Some(did) if !did.is_empty() => did,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetLatestCommitError {
                    error: "InvalidRequest".to_string(),
                    message: "Missing did".to_string(),
                }),
            )
                .into_response();
        }
    };

    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetLatestCommitError {
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
            Json(GetLatestCommitError {
                error: "NotFound".to_string(),
                message: "Repo not found".to_string(),
            }),
        )
            .into_response();
    }

    let commit = match state.db.get_repo_commit() {
        Ok(commit) => commit,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetLatestCommitError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to get repo commit: {}", e),
                }),
            )
                .into_response();
        }
    };

    (
        StatusCode::OK,
        Json(GetLatestCommitResponse {
            cid: commit.cid,
            rev: commit.rev,
        }),
    )
        .into_response()
}
