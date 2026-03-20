//! com.atproto.repo.describeRepo endpoint.
//!
//! Returns repository metadata including DID, handle, collections, and DID document.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;
use crate::pds::xrpc::auth_helpers::get_caller_info;

/// Query parameters for describeRepo.
#[derive(Deserialize)]
pub struct DescribeRepoQuery {
    /// The repository DID or handle.
    repo: Option<String>,
}

/// Successful response for describeRepo.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeRepoResponse {
    /// The user's handle.
    handle: String,
    /// The user's DID.
    did: String,
    /// The DID document.
    #[serde(skip_serializing_if = "Option::is_none")]
    did_doc: Option<serde_json::Value>,
    /// List of collections in the repository.
    collections: Vec<String>,
    /// Whether the handle resolves correctly to this DID.
    handle_is_correct: bool,
}

/// Error response for describeRepo.
#[derive(Serialize)]
pub struct DescribeRepoError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.repo.describeRepo - Get repository metadata.
///
/// Returns information about the repository including the DID, handle,
/// collections, and DID document.
///
/// # Query Parameters
///
/// * `repo` - Optional repository identifier (DID or handle). Defaults to local user.
///
/// # Returns
///
/// * `200 OK` with repository metadata
pub async fn describe_repo(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<DescribeRepoQuery>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.repo.describeRepo".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Get local user info
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(e) => {
            state.log.error(&format!("Failed to get UserDid: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DescribeRepoError {
                    error: "InternalError".to_string(),
                    message: "Failed to get user information".to_string(),
                }),
            )
                .into_response();
        }
    };

    let user_handle = match state.db.get_config_property("UserHandle") {
        Ok(handle) => handle,
        Err(e) => {
            state.log.error(&format!("Failed to get UserHandle: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(DescribeRepoError {
                    error: "InternalError".to_string(),
                    message: "Failed to get user information".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Check if this is a request for local repo
    let is_local = match &query.repo {
        None => true,
        Some(repo) => repo == &user_did || repo == &user_handle,
    };

    if !is_local {
        // For now, we only support describing the local repo
        return (
            StatusCode::BAD_REQUEST,
            Json(DescribeRepoError {
                error: "InvalidRequest".to_string(),
                message: "Only local repository is supported".to_string(),
            }),
        )
            .into_response();
    }

    // Get the DID document
    let app_view_host_name = state.db.get_config_property("AppViewHostName")
        .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
    let did_doc = state
        .lfs
        .resolve_actor_info(&user_did, None, &app_view_host_name)
        .await
        .ok()
        .and_then(|info| info.did_doc)
        .and_then(|doc| serde_json::from_str::<serde_json::Value>(&doc).ok());

    // Get the collections
    let collections = match state.db.get_unique_collections() {
        Ok(cols) => cols,
        Err(e) => {
            state.log.error(&format!("Failed to get collections: {}", e));
            Vec::new()
        }
    };

    (
        StatusCode::OK,
        Json(DescribeRepoResponse {
            handle: user_handle,
            did: user_did,
            did_doc,
            collections,
            handle_is_correct: true,
        }),
    )
        .into_response()
}
