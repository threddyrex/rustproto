//! com.atproto.sync.listRepos endpoint.
//!
//! Lists all repositories hosted on this PDS.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::pds::xrpc::auth_helpers::get_caller_info;

/// Query parameters for listRepos.
#[derive(Deserialize)]
pub struct ListReposQuery {
    /// Maximum number of repos to return.
    #[allow(dead_code)]
    limit: Option<i32>,
    /// Cursor for pagination.
    #[allow(dead_code)]
    cursor: Option<String>,
}

/// A repository entry in the listRepos response.
#[derive(Serialize)]
pub struct RepoEntry {
    /// Repository DID.
    did: String,
    /// Head commit CID.
    head: String,
    /// Current revision.
    rev: String,
    /// Whether the account is active.
    active: bool,
}

/// Successful response for listRepos.
#[derive(Serialize)]
pub struct ListReposResponse {
    /// List of repositories.
    repos: Vec<RepoEntry>,
    /// Cursor for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

/// Error response for listRepos.
#[derive(Serialize)]
pub struct ListReposError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.sync.listRepos - List all repositories.
///
/// Returns a list of all repositories hosted on this PDS.
/// For a single-user PDS, this returns only the local user's repository.
///
/// # Query Parameters
///
/// * `limit` - Maximum repos to return (default: 500)
/// * `cursor` - Pagination cursor
///
/// # Returns
///
/// * `200 OK` with list of repositories
pub async fn sync_list_repos(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(_query): Query<ListReposQuery>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.listRepos".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Get user DID
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(d) => d,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ListReposError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to get user DID: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Get user active status
    let user_active = state.db.get_config_property_bool("UserIsActive").unwrap_or(true);

    // Get repo commit for head and rev
    let repo_commit = match state.db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ListReposError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to get repo commit: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Build the response with single repo
    let repo_entry = RepoEntry {
        did: user_did,
        head: repo_commit.cid,
        rev: repo_commit.rev,
        active: user_active,
    };

    let response = ListReposResponse {
        repos: vec![repo_entry],
        cursor: None, // Single-user PDS, no pagination needed
    };

    (StatusCode::OK, Json(response)).into_response()
}
