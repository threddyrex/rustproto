//! com.atproto.server.checkAccountStatus endpoint.
//!
//! Returns the status of the authenticated user's account.

use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth};

/// Successful response for checkAccountStatus.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckAccountStatusResponse {
    /// Whether the account is activated.
    activated: bool,
    /// Whether the DID is valid.
    valid_did: bool,
    /// Current repo commit CID.
    #[serde(skip_serializing_if = "Option::is_none")]
    repo_commit: Option<String>,
    /// Current repo revision.
    #[serde(skip_serializing_if = "Option::is_none")]
    repo_rev: Option<String>,
}

/// GET /xrpc/com.atproto.server.checkAccountStatus - Account status endpoint.
///
/// Returns status information about the authenticated user's account.
///
/// # Headers
///
/// * `Authorization: Bearer <access_jwt>` - Required
///
/// # Returns
///
/// * `200 OK` with account status on success
/// * `400 Bad Request` if token is expired
/// * `401 Unauthorized` if not authenticated
pub async fn check_account_status(
    State(state): State<Arc<PdsState>>,
    headers: HeaderMap,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.checkAccountStatus".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication (supports Legacy and OAuth)
    let auth_result = check_user_auth(
        &state,
        &headers,
        None,
        "GET",
        "/xrpc/com.atproto.server.checkAccountStatus",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Get account activation status
    let activated = state
        .db
        .get_config_property_bool("UserIsActive")
        .unwrap_or(false);

    // Get repo commit info
    let (repo_commit, repo_rev) = match state.db.get_repo_commit() {
        Ok(commit) => (Some(commit.cid), Some(commit.rev)),
        Err(_) => (None, None),
    };

    Json(CheckAccountStatusResponse {
        activated,
        valid_did: true,
        repo_commit,
        repo_rev,
    })
    .into_response()
}
