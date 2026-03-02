//! com.atproto.sync.getRepoStatus endpoint.
//!
//! Gets the hosting status of a repository.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

/// Query parameters for getRepoStatus.
#[derive(Deserialize)]
pub struct GetRepoStatusQuery {
    /// Repository DID (required).
    did: Option<String>,
}

/// Successful response for getRepoStatus.
#[derive(Serialize)]
pub struct GetRepoStatusResponse {
    /// Repository DID.
    did: String,
    /// Whether the account is active.
    active: bool,
    /// Account status (if not active).
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<String>,
    /// Current revision.
    #[serde(skip_serializing_if = "Option::is_none")]
    rev: Option<String>,
}

/// Error response for getRepoStatus.
#[derive(Serialize)]
pub struct GetRepoStatusError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.sync.getRepoStatus - Get repository hosting status.
///
/// Returns the hosting status of a repository, including whether
/// the account is active and the current revision.
///
/// # Query Parameters
///
/// * `did` - Repository DID (required)
///
/// # Returns
///
/// * `200 OK` with repository status
/// * `400 Bad Request` if DID is missing
/// * `404 Not Found` if repository doesn't exist
pub async fn sync_get_repo_status(
    State(state): State<Arc<PdsState>>,
    Query(query): Query<GetRepoStatusQuery>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.getRepoStatus".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Validate DID parameter
    let did = match &query.did {
        Some(d) if !d.is_empty() => d,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetRepoStatusError {
                    error: "InvalidRequest".to_string(),
                    message: "Missing did".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Check if this DID matches the local user's DID
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(d) => d,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetRepoStatusError {
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
            Json(GetRepoStatusError {
                error: "NotFound".to_string(),
                message: "Repo not found".to_string(),
            }),
        )
            .into_response();
    }

    // Get user active status
    let user_active = state.db.get_config_property_bool("UserIsActive").unwrap_or(true);

    // Get repo commit for rev
    let rev = match state.db.get_repo_commit() {
        Ok(c) => Some(c.rev),
        Err(_) => None,
    };

    // Build response
    let response = GetRepoStatusResponse {
        did: user_did,
        active: user_active,
        status: if user_active { None } else { Some("deactivated".to_string()) },
        rev,
    };

    (StatusCode::OK, Json(response)).into_response()
}
