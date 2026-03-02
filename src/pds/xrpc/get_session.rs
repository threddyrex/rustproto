//! com.atproto.server.getSession endpoint.
//!
//! Returns information about the current authenticated session.

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

use super::auth_helpers::{auth_failure_response, check_legacy_auth};

/// Successful response for getSession.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetSessionResponse {
    /// The user's DID.
    did: String,
    /// The user's handle.
    handle: String,
    /// The user's email (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    /// Whether the email is confirmed.
    email_confirmed: bool,
}

/// GET /xrpc/com.atproto.server.getSession - Get current session endpoint.
///
/// Returns information about the authenticated user's current session.
///
/// # Headers
///
/// * `Authorization: Bearer <access_jwt>` - Required
///
/// # Returns
///
/// * `200 OK` with session info on success
/// * `400 Bad Request` if token is expired
/// * `401 Unauthorized` if not authenticated
pub async fn get_session(
    State(state): State<Arc<PdsState>>,
    headers: HeaderMap,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.getSession".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication
    let auth_result = check_legacy_auth(&state, &headers);
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Get user info from config
    let did = state.db.get_config_property("UserDid").unwrap_or_default();
    let handle = state.db.get_config_property("UserHandle").unwrap_or_default();
    let email = state.db.get_config_property("UserEmail").ok();

    Json(GetSessionResponse {
        did,
        handle,
        email,
        email_confirmed: true,
    })
    .into_response()
}
