//! com.atproto.server.getSession endpoint.
//!
//! Returns information about the current authenticated session.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info};

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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.getSession".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication (supports Legacy and OAuth)
    let auth_result = check_user_auth(
        &state,
        &headers,
        None,
        "GET",
        "/xrpc/com.atproto.server.getSession",
    );
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
