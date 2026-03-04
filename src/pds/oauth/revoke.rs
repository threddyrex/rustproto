//! OAuth Token Revocation endpoint.
//!
//! POST /oauth/revoke
//!
//! Handles token revocation as specified in RFC 7009.

use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::helpers::{get_caller_info, get_form_value, is_oauth_enabled};

/// POST /oauth/revoke
///
/// Revokes an OAuth token (typically a refresh token).
pub async fn oauth_revoke(
    State(state): State<Arc<PdsState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let (ip_address, user_agent) = get_caller_info(&headers);
    let stat_key = StatisticKey {
        name: "oauth/revoke".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Parse body
    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    // Get the token parameter
    let token = match get_form_value(&body_str, "token") {
        Some(t) if !t.is_empty() => t,
        _ => {
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    // Delete the OAuth session associated with the refresh token
    // Note: Per RFC 7009, revocation should return 200 even if the token doesn't exist
    let _ = state.db.delete_oauth_session_by_refresh_token(&token);

    // Return success with empty keys array (matching dnproto behavior)
    Json(serde_json::json!({
        "keys": []
    })).into_response()
}
