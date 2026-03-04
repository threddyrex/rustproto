//! app.bsky.actor.getPreferences endpoint.
//!
//! Returns the user's preferences stored on the PDS.
//! Preferences are the exception to the rule of proxying app.bsky.* to the AppView.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value as JsonValue;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info};

/// GET /xrpc/app.bsky.actor.getPreferences - Get user preferences.
///
/// Returns the authenticated user's preferences stored on the PDS.
///
/// # Headers
///
/// * `Authorization: Bearer <access_jwt>` - Required
///
/// # Returns
///
/// * `200 OK` with preferences JSON on success
/// * `204 No Content` if no preferences are stored
/// * `401 Unauthorized` if not authenticated
pub async fn get_preferences(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/app.bsky.actor.getPreferences".to_string(),
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
        "/xrpc/app.bsky.actor.getPreferences",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Check if preferences exist
    let prefs_count = match state.db.get_preferences_count() {
        Ok(count) => count,
        Err(e) => {
            state.log.error(&format!(
                "[PREFS] Failed to get preferences count: {}",
                e
            ));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "InternalServerError",
                    "message": "Failed to retrieve preferences"
                })),
            )
                .into_response();
        }
    };

    if prefs_count == 0 {
        // Return empty preferences array if no preferences are stored
        return Json(serde_json::json!({
            "preferences": []
        }))
        .into_response();
    }

    // Get preferences JSON
    let prefs_json = match state.db.get_preferences() {
        Ok(prefs) => prefs,
        Err(e) => {
            state.log.error(&format!(
                "[PREFS] Failed to get preferences: {}",
                e
            ));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "InternalServerError",
                    "message": "Failed to retrieve preferences"
                })),
            )
                .into_response();
        }
    };

    // Parse and return the preferences JSON
    // If the stored JSON is empty or invalid, return an empty preferences array
    if prefs_json.is_empty() {
        return Json(serde_json::json!({
            "preferences": []
        }))
        .into_response();
    }

    match serde_json::from_str::<JsonValue>(&prefs_json) {
        Ok(prefs) => Json(prefs).into_response(),
        Err(e) => {
            state.log.warning(&format!(
                "[PREFS] Failed to parse preferences JSON, returning empty: {}",
                e
            ));
            // Return empty preferences instead of failing
            Json(serde_json::json!({
                "preferences": []
            }))
            .into_response()
        }
    }
}
