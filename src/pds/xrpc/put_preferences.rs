//! app.bsky.actor.putPreferences endpoint.
//!
//! Stores the user's preferences on the PDS.
//! Preferences are the exception to the rule of proxying app.bsky.* to the AppView.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value as JsonValue;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_legacy_auth};

/// POST /xrpc/app.bsky.actor.putPreferences - Set user preferences.
///
/// Stores the authenticated user's preferences on the PDS.
///
/// # Headers
///
/// * `Authorization: Bearer <access_jwt>` - Required
/// * `Content-Type: application/json` - Required
///
/// # Request Body
///
/// JSON object containing user preferences.
///
/// # Returns
///
/// * `200 OK` with success message on success
/// * `400 Bad Request` if the JSON is invalid
/// * `401 Unauthorized` if not authenticated
pub async fn put_preferences(
    State(state): State<Arc<PdsState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/app.bsky.actor.putPreferences".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication
    let auth_result = check_legacy_auth(&state, &headers);
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Parse the request body as JSON
    let prefs_json: JsonValue = match serde_json::from_slice(&body) {
        Ok(json) => json,
        Err(e) => {
            state.log.warning(&format!(
                "[PREFS] Failed to parse preferences JSON: {}",
                e
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "InvalidRequest",
                    "message": "Failed to parse preferences JSON"
                })),
            )
                .into_response();
        }
    };

    // Convert back to string for storage
    let prefs_string = prefs_json.to_string();

    // Check if preferences already exist
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
                    "message": "Failed to update preferences"
                })),
            )
                .into_response();
        }
    };

    // Insert or update preferences
    let result = if prefs_count == 0 {
        state.db.insert_preferences(&prefs_string)
    } else {
        state.db.update_preferences(&prefs_string)
    };

    match result {
        Ok(()) => {
            state.log.trace("[PREFS] Preferences updated successfully");
            Json(serde_json::json!({
                "message": "Preferences updated"
            }))
            .into_response()
        }
        Err(e) => {
            state.log.error(&format!(
                "[PREFS] Failed to save preferences: {}",
                e
            ));
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "InternalServerError",
                    "message": "Failed to save preferences"
                })),
            )
                .into_response()
        }
    }
}
