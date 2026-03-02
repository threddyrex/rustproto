//! com.atproto.server.refreshSession endpoint.
//!
//! Refreshes an expired access token using a valid refresh token.
//! The old refresh token is invalidated and new tokens are issued.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde::Serialize;

use crate::pds::auth::{generate_access_jwt, generate_refresh_jwt, validate_refresh_jwt};
use crate::pds::db::{LegacySession, StatisticKey};
use crate::pds::server::PdsState;

use super::auth_helpers::{extract_bearer_token, get_caller_info};

/// Lock for refresh operations to prevent race conditions.
static REFRESH_LOCK: Mutex<()> = Mutex::new(());

/// Successful response for refreshSession.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshSessionResponse {
    /// The user's DID.
    did: String,
    /// The user's handle.
    handle: String,
    /// New access token (short-lived).
    access_jwt: String,
    /// New refresh token (long-lived).
    refresh_jwt: String,
}

/// Error response for refreshSession.
#[derive(Serialize)]
pub struct RefreshSessionError {
    error: String,
    message: String,
}

/// POST /xrpc/com.atproto.server.refreshSession - Refresh tokens endpoint.
///
/// Uses a refresh token to obtain new access and refresh tokens.
/// The old refresh token is invalidated.
///
/// # Headers
///
/// * `Authorization: Bearer <refresh_jwt>` - Required
///
/// # Returns
///
/// * `200 OK` with new tokens on success
/// * `400 Bad Request` if token is missing or expired
/// * `401 Unauthorized` if token is invalid or not found in database
pub async fn refresh_session(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.refreshSession".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Extract the refresh token from Authorization header
    let original_refresh_jwt = match extract_bearer_token(&headers) {
        Some(token) => token,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(RefreshSessionError {
                    error: "InvalidRequest".to_string(),
                    message: "Missing refresh token".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Get JWT secret
    let jwt_secret = match state.db.get_config_property("JwtSecret") {
        Ok(secret) => secret,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RefreshSessionError {
                    error: "ServerError".to_string(),
                    message: "Server configuration error".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Validate the refresh JWT
    let validation_result = validate_refresh_jwt(&original_refresh_jwt, &jwt_secret);

    if !validation_result.is_valid {
        return (
            StatusCode::BAD_REQUEST,
            Json(RefreshSessionError {
                error: "ExpiredToken".to_string(),
                message: "Token has expired".to_string(),
            }),
        )
            .into_response();
    }

    // Check that the DID matches our user
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(RefreshSessionError {
                    error: "ServerError".to_string(),
                    message: "Server configuration error".to_string(),
                }),
            )
                .into_response();
        }
    };

    if validation_result.sub.as_deref() != Some(&user_did) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(RefreshSessionError {
                error: "InvalidToken".to_string(),
                message: "Token did not match expected user".to_string(),
            }),
        )
            .into_response();
    }

    // Lock to prevent race conditions with concurrent refresh requests
    let _guard = REFRESH_LOCK.lock().unwrap();

    // Check that the refresh token exists in the database
    let refresh_exists = state
        .db
        .legacy_session_exists_for_refresh_jwt(&original_refresh_jwt)
        .unwrap_or(false);

    if !refresh_exists {
        return (
            StatusCode::UNAUTHORIZED,
            Json(RefreshSessionError {
                error: "InvalidToken".to_string(),
                message: "Token not found".to_string(),
            }),
        )
            .into_response();
    }

    // Delete the old session
    if let Err(e) = state
        .db
        .delete_legacy_session_for_refresh_jwt(&original_refresh_jwt)
    {
        state.log.error(&format!(
            "[AUTH] [LEGACY] Failed to delete old session: {}",
            e
        ));
    }

    // Generate new tokens
    let pds_did = state.db.get_config_property("PdsDid").unwrap_or_default();
    let handle = state.db.get_config_property("UserHandle").unwrap_or_default();

    let access_jwt = generate_access_jwt(&user_did, &pds_did, &jwt_secret);
    let new_refresh_jwt = generate_refresh_jwt(&user_did, &pds_did, &jwt_secret);

    match (access_jwt, new_refresh_jwt) {
        (Some(access_jwt), Some(refresh_jwt)) => {
            // Store the new session
            let session = LegacySession {
                created_date: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                access_jwt: access_jwt.clone(),
                refresh_jwt: refresh_jwt.clone(),
                ip_address,
                user_agent,
            };

            if let Err(e) = state.db.create_legacy_session(&session) {
                state.log.error(&format!(
                    "[AUTH] [LEGACY] Failed to store refreshed session: {}",
                    e
                ));
            }

            (
                StatusCode::OK,
                Json(RefreshSessionResponse {
                    did: user_did,
                    handle,
                    access_jwt,
                    refresh_jwt,
                }),
            )
                .into_response()
        }
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RefreshSessionError {
                error: "ServerError".to_string(),
                message: "Failed to generate new tokens".to_string(),
            }),
        )
            .into_response(),
    }
}
