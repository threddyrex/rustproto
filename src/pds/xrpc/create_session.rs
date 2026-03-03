//! com.atproto.server.createSession endpoint.
//!
//! Creates a new session (login) using handle/email and password.
//! Returns access and refresh JWT tokens.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::pds::auth::{generate_access_jwt, generate_refresh_jwt, verify_password};
use crate::pds::db::{LegacySession, StatisticKey};
use crate::pds::server::PdsState;
use crate::ws::{ActorQueryOptions, BlueskyClient};

use super::auth_helpers::get_caller_info;

/// Request body for createSession.
#[derive(Deserialize)]
pub struct CreateSessionRequest {
    /// Handle or email address of the account.
    identifier: String,
    /// Password for the account.
    password: String,
}

/// Successful response for createSession.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSessionResponse {
    /// The user's DID.
    did: String,
    /// The user's handle.
    handle: String,
    /// Access token (short-lived).
    access_jwt: String,
    /// Refresh token (long-lived).
    refresh_jwt: String,
}

/// Error response for createSession.
#[derive(Serialize)]
pub struct CreateSessionError {
    error: String,
    message: String,
}

/// POST /xrpc/com.atproto.server.createSession - Login endpoint.
///
/// Authenticates a user and returns session tokens.
///
/// # Request Body
///
/// * `identifier` - Handle or email address
/// * `password` - Account password
///
/// # Returns
///
/// * `200 OK` with session tokens on success
/// * `400 Bad Request` if parameters are invalid
/// * `401 Unauthorized` if credentials are incorrect
pub async fn create_session(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<CreateSessionRequest>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.createSession".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Validate input
    if body.identifier.is_empty() || body.password.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(CreateSessionError {
                error: "InvalidRequest".to_string(),
                message: "Error: invalid params.".to_string(),
            }),
        )
            .into_response();
    }

    // Resolve actor info using BlueskyClient
    let client = BlueskyClient::new();
    let options = ActorQueryOptions {
        resolve_handle_via_bluesky: true,
        ..Default::default()
    };

    let actor_info = match client.resolve_actor_info(&body.identifier, Some(options)).await {
        Ok(info) => info,
        Err(e) => {
            state.log.warning(&format!(
                "[AUTH] [LEGACY] Failed to resolve actor '{}': {}",
                body.identifier, e
            ));
            // Return generic auth failure to not leak information
            return (
                StatusCode::OK,
                Json(CreateSessionResponse {
                    did: String::new(),
                    handle: String::new(),
                    access_jwt: String::new(),
                    refresh_jwt: String::new(),
                }),
            )
                .into_response();
        }
    };

    // Get the DID - prefer the resolved DID field, then fallback to other resolved DIDs
    let actor_did = actor_info.did.clone()
        .or_else(|| actor_info.did_bsky.clone())
        .or_else(|| actor_info.did_dns.clone())
        .or_else(|| actor_info.did_http.clone());
    let actor_handle = actor_info.handle.clone().unwrap_or_default();

    // Check if this is our user
    let our_user_did = state.db.get_config_property("UserDid").unwrap_or_default();
    let actor_exists = actor_did.as_ref().map(|d| d == &our_user_did).unwrap_or(false);

    if !actor_exists {
        state.log.warning(&format!(
            "[AUTH] [LEGACY] Actor mismatch. resolved={:?} expected={} ip={} userAgent={}",
            actor_did, our_user_did, ip_address, user_agent
        ));
    }

    // Verify password
    let stored_hash = state.db.get_config_property("UserHashedPassword").ok();
    let password_matches = verify_password(stored_hash.as_deref(), &body.password);

    if !password_matches {
        state.log.warning(&format!(
            "[AUTH] [LEGACY] Password mismatch. hash_exists={} ip={} userAgent={}",
            stored_hash.is_some(), ip_address, user_agent
        ));
    }

    // Generate tokens only if both actor exists and password matches
    if actor_exists && password_matches {
        let pds_did = state.db.get_config_property("PdsDid").unwrap_or_default();
        let jwt_secret = state.db.get_config_property("JwtSecret").unwrap_or_default();

        let access_jwt = generate_access_jwt(&our_user_did, &pds_did, &jwt_secret);
        let refresh_jwt = generate_refresh_jwt(&our_user_did, &pds_did, &jwt_secret);

        if let (Some(access_jwt), Some(refresh_jwt)) = (access_jwt, refresh_jwt) {
            state.log.info(&format!(
                "[AUTH] [LEGACY] Successful login. ip={} userAgent={}",
                ip_address, user_agent
            ));

            // Store the session in database
            let session = LegacySession {
                created_date: Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                access_jwt: access_jwt.clone(),
                refresh_jwt: refresh_jwt.clone(),
                ip_address: ip_address.clone(),
                user_agent: user_agent.clone(),
            };

            if let Err(e) = state.db.create_legacy_session(&session) {
                state.log.error(&format!(
                    "[AUTH] [LEGACY] Failed to store session: {}",
                    e
                ));
            }

            return (
                StatusCode::OK,
                Json(CreateSessionResponse {
                    did: our_user_did,
                    handle: actor_handle,
                    access_jwt,
                    refresh_jwt,
                }),
            )
                .into_response();
        }
    }

    // Return empty response for failed login (matches dnproto behavior - returns 200 with empty tokens)
    (
        StatusCode::OK,
        Json(CreateSessionResponse {
            did: actor_did.unwrap_or_default(),
            handle: actor_handle,
            access_jwt: String::new(),
            refresh_jwt: String::new(),
        }),
    )
        .into_response()
}
