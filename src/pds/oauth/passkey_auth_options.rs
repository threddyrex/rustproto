//! Passkey authentication options endpoint for OAuth flow.
//!
//! POST /oauth/passkeyauthenticationoptions
//!
//! Returns WebAuthn authentication options for passkey login.

use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::pds::db::{PasskeyChallenge, StatisticKey};
use crate::pds::server::PdsState;

use super::helpers::{get_caller_info, get_hostname, is_oauth_enabled, is_passkeys_enabled};

/// Request body for passkey authentication options.
#[derive(Deserialize)]
struct PasskeyOptionsRequest {
    /// Request URI from PAR.
    request_uri: String,
    /// OAuth client identifier.
    client_id: String,
}

/// WebAuthn credential descriptor.
#[derive(Serialize)]
struct AllowCredential {
    /// Credential type (always "public-key").
    #[serde(rename = "type")]
    cred_type: String,
    /// Credential ID (base64url encoded).
    id: String,
}

/// Passkey authentication options response.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PasskeyOptionsResponse {
    /// Challenge (base64url encoded).
    challenge: String,
    /// Relying party ID.
    rp_id: String,
    /// Timeout in milliseconds.
    timeout: i32,
    /// User verification preference.
    user_verification: String,
    /// Allowed credentials.
    allow_credentials: Vec<AllowCredential>,
}

/// Error response.
#[derive(Serialize)]
struct PasskeyError {
    error: String,
}

/// POST /oauth/passkeyauthenticationoptions
///
/// Returns WebAuthn authentication options for OAuth passkey login.
pub async fn passkey_authentication_options(
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
        name: "oauth/passkeyauthenticationoptions".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check if passkeys are enabled
    if !is_passkeys_enabled(&state.db) {
        return (
            StatusCode::NOT_FOUND,
            Json(PasskeyError {
                error: "Passkeys not enabled".to_string(),
            }),
        )
            .into_response();
    }

    // Parse request body
    let request: PasskeyOptionsRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Invalid JSON".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Validate request_uri and client_id
    if request.request_uri.is_empty() || request.client_id.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(PasskeyError {
                error: "Missing request_uri or client_id".to_string(),
            }),
        )
            .into_response();
    }

    // Verify OAuth request exists
    if state.db.get_oauth_request(&request.request_uri).is_err() {
        state.log.warning(&format!(
            "[OAUTH] [PASSKEY] OAuth request does not exist or has expired. request_uri={}",
            request.request_uri
        ));
        return (
            StatusCode::BAD_REQUEST,
            Json(PasskeyError {
                error: "Invalid or expired OAuth request".to_string(),
            }),
        )
            .into_response();
    }

    // Check if any passkeys exist
    let existing_passkeys = match state.db.get_all_passkeys() {
        Ok(passkeys) => passkeys,
        Err(e) => {
            state.log.error(&format!(
                "[OAUTH] [PASSKEY] Failed to get passkeys: {}",
                e
            ));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({})),
            )
                .into_response();
        }
    };

    if existing_passkeys.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(PasskeyError {
                error: "No passkeys registered".to_string(),
            }),
        )
            .into_response();
    }

    // Generate challenge (32 random bytes)
    let mut challenge_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut challenge_bytes);
    let challenge = URL_SAFE_NO_PAD.encode(challenge_bytes);

    // Store challenge in database
    let now = chrono::Utc::now();
    let passkey_challenge = PasskeyChallenge {
        challenge: challenge.clone(),
        created_date: now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    };

    if let Err(e) = state.db.insert_passkey_challenge(&passkey_challenge) {
        state.log.error(&format!(
            "[OAUTH] [PASSKEY] Failed to insert challenge: {}",
            e
        ));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({})),
        )
            .into_response();
    }

    // Build allowCredentials from existing passkeys
    let allow_credentials: Vec<AllowCredential> = existing_passkeys
        .iter()
        .map(|p| AllowCredential {
            cred_type: "public-key".to_string(),
            id: p.credential_id.clone(),
        })
        .collect();

    let hostname = get_hostname(&state);

    let response = PasskeyOptionsResponse {
        challenge,
        rp_id: hostname,
        timeout: 60000,
        user_verification: "preferred".to_string(),
        allow_credentials,
    };

    Json(response).into_response()
}
