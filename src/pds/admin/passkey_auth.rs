//! Admin passkey authentication handlers.
//!
//! Provides WebAuthn passkey authentication endpoints for admin login.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::ecdsa::{Signature as P256Signature, VerifyingKey as P256VerifyingKey, signature::Verifier};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use tower_cookies::{Cookie, Cookies};
use uuid::Uuid;

use crate::pds::db::{AdminSession, PasskeyChallenge, PdsDb, StatisticKey};
use crate::pds::server::PdsState;

use super::login::get_caller_info;

// =============================================================================
// PASSKEY AUTHENTICATION OPTIONS
// =============================================================================

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

/// POST /admin/passkeyauthenticationoptions
///
/// Returns WebAuthn authentication options for admin passkey login.
/// No authentication required - this is called before login.
pub async fn admin_passkey_authentication_options(
    State(state): State<Arc<PdsState>>,
) -> impl IntoResponse {
    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "admin/passkeyauthenticationoptions".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
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

    // Check if any passkeys exist
    let existing_passkeys = match state.db.get_all_passkeys() {
        Ok(passkeys) => passkeys,
        Err(e) => {
            state.log.error(&format!(
                "[ADMIN] [PASSKEY] Failed to get passkeys: {}",
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
            "[ADMIN] [PASSKEY] Failed to insert challenge: {}",
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

// =============================================================================
// PASSKEY AUTHENTICATION (VERIFY AND CREATE SESSION)
// =============================================================================

/// WebAuthn assertion response.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AssertionResponse {
    /// Client data JSON (base64url encoded).
    client_data_json: String,
    /// Authenticator data (base64url encoded).
    authenticator_data: String,
    /// Signature (base64url encoded).
    signature: String,
    /// User handle (base64url encoded, optional).
    #[allow(dead_code)]
    user_handle: Option<String>,
}

/// Passkey authentication request.
#[derive(Deserialize)]
struct PasskeyAuthRequest {
    /// Credential ID (base64url encoded).
    id: String,
    /// WebAuthn assertion response.
    response: AssertionResponse,
}

/// Success response.
#[derive(Serialize)]
struct PasskeyAuthSuccess {
    success: bool,
}

/// POST /admin/authenticatepasskey
///
/// Authenticates an admin user via passkey and creates an admin session.
pub async fn admin_authenticate_passkey(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    cookies: Cookies,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "admin/authenticatepasskey".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
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
    let request: PasskeyAuthRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            state.log.warning(&format!("[ADMIN] [PASSKEY] Invalid JSON: {}", e));
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Invalid JSON".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Decode and validate clientDataJSON
    let client_data_bytes = match URL_SAFE_NO_PAD.decode(&request.response.client_data_json) {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Invalid clientDataJSON encoding".to_string(),
                }),
            )
                .into_response();
        }
    };

    let client_data: serde_json::Value = match serde_json::from_slice(&client_data_bytes) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Invalid clientDataJSON".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Validate ceremony type
    let ceremony_type = client_data.get("type").and_then(|v| v.as_str());
    if ceremony_type != Some("webauthn.get") {
        return (
            StatusCode::BAD_REQUEST,
            Json(PasskeyError {
                error: "Invalid ceremony type".to_string(),
            }),
        )
            .into_response();
    }

    // Validate challenge
    let challenge = match client_data.get("challenge").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Missing challenge".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Look up stored challenge
    let stored_challenge = match state.db.get_passkey_challenge(&challenge) {
        Ok(Some(c)) => c,
        Ok(None) | Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Invalid or expired challenge".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Check challenge is not too old (5 minutes)
    if let Ok(created) = chrono::DateTime::parse_from_rfc3339(&stored_challenge.created_date) {
        let now = chrono::Utc::now();
        let age = now.signed_duration_since(created);
        if age > chrono::Duration::minutes(5) {
            let _ = state.db.delete_passkey_challenge(&challenge);
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Challenge expired".to_string(),
                }),
            )
                .into_response();
        }
    }

    // Validate origin
    let origin = client_data.get("origin").and_then(|v| v.as_str()).unwrap_or_default();
    let hostname = get_hostname(&state);
    let listen_port = state.db.get_config_property_int("ServerListenPort").unwrap_or(443);
    let expected_origin = get_expected_origin(&hostname, listen_port);

    if origin != expected_origin {
        return (
            StatusCode::BAD_REQUEST,
            Json(PasskeyError {
                error: format!("Invalid origin. Expected {}, got {}", expected_origin, origin),
            }),
        )
            .into_response();
    }

    // Look up passkey by credential ID
    let passkey = match state.db.get_passkey_by_credential_id(&request.id) {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Unknown credential".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Decode authenticator data and signature
    let authenticator_data = match URL_SAFE_NO_PAD.decode(&request.response.authenticator_data) {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Invalid authenticatorData encoding".to_string(),
                }),
            )
                .into_response();
        }
    };

    let signature = match URL_SAFE_NO_PAD.decode(&request.response.signature) {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Invalid signature encoding".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Validate authenticator data structure
    if let Err(e) = validate_authenticator_data(&authenticator_data, &hostname) {
        state.log.warning(&format!(
            "[AUTH] [ADMIN] [PASSKEY] {} for credential {}",
            e, request.id
        ));
        return (
            StatusCode::BAD_REQUEST,
            Json(PasskeyError { error: e }),
        )
            .into_response();
    }

    // Build signed data: authenticatorData || SHA256(clientDataJSON)
    let mut signed_data = authenticator_data.clone();
    let mut hasher = Sha256::new();
    hasher.update(&client_data_bytes);
    signed_data.extend_from_slice(&hasher.finalize());

    // Parse public key and verify signature
    let public_key_bytes = match URL_SAFE_NO_PAD.decode(&passkey.public_key) {
        Ok(b) => b,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyError {
                    error: "Invalid public key".to_string(),
                }),
            )
                .into_response();
        }
    };

    if let Err(e) = verify_cose_signature(&public_key_bytes, &signed_data, &signature) {
        state.log.warning(&format!(
            "[AUTH] [ADMIN] [PASSKEY] Signature verification failed: {} for credential {}",
            e, request.id
        ));
        return (
            StatusCode::UNAUTHORIZED,
            Json(PasskeyError {
                error: "Invalid signature".to_string(),
            }),
        )
            .into_response();
    }

    // Delete used challenge
    let _ = state.db.delete_passkey_challenge(&challenge);

    // Extract caller info
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Create admin session
    let session_id = Uuid::new_v4().to_string();
    let created_date = PdsDb::get_current_datetime_for_db();

    let session = AdminSession {
        session_id: session_id.clone(),
        ip_address: ip_address.clone(),
        user_agent,
        created_date,
        auth_type: "Passkey".to_string(),
    };

    if let Err(e) = state.db.insert_admin_session(&session) {
        state.log.error(&format!("Failed to insert admin session: {}", e));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(PasskeyError {
                error: "Failed to create session".to_string(),
            }),
        )
            .into_response();
    }

    state.log.info(&format!(
        "[AUTH] [ADMIN] [PASSKEY] authSucceeded=true passkey={} ip={}",
        passkey.name, ip_address
    ));

    // Set session cookie
    let cookie = Cookie::build(("adminSessionId", session_id))
        .path("/")
        .http_only(true)
        .secure(true)
        .same_site(tower_cookies::cookie::SameSite::Strict)
        .max_age(tower_cookies::cookie::time::Duration::hours(1))
        .build();

    cookies.add(cookie);

    Json(PasskeyAuthSuccess { success: true }).into_response()
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Check if admin dashboard is enabled.
fn is_admin_enabled(db: &PdsDb) -> bool {
    db.get_config_property_bool("FeatureEnabled_AdminDashboard")
        .unwrap_or(false)
}

/// Check if passkeys are enabled.
fn is_passkeys_enabled(db: &PdsDb) -> bool {
    db.get_config_property_bool("FeatureEnabled_Passkey")
        .unwrap_or(false)
}

/// Get the PDS hostname.
fn get_hostname(state: &PdsState) -> String {
    state
        .db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "localhost".to_string())
}

/// Get the expected WebAuthn origin.
fn get_expected_origin(hostname: &str, port: i32) -> String {
    if port == 443 {
        format!("https://{}", hostname)
    } else {
        format!("https://{}:{}", hostname, port)
    }
}

/// Validate authenticator data structure.
fn validate_authenticator_data(data: &[u8], expected_rp_id: &str) -> Result<(), String> {
    // Authenticator data must be at least 37 bytes:
    // - 32 bytes: RP ID hash
    // - 1 byte: flags
    // - 4 bytes: signature counter
    if data.len() < 37 {
        return Err("Authenticator data too short".to_string());
    }

    // Verify RP ID hash
    let mut hasher = Sha256::new();
    hasher.update(expected_rp_id.as_bytes());
    let expected_rp_id_hash = hasher.finalize();

    if &data[..32] != expected_rp_id_hash.as_slice() {
        return Err("RP ID hash mismatch".to_string());
    }

    // Check user present flag (bit 0)
    let flags = data[32];
    if flags & 0x01 == 0 {
        return Err("User not present".to_string());
    }

    Ok(())
}

/// Verify a COSE signature (ES256 / P-256).
fn verify_cose_signature(
    public_key_cose: &[u8],
    signed_data: &[u8],
    signature: &[u8],
) -> Result<(), String> {
    // Parse COSE key (simplified - assumes ES256)
    // COSE key format is CBOR encoded
    let cose_key: ciborium::Value = ciborium::from_reader(public_key_cose)
        .map_err(|e| format!("Invalid COSE key: {}", e))?;

    let cose_map = cose_key.as_map().ok_or("COSE key is not a map")?;

    // Extract x and y coordinates (labels -2 and -3)
    let mut x_bytes: Option<Vec<u8>> = None;
    let mut y_bytes: Option<Vec<u8>> = None;

    for (key, value) in cose_map {
        if let Some(k) = key.as_integer() {
            let k_i64: i64 = k.try_into().unwrap_or(0);
            match k_i64 {
                -2 => {
                    // x coordinate
                    x_bytes = value.as_bytes().map(|b| b.to_vec());
                }
                -3 => {
                    // y coordinate
                    y_bytes = value.as_bytes().map(|b| b.to_vec());
                }
                _ => {}
            }
        }
    }

    let x = x_bytes.ok_or("COSE key missing x coordinate")?;
    let y = y_bytes.ok_or("COSE key missing y coordinate")?;

    // Create uncompressed point: 04 || x || y
    let mut point_bytes = vec![0x04];
    point_bytes.extend_from_slice(&pad_bytes(&x, 32));
    point_bytes.extend_from_slice(&pad_bytes(&y, 32));

    let verifying_key = P256VerifyingKey::from_sec1_bytes(&point_bytes)
        .map_err(|e| format!("Invalid EC public key: {}", e))?;

    // Parse signature (IEEE P1363 format)
    let sig = P256Signature::from_slice(signature)
        .map_err(|e| format!("Invalid signature format: {}", e))?;

    verifying_key
        .verify(signed_data, &sig)
        .map_err(|e| format!("Signature verification failed: {}", e))
}

/// Pad byte array to expected size (prepends zeros).
fn pad_bytes(data: &[u8], size: usize) -> Vec<u8> {
    if data.len() >= size {
        return data.to_vec();
    }
    let mut padded = vec![0u8; size];
    padded[size - data.len()..].copy_from_slice(data);
    padded
}
