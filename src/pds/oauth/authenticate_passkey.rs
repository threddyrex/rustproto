//! Passkey authentication endpoint for OAuth flow.
//!
//! POST /oauth/authenticatepasskey
//!
//! Authenticates a user via passkey (WebAuthn assertion verification) for OAuth.

use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::ecdsa::{Signature as P256Signature, VerifyingKey as P256VerifyingKey, signature::Verifier, DerSignature};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use uuid::Uuid;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::helpers::{get_allowed_redirect_uris, get_caller_info, get_form_value, get_hostname, is_oauth_enabled, is_passkeys_enabled};

/// WebAuthn assertion response.
#[derive(Deserialize)]
struct AssertionResponse {
    /// Client data JSON (base64url encoded).
    #[serde(rename = "clientDataJSON")]
    client_data_json: String,
    /// Authenticator data (base64url encoded).
    #[serde(rename = "authenticatorData")]
    authenticator_data: String,
    /// Signature (base64url encoded).
    signature: String,
    /// User handle (base64url encoded, optional).
    #[allow(dead_code)]
    #[serde(rename = "userHandle")]
    user_handle: Option<String>,
}

/// Passkey authentication request.
#[derive(Deserialize)]
struct PasskeyAuthRequest {
    /// Credential ID (base64url encoded).
    id: String,
    /// Request URI from PAR.
    request_uri: String,
    /// OAuth client identifier.
    client_id: String,
    /// WebAuthn assertion response.
    response: AssertionResponse,
}

/// Success response.
#[derive(Serialize)]
struct PasskeyAuthSuccess {
    success: bool,
    redirect_url: String,
}

/// Error response.
#[derive(Serialize)]
struct PasskeyAuthError {
    error: String,
}

/// POST /oauth/authenticatepasskey
///
/// Authenticates a user via passkey for OAuth and returns a redirect URL.
pub async fn authenticate_passkey(
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
        name: "oauth/authenticatepasskey".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check if passkeys are enabled
    if !is_passkeys_enabled(&state.db) {
        return (
            StatusCode::NOT_FOUND,
            Json(PasskeyAuthError {
                error: "Passkeys not enabled".to_string(),
            }),
        )
            .into_response();
    }

    // Parse request body
    let request: PasskeyAuthRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            state.log.warning(&format!("[OAUTH] [PASSKEY] Invalid JSON: {}", e));
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyAuthError {
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
            Json(PasskeyAuthError {
                error: "Missing request_uri or client_id".to_string(),
            }),
        )
            .into_response();
    }

    // Verify OAuth request exists
    let mut oauth_request = match state.db.get_oauth_request(&request.request_uri) {
        Ok(req) => req,
        Err(e) => {
            state.log.warning(&format!(
                "[OAUTH] [PASSKEY] OAuth request not found. request_uri={} error={}",
                request.request_uri, e
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(PasskeyAuthError {
                    error: "Invalid or expired OAuth request".to_string(),
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
                Json(PasskeyAuthError {
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
                Json(PasskeyAuthError {
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
            Json(PasskeyAuthError {
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
                Json(PasskeyAuthError {
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
                Json(PasskeyAuthError {
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
                Json(PasskeyAuthError {
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
            Json(PasskeyAuthError {
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
                Json(PasskeyAuthError {
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
                Json(PasskeyAuthError {
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
                Json(PasskeyAuthError {
                    error: "Invalid signature encoding".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Validate authenticator data structure
    if let Err(e) = validate_authenticator_data(&authenticator_data, &hostname) {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] [PASSKEY] {} for credential {}",
            e, request.id
        ));
        return (
            StatusCode::BAD_REQUEST,
            Json(PasskeyAuthError { error: e }),
        )
            .into_response();
    }

    // Build signed data: authenticatorData || SHA256(clientDataJSON)
    let mut signed_data = authenticator_data.clone();
    let mut hasher = Sha256::new();
    hasher.update(&client_data_bytes);
    signed_data.extend_from_slice(&hasher.finalize());

    // Parse public key (stored as JWK JSON) and verify signature
    if let Err(e) = verify_jwk_signature(&passkey.public_key, &signed_data, &signature) {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] [PASSKEY] Signature verification failed: {} for credential {}",
            e, request.id
        ));
        return (
            StatusCode::UNAUTHORIZED,
            Json(PasskeyAuthError {
                error: "Invalid signature".to_string(),
            }),
        )
            .into_response();
    }

    // Delete used challenge
    let _ = state.db.delete_passkey_challenge(&challenge);

    // Validate redirect_uri against allowlist
    let redirect_uri = get_form_value(&oauth_request.body, "redirect_uri").unwrap_or_default();
    let allowed_uris = get_allowed_redirect_uris(&state.db);

    if !allowed_uris.contains(&redirect_uri) {
        state.log.warning(&format!(
            "[OAUTH] [PASSKEY] [SECURITY] redirect_uri not in allowlist. redirect_uri={}",
            redirect_uri
        ));
        return (
            StatusCode::BAD_REQUEST,
            Json(PasskeyAuthError {
                error: "Invalid redirect_uri".to_string(),
            }),
        )
            .into_response();
    }

    // Generate authorization code
    let authorization_code = format!("authcode-{}", Uuid::new_v4());
    oauth_request.authorization_code = Some(authorization_code.clone());
    oauth_request.auth_type = Some("Passkey".to_string());

    // Update OAuth request
    if let Err(e) = state.db.update_oauth_request(&oauth_request) {
        state.log.error(&format!(
            "[OAUTH] [PASSKEY] Failed to update request: {}",
            e
        ));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({})),
        )
            .into_response();
    }

    // Build redirect URL
    let state_param = get_form_value(&oauth_request.body, "state").unwrap_or_default();
    let issuer = format!("https://{}", hostname);

    let redirect_url = format!(
        "{}?code={}&state={}&iss={}",
        redirect_uri,
        urlencoding::encode(&authorization_code),
        urlencoding::encode(&state_param),
        urlencoding::encode(&issuer)
    );

    state.log.info(&format!(
        "[AUTH] [OAUTH] [PASSKEY] authSucceeded=true passkey={} redirect_url={}",
        passkey.name, redirect_url
    ));

    Json(PasskeyAuthSuccess {
        success: true,
        redirect_url,
    })
    .into_response()
}

/// Get the expected WebAuthn origin.
/// Only includes port for localhost (mirrors dnproto behavior for reverse proxy setups).
fn get_expected_origin(hostname: &str, port: i32) -> String {
    if hostname == "localhost" {
        format!("https://{}:{}", hostname, port)
    } else {
        format!("https://{}", hostname)
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

/// Verify a signature using a JWK public key (ES256 / P-256).
fn verify_jwk_signature(
    public_key_jwk: &str,
    signed_data: &[u8],
    signature: &[u8],
) -> Result<(), String> {
    // Parse JWK JSON
    let jwk: serde_json::Value = serde_json::from_str(public_key_jwk)
        .map_err(|e| format!("Invalid JWK JSON: {}", e))?;

    let kty = jwk.get("kty").and_then(|v| v.as_str()).ok_or("Missing kty in JWK")?;

    if kty != "EC" {
        return Err(format!("Unsupported key type: {}", kty));
    }

    let x_b64 = jwk.get("x").and_then(|v| v.as_str()).ok_or("Missing x in JWK")?;
    let y_b64 = jwk.get("y").and_then(|v| v.as_str()).ok_or("Missing y in JWK")?;

    let x = URL_SAFE_NO_PAD.decode(x_b64)
        .map_err(|e| format!("Invalid x coordinate encoding: {}", e))?;
    let y = URL_SAFE_NO_PAD.decode(y_b64)
        .map_err(|e| format!("Invalid y coordinate encoding: {}", e))?;

    // Create uncompressed point: 04 || x || y
    let mut point_bytes = vec![0x04];
    point_bytes.extend_from_slice(&pad_bytes(&x, 32));
    point_bytes.extend_from_slice(&pad_bytes(&y, 32));

    let verifying_key = P256VerifyingKey::from_sec1_bytes(&point_bytes)
        .map_err(|e| format!("Invalid EC public key: {}", e))?;

    // WebAuthn signatures are DER-encoded (ASN.1), try DER first then P1363
    if let Ok(der_sig) = DerSignature::from_bytes(signature) {
        verifying_key
            .verify(signed_data, &der_sig)
            .map_err(|e| format!("Signature verification failed: {}", e))?;
    } else {
        // Fall back to P1363 format (r || s, 64 bytes)
        let sig = P256Signature::from_slice(signature)
            .map_err(|e| format!("Invalid signature format: {}", e))?;
        verifying_key
            .verify(signed_data, &sig)
            .map_err(|e| format!("Signature verification failed: {}", e))?;
    }

    Ok(())
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
