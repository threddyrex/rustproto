//! Passkey registration endpoint for OAuth.
//!
//! GET /oauth/register-passkey - Display registration form
//! POST /oauth/passkeyregistrationoptions - Get WebAuthn registration options
//! POST /oauth/registerpasskey - Complete passkey registration

use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tower_cookies::Cookies;

use crate::pds::db::{Passkey, PasskeyChallenge, StatisticKey};
use crate::pds::server::PdsState;

use super::helpers::{get_hostname, html_encode, is_oauth_enabled, is_passkeys_enabled};

// =============================================================================
// GET /oauth/register-passkey - REGISTRATION FORM
// =============================================================================

/// GET /oauth/register-passkey
///
/// Displays the passkey registration form.
/// Requires admin session authentication.
pub async fn register_passkey_get(
    State(state): State<Arc<PdsState>>,
    cookies: Cookies,
) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Html("OAuth is not enabled".to_string())).into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "oauth/register-passkey GET".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check if passkeys are enabled
    if !is_passkeys_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Html("Passkeys are not enabled".to_string())).into_response();
    }

    // Verify admin session
    if !verify_admin_session(&state, &cookies) {
        return (
            StatusCode::UNAUTHORIZED,
            Html("Admin authentication required. <a href=\"/admin/login\">Login</a>".to_string()),
        )
            .into_response();
    }

    let hostname = get_hostname(&state);
    let html = generate_register_passkey_html(&hostname);
    Html(html).into_response()
}

/// Verify admin session from cookies.
fn verify_admin_session(state: &Arc<PdsState>, cookies: &Cookies) -> bool {
    let session_id = match cookies.get("adminSessionId") {
        Some(cookie) => cookie.value().to_string(),
        None => return false,
    };

    // Check if session exists in database
    match state.db.get_all_admin_sessions() {
        Ok(sessions) => sessions.iter().any(|s| s.session_id == session_id),
        Err(_) => false,
    }
}

/// Generate the HTML registration form.
fn generate_register_passkey_html(hostname: &str) -> String {
    let safe_hostname = html_encode(hostname);

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Register Passkey - {hostname}</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #15202b; 
            color: #e7e9ea; 
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 20px;
        }}
        .container {{ 
            background-color: #192734;
            border-radius: 16px;
            padding: 32px;
            width: 100%;
            max-width: 400px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        }}
        h1 {{ 
            font-size: 24px; 
            margin-bottom: 8px;
            text-align: center;
        }}
        .subtitle {{
            color: #8899a6;
            text-align: center;
            margin-bottom: 24px;
            font-size: 14px;
        }}
        .form-group {{
            margin-bottom: 20px;
        }}
        label {{
            display: block;
            margin-bottom: 8px;
            color: #8899a6;
            font-size: 14px;
        }}
        input[type="text"] {{
            width: 100%;
            padding: 12px 16px;
            background-color: #253341;
            border: 1px solid #38444d;
            border-radius: 8px;
            color: #e7e9ea;
            font-size: 16px;
        }}
        input[type="text"]:focus {{
            outline: none;
            border-color: #1d9bf0;
        }}
        .btn {{
            width: 100%;
            padding: 14px;
            background-color: #1d9bf0;
            border: none;
            border-radius: 25px;
            color: white;
            font-size: 16px;
            font-weight: bold;
            cursor: pointer;
            transition: background-color 0.2s;
        }}
        .btn:hover:not(:disabled) {{
            background-color: #1a8cd8;
        }}
        .btn:disabled {{
            background-color: #38444d;
            cursor: not-allowed;
        }}
        .error-msg {{
            color: #f4212e;
            font-size: 14px;
            margin-top: 16px;
            text-align: center;
            display: none;
        }}
        .success-msg {{
            color: #00ba7c;
            font-size: 14px;
            margin-top: 16px;
            text-align: center;
            display: none;
        }}
        .back-link {{
            display: block;
            text-align: center;
            margin-top: 20px;
            color: #1d9bf0;
            text-decoration: none;
            font-size: 14px;
        }}
        .back-link:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
<div class="container">
    <h1>Register Passkey</h1>
    <p class="subtitle">Add a new passkey for passwordless authentication</p>
    
    <div class="form-group">
        <label for="passkey-name">Passkey Name</label>
        <input type="text" id="passkey-name" placeholder="e.g., MacBook Pro, YubiKey" />
    </div>
    
    <button type="button" id="register-btn" class="btn" onclick="registerPasskey()">
        Register Passkey
    </button>
    
    <div id="error-msg" class="error-msg"></div>
    <div id="success-msg" class="success-msg"></div>
    
    <a href="/admin/passkeys" class="back-link">&larr; Back to Passkeys</a>
</div>

<script>
    // Base64URL encoding/decoding utilities
    function base64urlToBuffer(base64url) {{
        const base64 = base64url.replace(/-/g, '+').replace(/_/g, '/');
        const padding = '='.repeat((4 - base64.length % 4) % 4);
        const binary = atob(base64 + padding);
        const bytes = new Uint8Array(binary.length);
        for (let i = 0; i < binary.length; i++) {{
            bytes[i] = binary.charCodeAt(i);
        }}
        return bytes.buffer;
    }}
    
    function bufferToBase64url(buffer) {{
        const bytes = new Uint8Array(buffer);
        let binary = '';
        for (let i = 0; i < bytes.length; i++) {{
            binary += String.fromCharCode(bytes[i]);
        }}
        const base64 = btoa(binary);
        return base64.replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
    }}
    
    async function registerPasskey() {{
        const btn = document.getElementById('register-btn');
        const errorDiv = document.getElementById('error-msg');
        const successDiv = document.getElementById('success-msg');
        const nameInput = document.getElementById('passkey-name');
        
        const passkeyName = nameInput.value.trim();
        if (!passkeyName) {{
            errorDiv.textContent = 'Please enter a name for this passkey';
            errorDiv.style.display = 'block';
            successDiv.style.display = 'none';
            return;
        }}
        
        errorDiv.style.display = 'none';
        successDiv.style.display = 'none';
        btn.disabled = true;
        btn.textContent = 'Registering...';
        
        try {{
            // Step 1: Get registration options from server
            const optionsResponse = await fetch('/oauth/passkeyregistrationoptions', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{ name: passkeyName }})
            }});
            
            if (!optionsResponse.ok) {{
                const err = await optionsResponse.json();
                throw new Error(err.error || 'Failed to get registration options');
            }}
            
            const options = await optionsResponse.json();
            
            // Step 2: Create credential using WebAuthn
            const publicKeyCredentialCreationOptions = {{
                challenge: base64urlToBuffer(options.challenge),
                rp: {{
                    name: options.rpName,
                    id: options.rpId
                }},
                user: {{
                    id: base64urlToBuffer(options.userId),
                    name: options.userName,
                    displayName: options.userDisplayName
                }},
                pubKeyCredParams: options.pubKeyCredParams,
                timeout: options.timeout,
                attestation: options.attestation,
                authenticatorSelection: options.authenticatorSelection
            }};
            
            const credential = await navigator.credentials.create({{
                publicKey: publicKeyCredentialCreationOptions
            }});
            
            if (!credential) {{
                throw new Error('Failed to create credential');
            }}
            
            // Step 3: Send credential to server
            const attestationResponse = credential.response;
            
            const registerResponse = await fetch('/oauth/registerpasskey', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                    name: passkeyName,
                    challenge: options.challenge,
                    id: bufferToBase64url(credential.rawId),
                    response: {{
                        clientDataJSON: bufferToBase64url(attestationResponse.clientDataJSON),
                        attestationObject: bufferToBase64url(attestationResponse.attestationObject)
                    }}
                }})
            }});
            
            if (!registerResponse.ok) {{
                const err = await registerResponse.json();
                throw new Error(err.error || 'Failed to register passkey');
            }}
            
            // Success!
            successDiv.textContent = 'Passkey registered successfully! Redirecting...';
            successDiv.style.display = 'block';
            btn.textContent = 'Success!';
            
            setTimeout(() => {{
                window.location.href = '/admin/passkeys';
            }}, 1500);
            
        }} catch (err) {{
            console.error('Passkey registration error:', err);
            errorDiv.textContent = err.message || 'Registration failed';
            errorDiv.style.display = 'block';
            btn.disabled = false;
            btn.textContent = 'Register Passkey';
        }}
    }}
</script>
</body>
</html>"#,
        hostname = safe_hostname,
    )
}

// =============================================================================
// POST /oauth/passkeyregistrationoptions - REGISTRATION OPTIONS
// =============================================================================

/// Request for passkey registration options.
#[derive(Deserialize)]
struct RegistrationOptionsRequest {
    /// User-friendly name for the passkey.
    name: String,
}

/// Public key credential parameters.
#[derive(Serialize)]
struct PubKeyCredParam {
    #[serde(rename = "type")]
    cred_type: String,
    alg: i32,
}

/// Authenticator selection criteria.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthenticatorSelection {
    authenticator_attachment: Option<String>,
    resident_key: String,
    user_verification: String,
}

/// Registration options response.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RegistrationOptionsResponse {
    /// Challenge (base64url encoded).
    challenge: String,
    /// Relying party name.
    rp_name: String,
    /// Relying party ID.
    rp_id: String,
    /// User ID (base64url encoded).
    user_id: String,
    /// User name.
    user_name: String,
    /// User display name.
    user_display_name: String,
    /// Supported public key algorithms.
    pub_key_cred_params: Vec<PubKeyCredParam>,
    /// Timeout in milliseconds.
    timeout: i32,
    /// Attestation preference.
    attestation: String,
    /// Authenticator selection criteria.
    authenticator_selection: AuthenticatorSelection,
}

/// Error response.
#[derive(Serialize)]
struct RegistrationError {
    error: String,
}

/// POST /oauth/passkeyregistrationoptions
///
/// Returns WebAuthn registration options for creating a new passkey.
/// Requires admin session authentication.
pub async fn passkey_registration_options(
    State(state): State<Arc<PdsState>>,
    cookies: Cookies,
    body: Bytes,
) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "oauth/passkeyregistrationoptions".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check if passkeys are enabled
    if !is_passkeys_enabled(&state.db) {
        return (
            StatusCode::NOT_FOUND,
            Json(RegistrationError {
                error: "Passkeys not enabled".to_string(),
            }),
        )
            .into_response();
    }

    // Verify admin session
    if !verify_admin_session(&state, &cookies) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(RegistrationError {
                error: "Admin authentication required".to_string(),
            }),
        )
            .into_response();
    }

    // Parse request body
    let request: RegistrationOptionsRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(RegistrationError {
                    error: "Invalid request body".to_string(),
                }),
            )
                .into_response();
        }
    };

    if request.name.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(RegistrationError {
                error: "Passkey name is required".to_string(),
            }),
        )
            .into_response();
    }

    // Generate challenge (32 random bytes)
    let mut challenge_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut challenge_bytes);
    let challenge = URL_SAFE_NO_PAD.encode(challenge_bytes);

    // Generate user ID (16 random bytes)
    let mut user_id_bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut user_id_bytes);
    let user_id = URL_SAFE_NO_PAD.encode(user_id_bytes);

    // Store challenge in database
    let now = chrono::Utc::now();
    let passkey_challenge = PasskeyChallenge {
        challenge: challenge.clone(),
        created_date: now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    };

    if let Err(e) = state.db.insert_passkey_challenge(&passkey_challenge) {
        state.log.error(&format!(
            "[OAUTH] [PASSKEY] Failed to insert registration challenge: {}",
            e
        ));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RegistrationError {
                error: "Failed to create challenge".to_string(),
            }),
        )
            .into_response();
    }

    let hostname = get_hostname(&state);

    let response = RegistrationOptionsResponse {
        challenge,
        rp_name: format!("PDS {}", hostname),
        rp_id: hostname,
        user_id,
        user_name: "admin".to_string(),
        user_display_name: "Administrator".to_string(),
        pub_key_cred_params: vec![
            PubKeyCredParam {
                cred_type: "public-key".to_string(),
                alg: -7, // ES256 (ECDSA with P-256 and SHA-256)
            },
            PubKeyCredParam {
                cred_type: "public-key".to_string(),
                alg: -257, // RS256 (RSASSA-PKCS1-v1_5 with SHA-256)
            },
        ],
        timeout: 60000,
        attestation: "none".to_string(),
        authenticator_selection: AuthenticatorSelection {
            authenticator_attachment: None,
            resident_key: "preferred".to_string(),
            user_verification: "preferred".to_string(),
        },
    };

    Json(response).into_response()
}

// =============================================================================
// POST /oauth/registerpasskey - COMPLETE REGISTRATION
// =============================================================================

/// Attestation response from WebAuthn credential creation.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttestationResponse {
    /// Client data JSON (base64url encoded).
    #[allow(dead_code)]
    client_data_json: String,
    /// Attestation object (base64url encoded).
    attestation_object: String,
}

/// Passkey registration request.
#[derive(Deserialize)]
struct RegisterPasskeyRequest {
    /// User-friendly name for the passkey.
    name: String,
    /// Challenge we sent (for verification).
    challenge: String,
    /// Credential ID (base64url encoded).
    id: String,
    /// WebAuthn attestation response.
    response: AttestationResponse,
}

/// Success response.
#[derive(Serialize)]
struct RegisterPasskeySuccess {
    success: bool,
}

/// POST /oauth/registerpasskey
///
/// Completes passkey registration by storing the credential.
/// Requires admin session authentication.
pub async fn register_passkey_post(
    State(state): State<Arc<PdsState>>,
    cookies: Cookies,
    body: Bytes,
) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "oauth/registerpasskey".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check if passkeys are enabled
    if !is_passkeys_enabled(&state.db) {
        return (
            StatusCode::NOT_FOUND,
            Json(RegistrationError {
                error: "Passkeys not enabled".to_string(),
            }),
        )
            .into_response();
    }

    // Verify admin session
    if !verify_admin_session(&state, &cookies) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(RegistrationError {
                error: "Admin authentication required".to_string(),
            }),
        )
            .into_response();
    }

    // Parse request body
    let request: RegisterPasskeyRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            state.log.warning(&format!(
                "[OAUTH] [PASSKEY] Failed to parse registration request: {}",
                e
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(RegistrationError {
                    error: "Invalid request body".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Verify the challenge exists in our database
    match state.db.get_passkey_challenge(&request.challenge) {
        Ok(_) => {}
        Err(e) => {
            state.log.warning(&format!(
                "[OAUTH] [PASSKEY] Challenge not found: {}",
                e
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(RegistrationError {
                    error: "Invalid or expired challenge".to_string(),
                }),
            )
                .into_response();
        }
    }

    // Delete the challenge (one-time use)
    if let Err(e) = state.db.delete_passkey_challenge(&request.challenge) {
        state.log.warning(&format!(
            "[OAUTH] [PASSKEY] Failed to delete challenge: {}",
            e
        ));
    }

    // Parse attestation object to extract public key
    let attestation_bytes = match URL_SAFE_NO_PAD.decode(&request.response.attestation_object) {
        Ok(bytes) => bytes,
        Err(e) => {
            state.log.warning(&format!(
                "[OAUTH] [PASSKEY] Failed to decode attestation object: {}",
                e
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(RegistrationError {
                    error: "Invalid attestation object".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Parse the CBOR attestation object to extract public key
    let public_key_jwk = match extract_public_key_from_attestation(&attestation_bytes) {
        Ok(jwk) => jwk,
        Err(e) => {
            state.log.warning(&format!(
                "[OAUTH] [PASSKEY] Failed to extract public key: {}",
                e
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(RegistrationError {
                    error: format!("Failed to extract public key: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Store the passkey
    let now = chrono::Utc::now();
    let passkey = Passkey {
        name: request.name.trim().to_string(),
        created_date: now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        credential_id: request.id,
        public_key: public_key_jwk,
    };

    if let Err(e) = state.db.insert_passkey(&passkey) {
        state.log.error(&format!(
            "[OAUTH] [PASSKEY] Failed to insert passkey: {}",
            e
        ));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(RegistrationError {
                error: "Failed to store passkey".to_string(),
            }),
        )
            .into_response();
    }

    state.log.info(&format!(
        "[OAUTH] [PASSKEY] Passkey registered: name={}",
        passkey.name
    ));

    Json(RegisterPasskeySuccess { success: true }).into_response()
}

/// Extract the public key from a WebAuthn attestation object.
///
/// The attestation object is CBOR-encoded and contains:
/// - fmt: attestation format
/// - authData: authenticator data containing the public key
/// - attStmt: attestation statement (not used for "none" attestation)
fn extract_public_key_from_attestation(attestation_bytes: &[u8]) -> Result<String, String> {
    // Parse CBOR attestation object
    let attestation: ciborium::Value = ciborium::from_reader(attestation_bytes)
        .map_err(|e| format!("Failed to parse CBOR: {}", e))?;

    // Extract authData
    let auth_data = attestation
        .as_map()
        .and_then(|m| {
            m.iter()
                .find(|(k, _)| k.as_text() == Some("authData"))
                .map(|(_, v)| v)
        })
        .and_then(|v| v.as_bytes())
        .ok_or("Missing authData in attestation")?;

    // AuthData structure:
    // - 32 bytes: rpIdHash
    // - 1 byte: flags
    // - 4 bytes: signCount
    // - variable: attestedCredentialData (if AT flag is set)
    //   - 16 bytes: aaguid
    //   - 2 bytes: credentialIdLength (big endian)
    //   - credentialIdLength bytes: credentialId
    //   - variable: credentialPublicKey (COSE encoded)

    if auth_data.len() < 37 {
        return Err("AuthData too short".to_string());
    }

    let flags = auth_data[32];
    let has_attested_credential_data = (flags & 0x40) != 0;

    if !has_attested_credential_data {
        return Err("No attested credential data in authData".to_string());
    }

    // Skip rpIdHash (32) + flags (1) + signCount (4) + aaguid (16) = 53 bytes
    if auth_data.len() < 55 {
        return Err("AuthData too short for credential ID length".to_string());
    }

    let cred_id_len = u16::from_be_bytes([auth_data[53], auth_data[54]]) as usize;
    let cose_key_offset = 55 + cred_id_len;

    if auth_data.len() <= cose_key_offset {
        return Err("AuthData too short for COSE key".to_string());
    }

    let cose_key_bytes = &auth_data[cose_key_offset..];

    // Parse COSE key
    let cose_key: ciborium::Value = ciborium::from_reader(cose_key_bytes)
        .map_err(|e| format!("Failed to parse COSE key: {}", e))?;

    // Convert COSE key to JWK
    cose_to_jwk(&cose_key)
}

/// Convert a COSE key to JWK format.
///
/// COSE key parameters (for EC2/P-256):
/// - 1 (kty): 2 (EC2)
/// - 3 (alg): -7 (ES256)
/// - -1 (crv): 1 (P-256)
/// - -2 (x): x coordinate
/// - -3 (y): y coordinate
fn cose_to_jwk(cose_key: &ciborium::Value) -> Result<String, String> {
    let map = cose_key
        .as_map()
        .ok_or("COSE key is not a map")?;

    // Helper to get integer value
    let get_int = |key: i128| -> Option<i128> {
        map.iter()
            .find(|(k, _)| k.as_integer().map(|i| i128::from(i)) == Some(key))
            .and_then(|(_, v)| v.as_integer().map(|i| i128::from(i)))
    };

    // Helper to get bytes value
    let get_bytes = |key: i128| -> Option<&[u8]> {
        map.iter()
            .find(|(k, _)| k.as_integer().map(|i| i128::from(i)) == Some(key))
            .and_then(|(_, v)| v.as_bytes())
            .map(|v| v.as_slice())
    };

    let kty = get_int(1).ok_or("Missing kty in COSE key")?;

    if kty == 2 {
        // EC2 key (ECDSA)
        let crv = get_int(-1).ok_or("Missing crv in COSE key")?;
        let x = get_bytes(-2).ok_or("Missing x in COSE key")?;
        let y = get_bytes(-3).ok_or("Missing y in COSE key")?;

        let crv_name = match crv {
            1 => "P-256",
            2 => "P-384",
            3 => "P-521",
            _ => return Err(format!("Unsupported curve: {}", crv)),
        };

        let jwk = serde_json::json!({
            "kty": "EC",
            "crv": crv_name,
            "x": URL_SAFE_NO_PAD.encode(x),
            "y": URL_SAFE_NO_PAD.encode(y)
        });

        Ok(jwk.to_string())
    } else if kty == 3 {
        // RSA key
        let n = get_bytes(-1).ok_or("Missing n in COSE key")?;
        let e = get_bytes(-2).ok_or("Missing e in COSE key")?;

        let jwk = serde_json::json!({
            "kty": "RSA",
            "n": URL_SAFE_NO_PAD.encode(n),
            "e": URL_SAFE_NO_PAD.encode(e)
        });

        Ok(jwk.to_string())
    } else {
        Err(format!("Unsupported key type: {}", kty))
    }
}
