//! OAuth Token endpoint.
//!
//! POST /oauth/token
//!
//! Handles token exchange for authorization_code and refresh_token grants.

use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Serialize;
use sha2::{Sha256, Digest};
use uuid::Uuid;

use crate::pds::db::{OauthSession, StatisticKey};
use crate::pds::server::PdsState;

use super::dpop::validate_dpop;
use super::helpers::{get_caller_info, get_form_value, get_hostname, is_oauth_enabled};

/// Token success response.
#[derive(Serialize)]
struct TokenResponse {
    /// The access token.
    access_token: String,
    /// Token type (always "DPoP" for OAuth 2.0 DPoP).
    token_type: String,
    /// Token lifetime in seconds.
    expires_in: i32,
    /// Refresh token for obtaining new access tokens.
    refresh_token: String,
    /// Granted scopes.
    scope: String,
    /// The authenticated user's DID.
    sub: String,
}

/// POST /oauth/token
///
/// Handles token exchange requests.
pub async fn oauth_token(
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
        name: "oauth/token".to_string(),
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

    if body_str.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
    }

    // Get grant_type
    let grant_type = get_form_value(&body_str, "grant_type").unwrap_or_default();

    match grant_type.as_str() {
        "authorization_code" => {
            handle_authorization_code(&state, &headers, &body_str).await.into_response()
        }
        "refresh_token" => {
            handle_refresh_token(&state, &headers, &body_str).await.into_response()
        }
        _ => {
            state.log.warning(&format!(
                "[AUTH] [OAUTH] token: Unsupported grant type. grant_type={}",
                grant_type
            ));
            (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response()
        }
    }
}

/// Handle authorization_code grant.
async fn handle_authorization_code(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    body_str: &str,
) -> impl IntoResponse {
    // Extract required parameters
    let code = get_form_value(body_str, "code");
    let code_verifier = get_form_value(body_str, "code_verifier");
    let redirect_uri = get_form_value(body_str, "redirect_uri");
    let client_id = get_form_value(body_str, "client_id");

    let code = match code {
        Some(c) if !c.is_empty() => c,
        _ => {
            state.log.warning("[AUTH] [OAUTH] authorization_code: Missing code");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    let code_verifier = match code_verifier {
        Some(v) if !v.is_empty() => v,
        _ => {
            state.log.warning("[AUTH] [OAUTH] authorization_code: Missing code_verifier");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    let redirect_uri = match redirect_uri {
        Some(r) if !r.is_empty() => r,
        _ => {
            state.log.warning("[AUTH] [OAUTH] authorization_code: Missing redirect_uri");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    let client_id = match client_id {
        Some(c) if !c.is_empty() => c,
        _ => {
            state.log.warning("[AUTH] [OAUTH] authorization_code: Missing client_id");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    // Validate DPoP
    let dpop_header = headers.get("DPoP").and_then(|v| v.to_str().ok());
    if dpop_header.is_none() {
        state.log.warning("[AUTH] [OAUTH] authorization_code: DPoP header missing");
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
    }

    let hostname = get_hostname(state);
    let dpop_result = validate_dpop(
        dpop_header,
        "POST",
        &format!("https://{}/oauth/token", hostname),
        300,
    );

    if !dpop_result.is_valid || dpop_result.jwk_thumbprint.is_none() {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] authorization_code: DPoP validation failed. error={:?}",
            dpop_result.error
        ));
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
    }

    let jwk_thumbprint = dpop_result.jwk_thumbprint.unwrap();

    // Look up OAuth request by authorization code
    let oauth_request = match state.db.get_oauth_request_by_authorization_code(&code) {
        Ok(req) => req,
        Err(e) => {
            state.log.warning(&format!(
                "[AUTH] [OAUTH] authorization_code: Code not found or expired. code={} error={}",
                code, e
            ));
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    // Verify code_verifier (PKCE S256)
    let stored_code_challenge = get_form_value(&oauth_request.body, "code_challenge").unwrap_or_default();
    let computed_challenge = compute_s256_code_challenge(&code_verifier);

    if stored_code_challenge != computed_challenge {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] authorization_code: code_verifier mismatch. code={}",
            code
        ));
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
    }

    // Verify redirect_uri
    let stored_redirect_uri = get_form_value(&oauth_request.body, "redirect_uri").unwrap_or_default();
    if stored_redirect_uri != redirect_uri {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] authorization_code: redirect_uri mismatch. code={}",
            code
        ));
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
    }

    // Verify client_id
    let stored_client_id = get_form_value(&oauth_request.body, "client_id").unwrap_or_default();
    if stored_client_id != client_id {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] authorization_code: client_id mismatch. code={}",
            code
        ));
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
    }

    // Get scope and auth type from original request
    let scope = get_form_value(&oauth_request.body, "scope").unwrap_or_default();
    let auth_type = oauth_request.auth_type.clone().unwrap_or_else(|| "Unknown".to_string());

    // Get caller info for session
    let (ip_address, _) = get_caller_info(headers);

    // Create new OAuth session
    let session_id = format!("sessionid-{}", Uuid::new_v4());
    let refresh_token = format!("refresh-{}", Uuid::new_v4());
    let now = chrono::Utc::now();
    let refresh_expires = now + chrono::Duration::days(90);

    let oauth_session = OauthSession {
        session_id: session_id.clone(),
        client_id: client_id.clone(),
        scope: scope.clone(),
        dpop_jwk_thumbprint: jwk_thumbprint.clone(),
        refresh_token: refresh_token.clone(),
        refresh_token_expires_date: refresh_expires.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        created_date: now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        ip_address,
        auth_type,
    };

    state.log.info(&format!(
        "[AUTH] [OAUTH] authorization_code: Scope from PAR: '{}'",
        scope
    ));

    if let Err(e) = state.db.insert_oauth_session(&oauth_session) {
        state.log.error(&format!(
            "[AUTH] [OAUTH] authorization_code: Failed to insert session: {}",
            e
        ));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({})),
        )
            .into_response();
    }

    state.log.info(&format!(
        "[AUTH] [OAUTH] authorization_code: Created new OAuth session. session_id={}",
        session_id
    ));

    // Delete the OAuth request (authorization code is single-use)
    if let Err(e) = state.db.delete_oauth_request_by_authorization_code(&code) {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] authorization_code: Failed to delete request: {}",
            e
        ));
    }

    // Generate access token
    let user_did = state.db.get_config_property("UserDid").unwrap_or_default();
    let jwt_secret = state.db.get_config_property("JwtSecret").unwrap_or_default();
    let issuer = format!("https://{}", hostname);
    let expires_in_seconds = 3600; // 1 hour

    let access_token = match generate_oauth_access_token(
        &user_did,
        &issuer,
        &scope,
        &jwk_thumbprint,
        &jwt_secret,
        &client_id,
        expires_in_seconds,
    ) {
        Some(token) => token,
        None => {
            state.log.error("[AUTH] [OAUTH] authorization_code: Failed to generate access token");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({})),
            )
                .into_response();
        }
    };

    state.log.info(&format!(
        "[AUTH] [OAUTH] authorization_code: Token issued. session_id={} scope={}",
        session_id, scope
    ));

    Json(TokenResponse {
        access_token,
        token_type: "DPoP".to_string(),
        expires_in: expires_in_seconds,
        refresh_token,
        scope,
        sub: user_did,
    })
    .into_response()
}

/// Handle refresh_token grant.
async fn handle_refresh_token(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    body_str: &str,
) -> impl IntoResponse {
    // Get refresh_token
    let refresh_token = match get_form_value(body_str, "refresh_token") {
        Some(t) if !t.is_empty() => t,
        _ => {
            state.log.warning("[AUTH] [OAUTH] refresh_token: Missing refresh_token");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    // Validate DPoP
    let dpop_header = headers.get("DPoP").and_then(|v| v.to_str().ok());
    if dpop_header.is_none() {
        state.log.warning("[AUTH] [OAUTH] refresh_token: DPoP header missing");
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
    }

    let hostname = get_hostname(state);
    let dpop_result = validate_dpop(
        dpop_header,
        "POST",
        &format!("https://{}/oauth/token", hostname),
        300,
    );

    if !dpop_result.is_valid || dpop_result.jwk_thumbprint.is_none() {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] refresh_token: DPoP validation failed. error={:?}",
            dpop_result.error
        ));
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
    }

    let jwk_thumbprint = dpop_result.jwk_thumbprint.unwrap();

    // Look up OAuth session by refresh token
    let mut oauth_session = match state.db.get_oauth_session_by_refresh_token(&refresh_token) {
        Ok(session) => session,
        Err(e) => {
            state.log.warning(&format!(
                "[AUTH] [OAUTH] refresh_token: Session not found. refresh_token={} error={}",
                refresh_token, e
            ));
            return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
        }
    };

    // Verify thumbprint matches
    if !oauth_session.dpop_jwk_thumbprint.eq_ignore_ascii_case(&jwk_thumbprint) {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] refresh_token: Thumbprint mismatch. session_id={}",
            oauth_session.session_id
        ));
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
    }

    // Generate new refresh token
    let new_refresh_token = format!("refresh-{}", Uuid::new_v4());
    let refresh_expires = chrono::Utc::now() + chrono::Duration::days(30);

    oauth_session.refresh_token = new_refresh_token.clone();
    oauth_session.refresh_token_expires_date = refresh_expires.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    if let Err(e) = state.db.update_oauth_session(&oauth_session) {
        state.log.error(&format!(
            "[AUTH] [OAUTH] refresh_token: Failed to update session: {}",
            e
        ));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({})),
        )
            .into_response();
    }

    // Generate access token
    let user_did = state.db.get_config_property("UserDid").unwrap_or_default();
    let jwt_secret = state.db.get_config_property("JwtSecret").unwrap_or_default();
    let issuer = format!("https://{}", hostname);
    let expires_in_seconds = 3600; // 1 hour

    let access_token = match generate_oauth_access_token(
        &user_did,
        &issuer,
        &oauth_session.scope,
        &oauth_session.dpop_jwk_thumbprint,
        &jwt_secret,
        &oauth_session.client_id,
        expires_in_seconds,
    ) {
        Some(token) => token,
        None => {
            state.log.error("[AUTH] [OAUTH] refresh_token: Failed to generate access token");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({})),
            )
                .into_response();
        }
    };

    state.log.info(&format!(
        "[AUTH] [OAUTH] refresh_token: Token refreshed. session_id={} scope={}",
        oauth_session.session_id, oauth_session.scope
    ));

    Json(TokenResponse {
        access_token,
        token_type: "DPoP".to_string(),
        expires_in: expires_in_seconds,
        refresh_token: new_refresh_token,
        scope: oauth_session.scope,
        sub: user_did,
    })
    .into_response()
}

/// Compute S256 code challenge from code verifier (PKCE).
fn compute_s256_code_challenge(code_verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(code_verifier.as_bytes());
    let hash = hasher.finalize();
    URL_SAFE_NO_PAD.encode(hash)
}

/// Generate an OAuth access token (DPoP-bound).
fn generate_oauth_access_token(
    user_did: &str,
    issuer: &str,
    scope: &str,
    dpop_jwk_thumbprint: &str,
    jwt_secret: &str,
    client_id: &str,
    expires_in_seconds: i32,
) -> Option<String> {
    if user_did.is_empty() || dpop_jwk_thumbprint.is_empty() || jwt_secret.is_empty() {
        return None;
    }

    use jsonwebtoken::{encode, Header, Algorithm, EncodingKey};
    use serde_json::json;

    let now = chrono::Utc::now().timestamp();
    let exp = now + expires_in_seconds as i64;
    let jti = Uuid::new_v4().to_string();

    // Build claims
    let claims = json!({
        "iss": issuer,
        "sub": user_did,
        "aud": issuer,
        "iat": now,
        "exp": exp,
        "jti": jti,
        "scope": scope,
        "client_id": client_id,
        "cnf": {
            "jkt": dpop_jwk_thumbprint
        }
    });

    // Create header with typ: "at+jwt"
    let mut header = Header::new(Algorithm::HS256);
    header.typ = Some("at+jwt".to_string());

    let key = EncodingKey::from_secret(jwt_secret.as_bytes());

    encode(&header, &claims, &key).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_s256_code_challenge() {
        // Test vector from RFC 7636
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let expected = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
        assert_eq!(compute_s256_code_challenge(verifier), expected);
    }
}
