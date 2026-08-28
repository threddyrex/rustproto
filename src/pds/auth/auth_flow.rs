//! Authentication flow for XRPC endpoints.
//!
//! This module provides utilities for extracting and validating authentication
//! from HTTP requests in XRPC handlers.
//!
//! Supported authentication types:
//! - Legacy: Original AT Protocol auth using handle/password with Bearer tokens
//! - OAuth: DPoP-bound OAuth 2.0 tokens with at+jwt type
//! - Service: Service auth tokens (JWT signed by remote service's signing key)
//! - SpaceCredential: DPoP-bound space-credential JWTs (atproto-space-credential+jwt)
//!   issued by this host for permissioned spaces. Never allowed by default; an
//!   endpoint must explicitly opt in.

use std::sync::Arc;

use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Serialize;

use crate::log::logger;
use crate::pds::auth::{validate_access_jwt, verify_service_auth_token, SPACE_CREDENTIAL_TYP};
use crate::pds::oauth::{is_oauth_enabled, validate_dpop, get_hostname, token_fp};
use crate::pds::server::PdsState;

/// Types of authentication supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthType {
    /// Legacy authentication using Bearer tokens (handle/password).
    Legacy,
    /// OAuth 2.0 authentication using DPoP-bound tokens.
    Oauth,
    /// Service authentication using JWTs signed by remote services.
    Service,
    /// Space credential authentication using DPoP-bound space-credential JWTs.
    ///
    /// Distinguished from all other types by the JWT `typ` header
    /// `atproto-space-credential+jwt`. Never included in the defaults: an
    /// endpoint must explicitly list `SpaceCredential` to accept it.
    SpaceCredential,
}

/// Error response for authentication failures.
#[derive(Serialize)]
pub struct AuthError {
    pub error: String,
    pub message: String,
}

/// Result of authentication check.
#[allow(dead_code)]
pub struct AuthResult {
    /// Whether the user is authenticated.
    pub is_authenticated: bool,
    /// The user's DID if authenticated.
    pub user_did: Option<String>,
    /// Error message if authentication failed.
    pub error: Option<String>,
    /// Whether the token was valid but expired.
    pub is_expired: bool,
    /// The space URI (`sub`) granted by a validated space credential, if the
    /// caller authenticated with `AuthType::SpaceCredential`. `None` for all
    /// other auth types. Endpoints must confirm this matches the space being
    /// accessed.
    pub space_uri: Option<String>,
    /// The type of authentication used, if any.
    pub auth_type: Option<AuthType>,
}




/// Check if the user is authenticated using any of the allowed auth types.
///
/// Entry point for user auth checks.
/// 
/// By default, allows both Legacy and OAuth authentication.
/// The function checks in order: OAuth (if DPoP token present), Service Auth, then Legacy.
///
/// # Arguments
///
/// * `state` - The PDS state containing database access
/// * `headers` - The HTTP headers from the request
/// * `allowed_auth_types` - Which auth types are allowed (defaults to Legacy + OAuth)
/// * `http_method` - The HTTP method of the request (needed for OAuth DPoP validation)
/// * `request_path` - The path of the request (needed for OAuth DPoP validation)
///
/// # Returns
///
/// An AuthResult indicating whether the user is authenticated.
pub fn check_user_auth(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    allowed_auth_types: Option<&[AuthType]>,
    http_method: &str,
    request_path: &str,
) -> AuthResult {
    check_user_auth_with_lxm(state, headers, allowed_auth_types, http_method, request_path, None)
}




/// Check if the user is authenticated using any of the allowed auth types, with lxm validation.
///
/// By default, allows both Legacy and OAuth authentication.
/// The function checks in order: OAuth (if DPoP token present), Service Auth, then Legacy.
///
/// # Arguments
///
/// * `state` - The PDS state containing database access
/// * `headers` - The HTTP headers from the request
/// * `allowed_auth_types` - Which auth types are allowed (defaults to Legacy + OAuth)
/// * `http_method` - The HTTP method of the request (needed for OAuth DPoP validation)
/// * `request_path` - The path of the request (needed for OAuth DPoP validation)
/// * `expected_lxm` - Optional: the expected lxm claim for service auth validation
///
/// # Returns
///
/// An AuthResult indicating whether the user is authenticated.
pub fn check_user_auth_with_lxm(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    allowed_auth_types: Option<&[AuthType]>,
    http_method: &str,
    request_path: &str,
    expected_lxm: Option<&str>,
) -> AuthResult {
    let default_types = [AuthType::Legacy, AuthType::Oauth];
    let allowed = allowed_auth_types.unwrap_or(&default_types);

    let ip = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Check if this looks like an OAuth request (has DPoP header and DPoP auth scheme)
    if is_oauth_token_request(headers) {
        if !allowed.contains(&AuthType::Oauth) {
            logger().info(&format!(
                "[AUTH] ip={} type=oauth authenticated=false error=oauth_not_allowed",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some("OAuth authentication not allowed for this endpoint".to_string()),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::Oauth),
            };
        }

        return check_oauth_auth(state, headers, http_method, request_path);
    }

    // Check if this looks like a Space Credential request (JWT typ = space credential)
    if is_space_credential_request(headers) {
        if !allowed.contains(&AuthType::SpaceCredential) {
            logger().info(&format!(
                "[AUTH] ip={} type=space_credential authenticated=false error=space_credential_not_allowed",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some(
                    "Space credential authentication not allowed for this endpoint".to_string(),
                ),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::SpaceCredential),
            };
        }

        return check_space_credential(state, headers, http_method, request_path);
    }

    // Check if this looks like a Service Auth request (ES256 JWT with lxm claim)
    if is_service_auth_request(headers) {
        if !allowed.contains(&AuthType::Service) {
            logger().info(&format!(
                "[AUTH] ip={} type=service authenticated=false error=service_auth_not_allowed",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some("Service authentication not allowed for this endpoint".to_string()),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::Service),
            };
        }

        return check_service_auth(state, headers, expected_lxm);
    }

    // Otherwise try legacy auth
    if allowed.contains(&AuthType::Legacy) {
        return check_legacy_auth(state, headers);
    }

    // No valid auth type
    logger().info(&format!(
        "[AUTH] ip={} authenticated=false error=no_valid_auth_type",
        ip
    ));
    AuthResult {
        is_authenticated: false,
        user_did: None,
        error: Some("No valid authentication provided".to_string()),
        is_expired: false,
        space_uri: None,
        auth_type: None,
    }
}

/// Async version of check_user_auth_with_lxm that properly awaits service auth DID resolution.
///
/// This should be used when Service auth is allowed and the caller is in an async context.
pub async fn check_user_auth_with_lxm_async(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    allowed_auth_types: Option<&[AuthType]>,
    http_method: &str,
    request_path: &str,
    expected_lxm: Option<&str>,
) -> AuthResult {
    let default_types = [AuthType::Legacy, AuthType::Oauth];
    let allowed = allowed_auth_types.unwrap_or(&default_types);

    let ip = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Check if this looks like an OAuth request (has DPoP header and DPoP auth scheme)
    if is_oauth_token_request(headers) {
        if !allowed.contains(&AuthType::Oauth) {
            logger().info(&format!(
                "[AUTH] ip={} type=oauth authenticated=false error=oauth_not_allowed",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some("OAuth authentication not allowed for this endpoint".to_string()),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::Oauth),
            };
        }

        return check_oauth_auth(state, headers, http_method, request_path);
    }

    // Check if this looks like a Space Credential request (JWT typ = space credential)
    if is_space_credential_request(headers) {
        if !allowed.contains(&AuthType::SpaceCredential) {
            logger().info(&format!(
                "[AUTH] ip={} type=space_credential authenticated=false error=space_credential_not_allowed",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some(
                    "Space credential authentication not allowed for this endpoint".to_string(),
                ),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::SpaceCredential),
            };
        }

        // Space credential verification is fully local (no remote DID resolution).
        return check_space_credential(state, headers, http_method, request_path);
    }

    // Check if this looks like a Service Auth request (ES256 JWT with lxm claim)
    if is_service_auth_request(headers) {
        if !allowed.contains(&AuthType::Service) {
            logger().info(&format!(
                "[AUTH] ip={} type=service authenticated=false error=service_auth_not_allowed",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some("Service authentication not allowed for this endpoint".to_string()),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::Service),
            };
        }

        // Use async version for service auth to avoid blocking
        return check_service_auth_async(state, headers, expected_lxm).await;
    }

    // Otherwise try legacy auth
    if allowed.contains(&AuthType::Legacy) {
        return check_legacy_auth(state, headers);
    }

    // No valid auth type
    logger().info(&format!(
        "[AUTH] ip={} authenticated=false error=no_valid_auth_type",
        ip
    ));
    AuthResult {
        is_authenticated: false,
        user_did: None,
        error: Some("No valid authentication provided".to_string()),
        is_expired: false,
        space_uri: None,
        auth_type: None,
    }
}







/// Extract the Bearer token from the Authorization header.
///
/// # Arguments
///
/// * `headers` - The HTTP headers from the request
///
/// # Returns
///
/// The Bearer token if present, None otherwise.
pub fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let auth_header = headers.get("Authorization")?;
    let auth_str = auth_header.to_str().ok()?;

    if !auth_str.starts_with("Bearer ") {
        return None;
    }

    Some(auth_str.strip_prefix("Bearer ")?.trim().to_string())
}

/// Check if the request is authenticated with a valid legacy (non-OAuth) session.
///
/// # Arguments
///
/// * `state` - The PDS state containing database access
/// * `headers` - The HTTP headers from the request
///
/// # Returns
///
/// An AuthResult indicating whether the user is authenticated.
pub fn check_legacy_auth(state: &Arc<PdsState>, headers: &HeaderMap) -> AuthResult {
    // Get IP from X-Forwarded-For header for logging
    let ip = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Extract the bearer token
    let access_jwt = match extract_bearer_token(headers) {
        Some(token) => token,
        None => {
            logger().info(&format!(
                "[AUTH] [LEGACY] ip={} authenticated=false error=no_token",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some("No authorization token".to_string()),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::Legacy)
            };
        }
    };

    // Get required config values
    let jwt_secret = match state.db.get_config_property("JwtSecret") {
        Ok(secret) => secret,
        Err(_) => {
            logger().info(&format!(
                "[AUTH] [LEGACY] ip={} authenticated=false error=config_error",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some("Server configuration error".to_string()),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::Legacy),
            };
        }
    };

    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            logger().info(&format!(
                "[AUTH] [LEGACY] ip={} authenticated=false error=config_error",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some("Server configuration error".to_string()),
                is_expired: false,
                space_uri: None,
                auth_type: Some(AuthType::Legacy),
            };
        }
    };

    // Validate the JWT with expiry checking
    let validation_result = validate_access_jwt(&access_jwt, &jwt_secret, &user_did, true);

    if !validation_result.is_valid {
        // Check if the token was valid but expired
        let expired_check = validate_access_jwt(&access_jwt, &jwt_secret, &user_did, false);
        if expired_check.is_valid {
            logger().info(&format!(
                "[AUTH] [LEGACY] ip={} authenticated=false expired=true",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: expired_check.sub,
                error: Some("Token expired".to_string()),
                is_expired: true,
                space_uri: None,
                auth_type: Some(AuthType::Legacy),
            };
        }

        logger().info(&format!(
            "[AUTH] [LEGACY] ip={} authenticated=false expired=false",
            ip
        ));
        return AuthResult {
            is_authenticated: false,
            user_did: None,
            error: validation_result.error,
            is_expired: false,
            space_uri: None,
            auth_type: Some(AuthType::Legacy)
        };
    }

    // Check that the session exists in the database
    let session_exists = state
        .db
        .legacy_session_exists_for_access_jwt(&access_jwt)
        .unwrap_or(false);

    if !session_exists {
        logger().info(&format!(
            "[AUTH] [LEGACY] ip={} authenticated=false expired=false existsInDb=false",
            ip
        ));
        return AuthResult {
            is_authenticated: false,
            user_did: validation_result.sub,
            error: Some("Session not found".to_string()),
            is_expired: false,
            space_uri: None,
            auth_type: Some(AuthType::Legacy)
        };
    }

    logger().info(&format!(
        "[AUTH] [LEGACY] ip={} authenticated=true expired=false existsInDb=true",
        ip
    ));

    // update last used
    let _ = state.db.update_legacy_session_last_used_date(&access_jwt);

    // return result
    AuthResult {
        is_authenticated: true,
        user_did: validation_result.sub,
        error: None,
        is_expired: false,
        space_uri: None,
        auth_type: Some(AuthType::Legacy),
    }
}

/// Create an authentication failure response.
///
/// # Arguments
///
/// * `auth_result` - The result of the authentication check
///
/// # Returns
///
/// An HTTP response with the appropriate error.
pub fn auth_failure_response(auth_result: &AuthResult) -> Response {
    if auth_result.is_expired {
        (
            StatusCode::BAD_REQUEST,
            Json(AuthError {
                error: "ExpiredToken".to_string(),
                message: "Please refresh the token.".to_string(),
            }),
        )
            .into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(AuthError {
                error: "Unauthorized".to_string(),
                message: auth_result
                    .error
                    .clone()
                    .unwrap_or_else(|| "User is not authorized.".to_string()),
            }),
        )
            .into_response()
    }
}


/// Result of OAuth token validation.
#[allow(dead_code)]
pub struct OauthValidationResult {
    /// Whether the token is valid.
    pub is_valid: bool,
    /// Whether the token was valid but expired.
    pub is_expired: bool,
    /// Error message if validation failed.
    pub error: Option<String>,
    /// The subject (user DID) from the token.
    pub subject: Option<String>,
    /// The scope from the token.
    pub scope: Option<String>,
    /// The client_id from the token.
    pub client_id: Option<String>,
    /// The JWK thumbprint (cnf.jkt) from the token.
    pub jwk_thumbprint: Option<String>,
}

impl Default for OauthValidationResult {
    fn default() -> Self {
        Self {
            is_valid: false,
            is_expired: false,
            error: None,
            subject: None,
            scope: None,
            client_id: None,
            jwk_thumbprint: None,
        }
    }
}

/// Extract the DPoP token from the Authorization header.
///
/// DPoP tokens use "DPoP <token>" format instead of "Bearer <token>".
fn extract_dpop_token(headers: &HeaderMap) -> Option<String> {
    let auth_header = headers.get("Authorization")?;
    let auth_str = auth_header.to_str().ok()?;

    if !auth_str.starts_with("DPoP ") {
        return None;
    }

    Some(auth_str.strip_prefix("DPoP ")?.trim().to_string())
}

/// Check if a JWT has the OAuth access token type (at+jwt).
fn is_oauth_access_token(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return false;
    }

    let header_bytes = match URL_SAFE_NO_PAD.decode(parts[0]) {
        Ok(b) => b,
        Err(_) => return false,
    };

    let header_str = match String::from_utf8(header_bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };

    let header: serde_json::Value = match serde_json::from_str(&header_str) {
        Ok(v) => v,
        Err(_) => return false,
    };

    header.get("typ").and_then(|v| v.as_str()) == Some("at+jwt")
}

/// Check if the request is using a DPoP-bound OAuth access token.
///
/// This checks for the presence of a DPoP header and a DPoP-scheme token with at+jwt type.
pub fn is_oauth_token_request(headers: &HeaderMap) -> bool {
    // Must have DPoP header
    if !headers.contains_key("DPoP") {
        return false;
    }

    // Must have a DPoP-scheme access token
    let access_token = match extract_dpop_token(headers) {
        Some(t) => t,
        None => return false,
    };

    // Check if the token has at+jwt type (OAuth access token)
    is_oauth_access_token(&access_token)
}

/// Check if the request is using a Service Auth token.
///
/// Service Auth tokens are distinguished from Legacy Auth tokens by:
/// - Using ES256 algorithm (asymmetric ECDSA) instead of HS256 (symmetric HMAC)
/// - Having an 'lxm' claim (lexicon method identifier)
/// - Having an 'iss' claim that is a DID (the remote service's DID)
///
/// This does NOT validate the token, only checks if it has the structure of a Service Auth token.
pub fn is_service_auth_request(headers: &HeaderMap) -> bool {
    // Must have a Bearer token (not DPoP)
    let token = match extract_bearer_token(headers) {
        Some(t) => t,
        None => return false,
    };

    // Parse the JWT header to check algorithm
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return false;
    }

    // Decode the header
    let header_bytes = match URL_SAFE_NO_PAD.decode(parts[0]) {
        Ok(b) => b,
        Err(_) => return false,
    };

    let header_str = match String::from_utf8(header_bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };

    let header: serde_json::Value = match serde_json::from_str(&header_str) {
        Ok(v) => v,
        Err(_) => return false,
    };

    // Check if algorithm is ES256 (Service Auth uses asymmetric ECDSA)
    // Legacy Auth uses HS256 (symmetric HMAC)
    let alg = header.get("alg").and_then(|v| v.as_str());
    if alg != Some("ES256") {
        return false;
    }

    // Decode the payload to check for service auth claims
    let payload_bytes = match URL_SAFE_NO_PAD.decode(parts[1]) {
        Ok(b) => b,
        Err(_) => return false,
    };

    let payload_str = match String::from_utf8(payload_bytes) {
        Ok(s) => s,
        Err(_) => return false,
    };

    let payload: serde_json::Value = match serde_json::from_str(&payload_str) {
        Ok(v) => v,
        Err(_) => return false,
    };

    // Check for 'lxm' claim which is specific to Service Auth tokens
    if payload.get("lxm").is_none() {
        return false;
    }

    // Check that 'iss' claim exists and looks like a DID
    let iss = payload.get("iss").and_then(|v| v.as_str());
    match iss {
        Some(issuer) if issuer.starts_with("did:") => true,
        _ => false,
    }
}

/// Check if a JWT declares the space-credential type in its header.
///
/// Space credentials are the only tokens whose JWT `typ` header is
/// `atproto-space-credential+jwt`, which is what distinguishes them from OAuth
/// (`at+jwt`), Service auth (ES256 + `lxm`) and Legacy (HS256) tokens.
fn is_space_credential_token(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return false;
    }

    let header_bytes = match URL_SAFE_NO_PAD.decode(parts[0]) {
        Ok(b) => b,
        Err(_) => return false,
    };

    let header: serde_json::Value = match serde_json::from_slice(&header_bytes) {
        Ok(v) => v,
        Err(_) => return false,
    };

    header.get("typ").and_then(|v| v.as_str()) == Some(SPACE_CREDENTIAL_TYP)
}

/// Check if the request presents a space credential.
///
/// Space credentials are DPoP-bound JWTs distinguished from every other auth
/// type by their JWT `typ` header of `atproto-space-credential+jwt`. They may be
/// presented with either the `DPoP` or the `Bearer` authorization scheme.
///
/// This does NOT validate the credential, only checks whether the presented
/// token has the shape of a space credential.
pub fn is_space_credential_request(headers: &HeaderMap) -> bool {
    let token = match extract_dpop_token(headers).or_else(|| extract_bearer_token(headers)) {
        Some(t) => t,
        None => return false,
    };

    is_space_credential_token(&token)
}

/// Result of service auth token validation.
#[allow(dead_code)]
pub struct ServiceAuthResult {
    /// Whether validation succeeded.
    pub is_valid: bool,
    /// Error message if validation failed.
    pub error: Option<String>,
    /// The issuer DID (remote service's DID).
    pub issuer: Option<String>,
    /// The audience DID (should be this PDS's DID).
    pub audience: Option<String>,
    /// The lexicon method claim.
    pub lxm: Option<String>,
}

impl Default for ServiceAuthResult {
    fn default() -> Self {
        Self {
            is_valid: false,
            error: None,
            issuer: None,
            audience: None,
            lxm: None,
        }
    }
}

/// Validate a service auth token from the Authorization header.
///
/// Service auth tokens are JWTs signed by a remote service's atproto signing key.
/// The token's iss claim is the remote service's DID, and aud should be this PDS's DID.
///
/// # Arguments
///
/// * `state` - The PDS state containing database access
/// * `headers` - The HTTP headers from the request
/// * `expected_lxm` - Optional: the expected lxm claim (NSID of the endpoint being called)
///
/// # Returns
///
/// A ServiceAuthResult with validation result and claims.
pub fn validate_service_auth_token(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    expected_lxm: Option<&str>,
) -> ServiceAuthResult {
    use crate::ws::{ActorQueryOptions, BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};
    
    let mut result = ServiceAuthResult::default();

    // Get the bearer token
    let token = match extract_bearer_token(headers) {
        Some(t) => t,
        None => {
            result.error = Some("Missing Authorization header".to_string());
            return result;
        }
    };

    // Parse the JWT parts
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        result.error = Some("Invalid JWT format".to_string());
        return result;
    }

    // Decode payload to extract claims
    let payload_bytes = match URL_SAFE_NO_PAD.decode(parts[1]) {
        Ok(b) => b,
        Err(_) => {
            result.error = Some("Failed to decode JWT payload".to_string());
            return result;
        }
    };

    let payload_str = match String::from_utf8(payload_bytes) {
        Ok(s) => s,
        Err(_) => {
            result.error = Some("Invalid UTF-8 in JWT payload".to_string());
            return result;
        }
    };

    let payload: serde_json::Value = match serde_json::from_str(&payload_str) {
        Ok(v) => v,
        Err(_) => {
            result.error = Some("Invalid JSON in JWT payload".to_string());
            return result;
        }
    };

    // Extract claims
    let issuer = payload.get("iss").and_then(|v| v.as_str()).map(String::from);
    let audience = payload.get("aud").and_then(|v| v.as_str()).map(String::from);
    let lxm = payload.get("lxm").and_then(|v| v.as_str()).map(String::from);

    result.issuer = issuer.clone();
    result.audience = audience.clone();
    result.lxm = lxm.clone();

    // Validate issuer exists
    let issuer = match issuer {
        Some(iss) => iss,
        None => {
            result.error = Some("Token missing iss claim".to_string());
            return result;
        }
    };

    // Validate audience exists and matches this PDS's DID
    let audience = match audience {
        Some(aud) => aud,
        None => {
            result.error = Some("Token missing aud claim".to_string());
            return result;
        }
    };

    let pds_did = match state.db.get_config_property("PdsDid") {
        Ok(did) => did,
        Err(_) => {
            result.error = Some("Failed to get PDS DID from config".to_string());
            return result;
        }
    };

    if audience != pds_did {
        result.error = Some(format!(
            "Token audience '{}' does not match PDS DID '{}'",
            audience, pds_did
        ));
        return result;
    }

    // Validate lxm if expected
    if let Some(expected) = expected_lxm {
        if let Some(ref actual_lxm) = lxm {
            if actual_lxm != expected {
                result.error = Some(format!(
                    "Token lxm '{}' does not match expected '{}'",
                    actual_lxm, expected
                ));
                return result;
            }
        }
    }

    // Check token expiry
    if let Some(exp) = payload.get("exp").and_then(|v| v.as_i64()) {
        let now = chrono::Utc::now().timestamp();
        if now > exp {
            result.error = Some("Token has expired".to_string());
            return result;
        }
    }

    // Resolve the issuer's DID document to get their public key
    // Use tokio's Handle to run async code in sync context
    let app_view_host_name = state.db.get_config_property("AppViewHostName")
        .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
    let actor_info = match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            let issuer_clone = issuer.clone();
            handle.block_on(async {
                let client = BlueskyClient::new(&app_view_host_name);
                let options = ActorQueryOptions {
                    resolve_did_doc: true,
                    ..Default::default()
                };
                client.resolve_actor_info(&issuer_clone, Some(options)).await.ok()
            })
        }
        Err(_) => {
            result.error = Some("Failed to get async runtime handle".to_string());
            return result;
        }
    };

    let did_doc = match actor_info.and_then(|info| info.did_doc) {
        Some(doc) => doc,
        None => {
            result.error = Some(format!(
                "Failed to resolve DID document for issuer '{}'",
                issuer
            ));
            return result;
        }
    };

    // Extract the #atproto verification method public key
    let public_key_multibase = match extract_atproto_public_key(&did_doc) {
        Some(key) => key,
        None => {
            result.error = Some(format!(
                "Failed to extract atproto public key from DID document for '{}'",
                issuer
            ));
            return result;
        }
    };

    // Validate the token signature using the public key
    match crate::pds::auth::verify_service_auth_token(&token, &public_key_multibase) {
        Ok(true) => {
            result.is_valid = true;
        }
        Ok(false) => {
            result.error = Some("Token signature validation failed".to_string());
        }
        Err(e) => {
            result.error = Some(format!("Token signature validation error: {}", e));
        }
    }

    result
}

/// Async version of validate_service_auth_token that properly awaits DID resolution.
///
/// This should be used in async contexts to avoid blocking the runtime.
pub async fn validate_service_auth_token_async(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    expected_lxm: Option<&str>,
) -> ServiceAuthResult {
    use crate::ws::{ActorQueryOptions, BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};

    let mut result = ServiceAuthResult::default();

    // Get the bearer token
    let token = match extract_bearer_token(headers) {
        Some(t) => t,
        None => {
            result.error = Some("Missing Authorization header".to_string());
            return result;
        }
    };

    // Parse the JWT parts
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        result.error = Some("Invalid JWT format".to_string());
        return result;
    }

    // Decode payload to extract claims
    let payload_bytes = match URL_SAFE_NO_PAD.decode(parts[1]) {
        Ok(b) => b,
        Err(_) => {
            result.error = Some("Failed to decode JWT payload".to_string());
            return result;
        }
    };

    let payload_str = match String::from_utf8(payload_bytes) {
        Ok(s) => s,
        Err(_) => {
            result.error = Some("Invalid UTF-8 in JWT payload".to_string());
            return result;
        }
    };

    let payload: serde_json::Value = match serde_json::from_str(&payload_str) {
        Ok(v) => v,
        Err(_) => {
            result.error = Some("Invalid JSON in JWT payload".to_string());
            return result;
        }
    };

    // Extract claims
    let issuer = payload.get("iss").and_then(|v| v.as_str()).map(String::from);
    let audience = payload.get("aud").and_then(|v| v.as_str()).map(String::from);
    let lxm = payload.get("lxm").and_then(|v| v.as_str()).map(String::from);

    result.issuer = issuer.clone();
    result.audience = audience.clone();
    result.lxm = lxm.clone();

    // Validate issuer exists
    let issuer = match issuer {
        Some(iss) => iss,
        None => {
            result.error = Some("Token missing iss claim".to_string());
            return result;
        }
    };

    // Validate audience exists and matches this PDS's DID
    let audience = match audience {
        Some(aud) => aud,
        None => {
            result.error = Some("Token missing aud claim".to_string());
            return result;
        }
    };

    let pds_did = match state.db.get_config_property("PdsDid") {
        Ok(did) => did,
        Err(_) => {
            result.error = Some("Failed to get PDS DID from config".to_string());
            return result;
        }
    };

    if audience != pds_did {
        result.error = Some(format!(
            "Token audience '{}' does not match PDS DID '{}'",
            audience, pds_did
        ));
        return result;
    }

    // Validate lxm if expected
    if let Some(expected) = expected_lxm {
        if let Some(ref actual_lxm) = lxm {
            if actual_lxm != expected {
                result.error = Some(format!(
                    "Token lxm '{}' does not match expected '{}'",
                    actual_lxm, expected
                ));
                return result;
            }
        }
    }

    // Check token expiry
    if let Some(exp) = payload.get("exp").and_then(|v| v.as_i64()) {
        let now = chrono::Utc::now().timestamp();
        if now > exp {
            result.error = Some("Token has expired".to_string());
            return result;
        }
    }

    // Resolve the issuer's DID document to get their public key (async)
    let app_view_host_name = state.db.get_config_property("AppViewHostName")
        .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
    let client = BlueskyClient::new(&app_view_host_name);
    let options = ActorQueryOptions {
        resolve_did_doc: true,
        ..Default::default()
    };
    let actor_info = client.resolve_actor_info(&issuer, Some(options)).await.ok();

    let did_doc = match actor_info.and_then(|info| info.did_doc) {
        Some(doc) => doc,
        None => {
            result.error = Some(format!(
                "Failed to resolve DID document for issuer '{}'",
                issuer
            ));
            return result;
        }
    };

    // Extract the #atproto verification method public key
    let public_key_multibase = match extract_atproto_public_key(&did_doc) {
        Some(key) => key,
        None => {
            result.error = Some(format!(
                "Failed to extract atproto public key from DID document for '{}'",
                issuer
            ));
            return result;
        }
    };

    // Validate the token signature using the public key
    match crate::pds::auth::verify_service_auth_token(&token, &public_key_multibase) {
        Ok(true) => {
            result.is_valid = true;
        }
        Ok(false) => {
            result.error = Some("Token signature validation failed".to_string());
        }
        Err(e) => {
            result.error = Some(format!("Token signature validation error: {}", e));
        }
    }

    result
}

/// Extract the atproto public key from a DID document.
///
/// Looks for a verificationMethod with id ending in "#atproto".
pub fn extract_atproto_public_key(did_doc: &str) -> Option<String> {
    let doc: serde_json::Value = serde_json::from_str(did_doc).ok()?;
    
    let methods = doc.get("verificationMethod")?.as_array()?;
    
    for method in methods {
        let id = method.get("id")?.as_str()?;
        if id.ends_with("#atproto") {
            return method.get("publicKeyMultibase").and_then(|v| v.as_str()).map(String::from);
        }
    }
    
    None
}

/// Check if the request is authenticated with a valid service auth token.
///
/// # Arguments
///
/// * `state` - The PDS state containing database access
/// * `headers` - The HTTP headers from the request
/// * `expected_lxm` - Optional: the expected lxm claim (NSID of the endpoint being called)
///
/// # Returns
///
/// An AuthResult indicating whether the user is authenticated.
pub fn check_service_auth(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    expected_lxm: Option<&str>,
) -> AuthResult {
    let ip = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let result = validate_service_auth_token(state, headers, expected_lxm);

    if !result.is_valid {
        logger().info(&format!(
            "[AUTH] [SERVICE] ip={} authenticated=false error={:?} aud={:?} lxm={:?}",
            ip, result.error, result.audience, result.lxm
        ));
        return AuthResult {
            is_authenticated: false,
            user_did: None,
            error: result.error,
            is_expired: false,
            space_uri: None,
            auth_type: Some(AuthType::Service),
        };
    }

    logger().info(&format!(
        "[AUTH] [SERVICE] ip={} authenticated=true iss={:?} aud={:?} lxm={:?}",
        ip, result.issuer, result.audience, result.lxm
    ));

    AuthResult {
        is_authenticated: true,
        user_did: None, // Service auth doesn't authenticate as a specific user
        error: None,
        is_expired: false,
        space_uri: None,
        auth_type: Some(AuthType::Service),
    }
}

/// Async version of check_service_auth that properly awaits DID resolution.
///
/// This should be used in async contexts to avoid blocking the runtime.
pub async fn check_service_auth_async(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    expected_lxm: Option<&str>,
) -> AuthResult {
    let ip = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let result = validate_service_auth_token_async(state, headers, expected_lxm).await;

    if !result.is_valid {
        logger().info(&format!(
            "[AUTH] [SERVICE] ip={} authenticated=false error={:?} aud={:?} lxm={:?}",
            ip, result.error, result.audience, result.lxm
        ));
        return AuthResult {
            is_authenticated: false,
            user_did: None,
            error: result.error,
            is_expired: false,
            space_uri: None,
            auth_type: Some(AuthType::Service),
        };
    }

    logger().info(&format!(
        "[AUTH] [SERVICE] ip={} authenticated=true iss={:?} aud={:?} lxm={:?}",
        ip, result.issuer, result.audience, result.lxm
    ));

    AuthResult {
        is_authenticated: true,
        user_did: None, // Service auth doesn't authenticate as a specific user
        error: None,
        is_expired: false,
        space_uri: None,
        auth_type: Some(AuthType::Service),
    }
}

/// Check if the request is authenticated with a valid space credential.
///
/// Space credentials are DPoP-bound JWTs (`atproto-space-credential+jwt`) minted
/// by this host (the space authority) via `com.atproto.space.getSpaceCredential`.
/// Verification is entirely local: it checks the credential's `typ`, issuer
/// (must be this account), expiry, ES256 signature (against this account's
/// signing key) and DPoP binding (the caller must prove possession of the key
/// named in `cnf.jkt` via a DPoP proof over this request).
///
/// This does NOT check which space the credential grants access to. The calling
/// endpoint must additionally confirm the credential's `sub` claim matches the
/// space being accessed.
///
/// # Arguments
///
/// * `state` - The PDS state containing database/config access
/// * `headers` - The HTTP headers from the request
/// * `http_method` - The HTTP method of the request (needed for DPoP validation)
/// * `request_path` - The path of the request (needed for DPoP validation)
///
/// # Returns
///
/// An AuthResult indicating whether the caller holds a valid space credential.
pub fn check_space_credential(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    http_method: &str,
    request_path: &str,
) -> AuthResult {
    let ip = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let fail = |error: &str| AuthResult {
        is_authenticated: false,
        user_did: None,
        error: Some(error.to_string()),
        is_expired: false,
        space_uri: None,
        auth_type: Some(AuthType::SpaceCredential),
    };

    // Extract the credential (presented via DPoP or Bearer scheme).
    let token = match extract_dpop_token(headers).or_else(|| extract_bearer_token(headers)) {
        Some(t) => t,
        None => {
            logger().info(&format!(
                "[AUTH] [SPACECRED] ip={} authenticated=false error=no_token",
                ip
            ));
            return fail("No authorization token");
        }
    };

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return fail("Space credential is not a valid JWT");
    }

    // Header: must declare the space-credential type.
    let header: serde_json::Value = match URL_SAFE_NO_PAD
        .decode(parts[0])
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
    {
        Some(h) => h,
        None => return fail("Invalid space credential header"),
    };
    if header.get("typ").and_then(|v| v.as_str()) != Some(SPACE_CREDENTIAL_TYP) {
        return fail("Unexpected space credential typ");
    }

    // Payload claims.
    let payload: serde_json::Value = match URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
    {
        Some(p) => p,
        None => return fail("Invalid space credential payload"),
    };

    // Issuer must be this host's account (the space authority).
    let authority_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => return fail("Server configuration error"),
    };
    let iss = payload.get("iss").and_then(|v| v.as_str()).unwrap_or_default();
    if iss != authority_did {
        logger().info(&format!(
            "[AUTH] [SPACECRED] ip={} authenticated=false error=wrong_issuer",
            ip
        ));
        return fail("Space credential was not issued by this authority");
    }

    // Expiry.
    let now = chrono::Utc::now().timestamp();
    match payload.get("exp").and_then(|v| v.as_i64()) {
        Some(exp) if exp > now => {}
        Some(_) => {
            logger().info(&format!(
                "[AUTH] [SPACECRED] ip={} authenticated=false expired=true",
                ip
            ));
            return AuthResult {
                is_authenticated: false,
                user_did: None,
                error: Some("Space credential has expired".to_string()),
                is_expired: true,
                space_uri: None,
                auth_type: Some(AuthType::SpaceCredential),
            };
        }
        None => return fail("Space credential missing exp claim"),
    }

    // Signature: verify against the authority's (this account's) signing key.
    let public_key = match state.db.get_config_property("UserPublicKeyMultibase") {
        Ok(key) => key,
        Err(_) => return fail("Signing key not configured"),
    };
    match verify_service_auth_token(&token, &public_key) {
        Ok(true) => {}
        Ok(false) => return fail("Space credential signature is invalid"),
        Err(e) => return fail(&format!("Failed to verify space credential: {}", e)),
    }

    // DPoP binding: the credential is bound to a key via cnf.jkt; the caller must
    // prove possession of that key with a DPoP proof over this request.
    let cnf_jkt = match payload
        .get("cnf")
        .and_then(|c| c.get("jkt"))
        .and_then(|v| v.as_str())
    {
        Some(j) => j,
        None => return fail("Space credential missing cnf.jkt"),
    };

    let dpop_header = match headers
        .get("DPoP")
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty())
    {
        Some(h) => h,
        None => return fail("Missing DPoP proof"),
    };

    let request_uri = format!("https://{}{}", get_hostname(state), request_path);
    let dpop_result = validate_dpop(Some(dpop_header), http_method, &request_uri, 300);
    let dpop_valid = dpop_result.is_valid;
    let dpop_jkt = dpop_result.jwk_thumbprint.clone();
    let dpop_err = dpop_result.error.clone();
    match (dpop_valid, dpop_jkt.as_deref()) {
        (true, Some(jkt)) if jkt == cnf_jkt => {}
        (true, Some(_)) => {
            return fail("DPoP proof key does not match the credential binding");
        }
        _ => {
            return fail(
                &dpop_err.unwrap_or_else(|| "DPoP proof validation failed".to_string()),
            );
        }
    }

    let sub = payload.get("sub").and_then(|v| v.as_str()).unwrap_or_default();
    logger().info(&format!(
        "[AUTH] [SPACECRED] ip={} authenticated=true sub={} iss={}",
        ip, sub, iss
    ));

    // A space credential authenticates access to a space, not a specific user.
    AuthResult {
        is_authenticated: true,
        user_did: None,
        error: None,
        is_expired: false,
        space_uri: Some(sub.to_string()),
        auth_type: Some(AuthType::SpaceCredential),
    }
}

/// Validate an OAuth access token from the request including DPoP proof validation.
///
/// # Arguments
///
/// * `state` - The PDS state containing database access
/// * `headers` - The HTTP headers from the request
/// * `http_method` - The HTTP method of the request (e.g., "GET", "POST")
/// * `request_path` - The path of the request (e.g., "/xrpc/com.atproto.repo.createRecord")
///
/// # Returns
///
/// An OauthValidationResult with validation result and token claims.
pub fn validate_oauth_access_token(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    http_method: &str,
    request_path: &str,
) -> OauthValidationResult {
    let mut result = OauthValidationResult::default();

    // Get the access token (using DPoP scheme for OAuth)
    let access_token = match extract_dpop_token(headers) {
        Some(t) => t,
        None => {
            result.error = Some("Missing access token".to_string());
            return result;
        }
    };

    // Get the DPoP header
    let dpop_header = match headers.get("DPoP").and_then(|v| v.to_str().ok()) {
        Some(h) => h,
        None => {
            result.error = Some("Missing DPoP header".to_string());
            return result;
        }
    };

    // Build the full request URI
    let hostname = get_hostname(state);
    let request_uri = format!("https://{}{}", hostname, request_path);

    // Validate the DPoP proof
    let dpop_result = validate_dpop(Some(dpop_header), http_method, &request_uri, 300);
    if !dpop_result.is_valid || dpop_result.jwk_thumbprint.is_none() {
        result.error = Some(format!(
            "DPoP validation failed: {}",
            dpop_result.error.unwrap_or_else(|| "Unknown error".to_string())
        ));
        return result;
    }

    let dpop_thumbprint = dpop_result.jwk_thumbprint.unwrap();

    // Validate the access token JWT
    let token_result = validate_oauth_access_token_internal(state, &access_token, true);
    if !token_result.is_valid {
        // Check if it's just expired
        let expired_check = validate_oauth_access_token_internal(state, &access_token, false);
        if expired_check.is_valid {
            result.is_expired = true;
            result.error = Some("Token expired".to_string());
            result.subject = expired_check.subject;
            result.scope = expired_check.scope;
            result.client_id = expired_check.client_id;
            result.jwk_thumbprint = expired_check.jwk_thumbprint;
            return result;
        }

        result.error = token_result.error;
        return result;
    }

    // Verify DPoP binding - the token's cnf.jkt must match the DPoP proof's JWK thumbprint
    if let Some(ref token_thumbprint) = token_result.jwk_thumbprint {
        if !token_thumbprint.eq_ignore_ascii_case(&dpop_thumbprint) {
            result.error = Some("DPoP proof key does not match token binding".to_string());
            return result;
        }
    } else {
        result.error = Some("Token missing DPoP binding (cnf.jkt)".to_string());
        return result;
    }

    // Verify the subject matches the PDS user
    let user_did = state.db.get_config_property("UserDid").unwrap_or_default();
    if token_result.subject.as_ref() != Some(&user_did) {
        result.error = Some("Token subject does not match PDS user".to_string());
        return result;
    }

    // Verify a valid session exists for this DPoP key
    let jwk_thumbprint = token_result.jwk_thumbprint.clone().unwrap();
    if !state
        .db
        .has_valid_oauth_session_by_dpop_thumbprint(&jwk_thumbprint)
        .unwrap_or(false)
    {
        result.error = Some("No valid OAuth session found for this token".to_string());
        return result;
    }

    // update db
    let _ = state.db.update_oauth_session_last_used_date(&jwk_thumbprint);

    result.is_valid = true;
    result.subject = token_result.subject;
    result.scope = token_result.scope;
    result.client_id = token_result.client_id;
    result.jwk_thumbprint = token_result.jwk_thumbprint;

    logger().info(&format!(
        "[AUTH] [OAUTH] xrpc: token used. at_fp={} method={} path={}",
        token_fp(&access_token),
        http_method,
        request_path
    ));

    result
}

/// Internal helper to validate an OAuth access token JWT.
fn validate_oauth_access_token_internal(
    state: &Arc<PdsState>,
    access_token: &str,
    validate_expiry: bool,
) -> OauthValidationResult {
    let mut result = OauthValidationResult::default();

    let jwt_secret = match state.db.get_config_property("JwtSecret") {
        Ok(s) => s,
        Err(_) => {
            result.error = Some("Server configuration error".to_string());
            return result;
        }
    };

    let hostname = get_hostname(state);
    let issuer = format!("https://{}", hostname);

    // Decode and validate the JWT
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};

    let key = DecodingKey::from_secret(jwt_secret.as_bytes());
    let mut validation = Validation::new(Algorithm::HS256);
    validation.set_issuer(&[&issuer]);
    validation.set_audience(&[&issuer]);
    
    if !validate_expiry {
        validation.validate_exp = false;
    }

    #[derive(serde::Deserialize)]
    struct OauthClaims {
        sub: Option<String>,
        scope: Option<String>,
        client_id: Option<String>,
        cnf: Option<CnfClaim>,
    }

    #[derive(serde::Deserialize)]
    struct CnfClaim {
        jkt: Option<String>,
    }

    match decode::<OauthClaims>(access_token, &key, &validation) {
        Ok(token_data) => {
            result.is_valid = true;
            result.subject = token_data.claims.sub;
            result.scope = token_data.claims.scope;
            result.client_id = token_data.claims.client_id;
            result.jwk_thumbprint = token_data.claims.cnf.and_then(|c| c.jkt);
        }
        Err(e) => {
            result.error = Some(format!("Token validation error: {}", e));
        }
    }

    result
}

/// Check if the request is authenticated with a valid OAuth session.
///
/// # Arguments
///
/// * `state` - The PDS state containing database access
/// * `headers` - The HTTP headers from the request
/// * `http_method` - The HTTP method of the request
/// * `request_path` - The path of the request
///
/// # Returns
///
/// An AuthResult indicating whether the user is authenticated.
pub fn check_oauth_auth(
    state: &Arc<PdsState>,
    headers: &HeaderMap,
    http_method: &str,
    request_path: &str,
) -> AuthResult {
    let ip = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        logger().info(&format!(
            "[AUTH] [OAUTH] ip={} authenticated=false error=oauth_disabled",
            ip
        ));
        return AuthResult {
            is_authenticated: false,
            user_did: None,
            error: Some("OAuth is not enabled".to_string()),
            is_expired: false,
            space_uri: None,
            auth_type: Some(AuthType::Oauth)
        };
    }

    let oauth_result = validate_oauth_access_token(state, headers, http_method, request_path);

    if oauth_result.is_expired {
        logger().info(&format!(
            "[AUTH] [OAUTH] ip={} authenticated=false expired=true",
            ip
        ));
        return AuthResult {
            is_authenticated: false,
            user_did: oauth_result.subject,
            error: Some("Token expired".to_string()),
            is_expired: true,
            space_uri: None,
            auth_type: Some(AuthType::Oauth),
        };
    }

    if !oauth_result.is_valid {
        logger().info(&format!(
            "[AUTH] [OAUTH] ip={} authenticated=false error={:?}",
            ip, oauth_result.error
        ));
        return AuthResult {
            is_authenticated: false,
            user_did: oauth_result.subject,
            error: oauth_result.error,
            is_expired: false,
            space_uri: None,
            auth_type: Some(AuthType::Oauth),
        };
    }

    logger().info(&format!(
        "[AUTH] [OAUTH] ip={} authenticated=true scope={:?}",
        ip, oauth_result.scope
    ));

    AuthResult {
        is_authenticated: true,
        user_did: oauth_result.subject,
        error: None,
        is_expired: false,
        space_uri: None,
        auth_type: Some(AuthType::Oauth),
    }
}


