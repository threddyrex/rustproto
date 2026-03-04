//! Authentication helpers for XRPC endpoints.
//!
//! This module provides utilities for extracting and validating authentication
//! from HTTP requests in XRPC handlers.
//!
//! Supported authentication types:
//! - Legacy: Original AT Protocol auth using handle/password with Bearer tokens
//! - OAuth: DPoP-bound OAuth 2.0 tokens with at+jwt type
//! - Service: Service auth tokens (JWT signed by remote service's signing key)

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Serialize;

use crate::log::logger;
use crate::pds::auth::validate_access_jwt;
use crate::pds::oauth::{is_oauth_enabled, validate_dpop, get_hostname};
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
        };
    }

    logger().info(&format!(
        "[AUTH] [LEGACY] ip={} authenticated=true expired=false existsInDb=true",
        ip
    ));

    AuthResult {
        is_authenticated: true,
        user_did: validation_result.sub,
        error: None,
        is_expired: false,
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

/// Extract caller info (IP address and user agent) from request headers.
///
/// # Arguments
///
/// * `headers` - The HTTP headers from the request
/// * `socket_addr` - The socket address of the connection
///
/// # Returns
///
/// A tuple of (ip_address, user_agent).
pub fn get_caller_info(headers: &HeaderMap, socket_addr: Option<SocketAddr>) -> (String, String) {
    // Get User-Agent
    let user_agent = headers
        .get("User-Agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Get IP address from X-Forwarded-For, or fall back to socket address
    let ip_address = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            // X-Forwarded-For can contain multiple IPs, take the first one
            s.split(',').next().unwrap_or(s).trim().to_string()
        })
        .unwrap_or_else(|| {
            socket_addr
                .map(|addr| addr.ip().to_string())
                .unwrap_or_else(|| "unknown".to_string())
        });

    (ip_address, user_agent)
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

    result.is_valid = true;
    result.subject = token_result.subject;
    result.scope = token_result.scope;
    result.client_id = token_result.client_id;
    result.jwk_thumbprint = token_result.jwk_thumbprint;
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
    }
}

/// Check if the user is authenticated using any of the allowed auth types.
///
/// By default, allows both Legacy and OAuth authentication.
/// The function checks in order: OAuth (if DPoP token present), then Legacy.
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
            };
        }

        return check_oauth_auth(state, headers, http_method, request_path);
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
    }
}
