//! Authentication helpers for XRPC endpoints.
//!
//! This module provides utilities for extracting and validating authentication
//! from HTTP requests in XRPC handlers.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::pds::auth::validate_access_jwt;
use crate::pds::server::PdsState;

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
    // Extract the bearer token
    let access_jwt = match extract_bearer_token(headers) {
        Some(token) => token,
        None => {
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
            return AuthResult {
                is_authenticated: false,
                user_did: expired_check.sub,
                error: Some("Token expired".to_string()),
                is_expired: true,
            };
        }

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
        return AuthResult {
            is_authenticated: false,
            user_did: validation_result.sub,
            error: Some("Session not found".to_string()),
            is_expired: false,
        };
    }

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
