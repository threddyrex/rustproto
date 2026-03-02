//! com.atproto.server.getServiceAuth endpoint.
//!
//! Returns a signed token on behalf of the requesting DID for the requested service.
//! This is used for inter-service authentication in the AT Protocol.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::auth::sign_service_auth_token;
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_legacy_auth};

/// Query parameters for getServiceAuth.
#[derive(Deserialize)]
pub struct GetServiceAuthParams {
    /// DID of the service that will receive the token (required).
    aud: Option<String>,
    /// Lexicon method to bind the token to (optional).
    lxm: Option<String>,
    /// Expiry in Unix epoch seconds (optional, defaults to 60 seconds in future).
    exp: Option<String>,
}

/// Successful response for getServiceAuth.
#[derive(Serialize)]
pub struct GetServiceAuthResponse {
    /// The signed service auth token (JWT).
    token: String,
}

/// Error response for getServiceAuth.
#[derive(Serialize)]
pub struct GetServiceAuthError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.server.getServiceAuth - Service auth token endpoint.
///
/// Returns a signed JWT token that can be used to authenticate with another
/// AT Protocol service on behalf of the user.
///
/// # Headers
///
/// * `Authorization: Bearer <access_jwt>` - Required
///
/// # Query Parameters
///
/// * `aud` - Required. DID of the service that will receive the token.
/// * `lxm` - Optional. Lexicon method to bind the token to.
/// * `exp` - Optional. Expiry in Unix epoch seconds.
///
/// # Returns
///
/// * `200 OK` with signed token on success
/// * `400 Bad Request` if parameters are invalid
/// * `401 Unauthorized` if not authenticated
pub async fn get_service_auth(
    State(state): State<Arc<PdsState>>,
    headers: HeaderMap,
    Query(params): Query<GetServiceAuthParams>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.getServiceAuth".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication
    let auth_result = check_legacy_auth(&state, &headers);
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Validate required aud parameter
    let aud = match params.aud {
        Some(aud) if !aud.is_empty() => aud,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetServiceAuthError {
                    error: "InvalidRequest".to_string(),
                    message: "Missing required parameter: aud".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Parse optional exp parameter
    let expires_in_seconds = if let Some(exp_str) = params.exp {
        match exp_str.parse::<i64>() {
            Ok(exp_unix) => {
                let now = chrono::Utc::now().timestamp();
                let secs = exp_unix - now;

                // Clamp to reasonable bounds
                if secs < 1 {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(GetServiceAuthError {
                            error: "InvalidRequest".to_string(),
                            message: "exp must be in the future".to_string(),
                        }),
                    )
                        .into_response();
                }
                // Services may enforce max bounds (cap at 300 seconds)
                if secs > 300 {
                    300
                } else {
                    secs
                }
            }
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(GetServiceAuthError {
                        error: "InvalidRequest".to_string(),
                        message: "Invalid exp parameter".to_string(),
                    }),
                )
                    .into_response();
            }
        }
    } else {
        60 // Default to 60 seconds
    };

    // Get signing keys from config
    let private_key = match state.db.get_config_property("UserPrivateKeyMultibase") {
        Ok(key) => key,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetServiceAuthError {
                    error: "ServerError".to_string(),
                    message: "Signing key not configured".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Get user DID (issuer)
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetServiceAuthError {
                    error: "ServerError".to_string(),
                    message: "User DID not configured".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Sign the service auth token
    let token = match sign_service_auth_token(
        &private_key,
        &user_did,
        &aud,
        params.lxm.as_deref(),
        expires_in_seconds,
    ) {
        Ok(token) => token,
        Err(e) => {
            state.log.error(&format!(
                "[AUTH] [SERVICE] Failed to sign token: {}",
                e
            ));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetServiceAuthError {
                    error: "ServerError".to_string(),
                    message: "Failed to sign token".to_string(),
                }),
            )
                .into_response();
        }
    };

    Json(GetServiceAuthResponse { token }).into_response()
}
