//! OAuth Pushed Authorization Request (PAR) endpoint.
//!
//! POST /oauth/par
//!
//! Implements RFC 9126 (OAuth 2.0 Pushed Authorization Requests).
//! Accepts authorization request parameters and returns a request_uri.

use std::sync::Arc;

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::Serialize;
use uuid::Uuid;

use crate::pds::db::{OauthRequest, StatisticKey};
use crate::pds::server::PdsState;

use super::dpop::validate_dpop;
use super::helpers::{get_allowed_redirect_uris, get_caller_info, get_form_value, get_hostname, is_oauth_enabled};

/// PAR success response.
#[derive(Serialize)]
struct ParResponse {
    /// The request URI to use in the authorization request.
    request_uri: String,
    /// Expiration time in seconds.
    expires_in: i32,
}

/// PAR error response.
#[derive(Serialize)]
struct ParError {
    error: String,
}

/// POST /oauth/par
///
/// Accepts a pushed authorization request and returns a request_uri.
pub async fn oauth_par(
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
        name: "oauth/par".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Get DPoP header
    let dpop_header = headers
        .get("DPoP")
        .and_then(|v| v.to_str().ok());

    // Get body as string
    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => {
            state
                .log
                .warning("[OAUTH] PAR: Invalid body encoding");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    // Validate DPoP and body are present
    if dpop_header.is_none() || body_str.is_empty() {
        state.log.warning(&format!(
            "[OAUTH] PAR: dpop or body is empty. dpop={:?} body_len={}",
            dpop_header.is_some(),
            body_str.len()
        ));
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
    }

    // Validate DPoP
    let hostname = get_hostname(&state);
    let dpop_result = validate_dpop(
        dpop_header,
        "POST",
        &format!("https://{}/oauth/par", hostname),
        300,
    );

    if !dpop_result.is_valid {
        state.log.warning(&format!(
            "[AUTH] [OAUTH] PAR: DPoP validation failed. error={:?}",
            dpop_result.error
        ));
        if let Some(debug) = dpop_result.debug_info {
            state.log.warning(&format!("[AUTH] [OAUTH] PAR: debug: {}", debug));
        }
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
    }

    // Validate redirect_uri against allowlist
    let redirect_uri = get_form_value(&body_str, "redirect_uri");
    let allowed_uris = get_allowed_redirect_uris(&state.db);

    if let Some(ref uri) = redirect_uri {
        if !allowed_uris.contains(uri) {
            state.log.warning(&format!(
                "[OAUTH] [SECURITY] PAR: redirect_uri not in allowlist. redirect_uri={}",
                uri
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(ParError {
                    error: "invalid_redirect_uri".to_string(),
                }),
            )
                .into_response();
        }
    }

    // Create new OauthRequest
    let expires_seconds = 300;
    let request_uri = format!("urn:ietf:params:oauth:request_uri:{}", Uuid::new_v4());
    let expires_date = chrono::Utc::now() + chrono::Duration::seconds(expires_seconds);

    let oauth_request = OauthRequest {
        request_uri: request_uri.clone(),
        expires_date: expires_date.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        dpop: dpop_header.unwrap_or_default().to_string(),
        body: body_str,
        authorization_code: None,
        auth_type: None,
    };

    // Insert into database
    if let Err(e) = state.db.insert_oauth_request(&oauth_request) {
        state
            .log
            .error(&format!("[OAUTH] PAR: Failed to insert request: {}", e));
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({})),
        )
            .into_response();
    }

    state.log.info(&format!(
        "[AUTH] [OAUTH] PAR: success. request_uri={} expires_in={}",
        request_uri, expires_seconds
    ));

    (
        StatusCode::CREATED,
        Json(ParResponse {
            request_uri,
            expires_in: expires_seconds as i32,
        }),
    )
        .into_response()
}
