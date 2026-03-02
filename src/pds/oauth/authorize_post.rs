//! OAuth Authorization endpoint (POST).
//!
//! POST /oauth/authorize
//!
//! Handles the authorization form submission.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
    Json,
};
use uuid::Uuid;

use crate::pds::auth::verify_password;
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::ws::BlueskyClient;

use super::authorize_get::generate_auth_form;
use super::helpers::{
    get_allowed_redirect_uris, get_form_value, get_hostname, is_oauth_enabled, is_passkeys_enabled,
};

/// POST /oauth/authorize
///
/// Handles the OAuth authorization form submission.
pub async fn oauth_authorize_post(
    State(state): State<Arc<PdsState>>,
    body: Bytes,
) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "oauth/authorize POST".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Parse form data
    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(_) => {
            state.log.warning("[OAUTH] authorize POST: Invalid body encoding");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    if body_str.is_empty() {
        state.log.warning("[OAUTH] authorize POST: Empty form data");
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
    }

    // Extract form parameters
    let client_id = get_form_value(&body_str, "client_id");
    let request_uri = get_form_value(&body_str, "request_uri");
    let username = get_form_value(&body_str, "username");
    let password = get_form_value(&body_str, "password");

    // Validate required parameters
    let client_id = match client_id {
        Some(id) if !id.is_empty() => id,
        _ => {
            state.log.warning("[OAUTH] authorize POST: Missing client_id");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    let request_uri = match request_uri {
        Some(uri) if !uri.is_empty() => uri,
        _ => {
            state.log.warning("[OAUTH] authorize POST: Missing request_uri");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    let username = match username {
        Some(u) if !u.is_empty() => u,
        _ => {
            state.log.warning("[OAUTH] authorize POST: Missing username");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    let password = match password {
        Some(p) if !p.is_empty() => p,
        _ => {
            state.log.warning("[OAUTH] authorize POST: Missing password");
            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))).into_response();
        }
    };

    // Load OAuth request
    let mut oauth_request = match state.db.get_oauth_request(&request_uri) {
        Ok(req) => req,
        Err(e) => {
            state.log.warning(&format!(
                "[OAUTH] authorize POST: OAuth request not found or expired. request_uri={} error={}",
                request_uri, e
            ));
            return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({}))).into_response();
        }
    };

    // Resolve actor info
    let bluesky_client = BlueskyClient::new();
    let actor_exists = bluesky_client.resolve_actor_info(&username, None).await.is_ok();

    // Check password
    let stored_hashed_password = state
        .db
        .get_config_property("UserHashedPassword")
        .ok();
    let password_matches = verify_password(stored_hashed_password.as_deref(), &password);

    let auth_succeeded = actor_exists && password_matches;

    if !auth_succeeded {
        state.log.warning(&format!(
            "[OAUTH] authorize POST: Authentication failed. username={} actor_exists={} password_matches={}",
            username, actor_exists, password_matches
        ));

        // Return the form with error
        let scope = get_form_value(&oauth_request.body, "scope").unwrap_or_default();
        let html = generate_auth_form(
            &request_uri,
            &client_id,
            &scope,
            true,
            is_passkeys_enabled(&state.db),
        );
        return Html(html).into_response();
    }

    state.log.info(&format!(
        "[OAUTH] authorize POST: Authentication succeeded. username={}",
        username
    ));

    // Validate redirect_uri against allowlist
    let redirect_uri = get_form_value(&oauth_request.body, "redirect_uri").unwrap_or_default();
    let allowed_uris = get_allowed_redirect_uris(&state.db);

    if !allowed_uris.contains(&redirect_uri) {
        state.log.warning(&format!(
            "[OAUTH] [SECURITY] authorize POST: redirect_uri not in allowlist. redirect_uri={}",
            redirect_uri
        ));
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid_redirect_uri" })),
        )
            .into_response();
    }

    // Generate authorization code
    let authorization_code = format!("authcode-{}", Uuid::new_v4());
    oauth_request.authorization_code = Some(authorization_code.clone());
    oauth_request.auth_type = Some("Legacy".to_string());

    // Update OAuth request in database
    if let Err(e) = state.db.update_oauth_request(&oauth_request) {
        state.log.error(&format!(
            "[OAUTH] authorize POST: Failed to update request: {}",
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
    let hostname = get_hostname(&state);
    let issuer = format!("https://{}", hostname);

    let redirect_url = format!(
        "{}?code={}&state={}&iss={}",
        redirect_uri,
        urlencoding::encode(&authorization_code),
        urlencoding::encode(&state_param),
        urlencoding::encode(&issuer)
    );

    state.log.info(&format!(
        "[OAUTH] authorize POST: Redirecting to client. redirect_url={}",
        redirect_url
    ));

    Redirect::to(&redirect_url).into_response()
}
