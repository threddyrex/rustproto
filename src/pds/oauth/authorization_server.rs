//! OAuth Authorization Server metadata endpoint.
//!
//! GET /.well-known/oauth-authorization-server
//!
//! Returns metadata about this authorization server as specified in
//! RFC 8414 (OAuth Authorization Server Metadata).

use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::helpers::{get_hostname, is_oauth_enabled};

/// Authorization Server metadata response.
#[derive(Serialize)]
struct AuthorizationServerResponse {
    /// The authorization server's issuer identifier.
    issuer: String,
    /// Whether request parameter is supported.
    request_parameter_supported: bool,
    /// Whether request_uri parameter is supported.
    request_uri_parameter_supported: bool,
    /// Whether request_uri must be pre-registered.
    require_request_uri_registration: bool,
    /// Supported scopes.
    scopes_supported: Vec<String>,
    /// Supported subject types.
    subject_types_supported: Vec<String>,
    /// Supported response types.
    response_types_supported: Vec<String>,
    /// Supported response modes.
    response_modes_supported: Vec<String>,
    /// Supported grant types.
    grant_types_supported: Vec<String>,
    /// Supported code challenge methods.
    code_challenge_methods_supported: Vec<String>,
    /// Supported UI locales.
    ui_locales_supported: Vec<String>,
    /// Supported display values.
    display_values_supported: Vec<String>,
    /// Supported request object signing algorithms.
    request_object_signing_alg_values_supported: Vec<String>,
    /// Whether authorization response includes 'iss' parameter.
    authorization_response_iss_parameter_supported: bool,
    /// Supported request object encryption algorithms.
    request_object_encryption_alg_values_supported: Vec<String>,
    /// Supported request object encryption encoding values.
    request_object_encryption_enc_values_supported: Vec<String>,
    /// JWKS URI.
    jwks_uri: String,
    /// Authorization endpoint.
    authorization_endpoint: String,
    /// Token endpoint.
    token_endpoint: String,
    /// Supported token endpoint authentication methods.
    token_endpoint_auth_methods_supported: Vec<String>,
    /// Supported token endpoint authentication signing algorithms.
    token_endpoint_auth_signing_alg_values_supported: Vec<String>,
    /// Revocation endpoint.
    revocation_endpoint: String,
    /// Pushed authorization request endpoint.
    pushed_authorization_request_endpoint: String,
    /// Whether PAR is required.
    require_pushed_authorization_requests: bool,
    /// Supported DPoP signing algorithms.
    dpop_signing_alg_values_supported: Vec<String>,
    /// Protected resources.
    protected_resources: Vec<String>,
    /// Whether client_id metadata documents are supported.
    client_id_metadata_document_supported: bool,
    /// Supported prompt values.
    prompt_values_supported: Vec<String>,
}

/// GET /.well-known/oauth-authorization-server
///
/// Returns OAuth authorization server metadata for this PDS.
pub async fn oauth_authorization_server(State(state): State<Arc<PdsState>>) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: ".well-known/oauth-authorization-server".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    let hostname = get_hostname(&state);
    let base_url = format!("https://{}", hostname);

    let signing_algs = vec![
        "RS256".to_string(),
        "RS384".to_string(),
        "RS512".to_string(),
        "PS256".to_string(),
        "PS384".to_string(),
        "PS512".to_string(),
        "ES256".to_string(),
        "ES256K".to_string(),
        "ES384".to_string(),
        "ES512".to_string(),
    ];

    let response = AuthorizationServerResponse {
        issuer: base_url.clone(),
        request_parameter_supported: true,
        request_uri_parameter_supported: true,
        require_request_uri_registration: true,
        scopes_supported: vec![
            "atproto".to_string(),
            "transition:email".to_string(),
            "transition:generic".to_string(),
            "transition:chat.bsky".to_string(),
        ],
        subject_types_supported: vec!["public".to_string()],
        response_types_supported: vec!["code".to_string()],
        response_modes_supported: vec![
            "query".to_string(),
            "fragment".to_string(),
            "form_post".to_string(),
        ],
        grant_types_supported: vec![
            "authorization_code".to_string(),
            "refresh_token".to_string(),
        ],
        code_challenge_methods_supported: vec!["S256".to_string()],
        ui_locales_supported: vec!["en-US".to_string()],
        display_values_supported: vec![
            "page".to_string(),
            "popup".to_string(),
            "touch".to_string(),
        ],
        request_object_signing_alg_values_supported: {
            let mut algs = signing_algs.clone();
            algs.push("none".to_string());
            algs
        },
        authorization_response_iss_parameter_supported: true,
        request_object_encryption_alg_values_supported: vec![],
        request_object_encryption_enc_values_supported: vec![],
        jwks_uri: format!("{}/oauth/jwks", base_url),
        authorization_endpoint: format!("{}/oauth/authorize", base_url),
        token_endpoint: format!("{}/oauth/token", base_url),
        token_endpoint_auth_methods_supported: vec![
            "none".to_string(),
            "private_key_jwt".to_string(),
        ],
        token_endpoint_auth_signing_alg_values_supported: signing_algs.clone(),
        revocation_endpoint: format!("{}/oauth/revoke", base_url),
        pushed_authorization_request_endpoint: format!("{}/oauth/par", base_url),
        require_pushed_authorization_requests: true,
        dpop_signing_alg_values_supported: signing_algs,
        protected_resources: vec![base_url.clone()],
        client_id_metadata_document_supported: true,
        prompt_values_supported: vec![
            "none".to_string(),
            "login".to_string(),
            "consent".to_string(),
            "select_account".to_string(),
            "create".to_string(),
        ],
    };

    Json(response).into_response()
}
