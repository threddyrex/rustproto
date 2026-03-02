//! OAuth Protected Resource metadata endpoint.
//!
//! GET /.well-known/oauth-protected-resource
//!
//! Returns metadata about this resource server as specified in
//! RFC 8707 (OAuth Protected Resource Metadata).

use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Serialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::helpers::{get_hostname, is_oauth_enabled};

/// Protected Resource metadata response.
#[derive(Serialize)]
struct ProtectedResourceResponse {
    /// The resource server identifier.
    resource: String,
    /// List of authorization servers that can issue tokens for this resource.
    authorization_servers: Vec<String>,
    /// Supported scopes.
    scopes_supported: Vec<String>,
    /// Supported bearer token methods.
    bearer_methods_supported: Vec<String>,
    /// Documentation URL.
    resource_documentation: String,
}

/// GET /.well-known/oauth-protected-resource
///
/// Returns OAuth protected resource metadata for this PDS.
pub async fn oauth_protected_resource(State(state): State<Arc<PdsState>>) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: ".well-known/oauth-protected-resource".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    let hostname = get_hostname(&state);
    let resource_url = format!("https://{}", hostname);

    let response = ProtectedResourceResponse {
        resource: resource_url.clone(),
        authorization_servers: vec![resource_url],
        scopes_supported: vec![],
        bearer_methods_supported: vec!["header".to_string()],
        resource_documentation: "https://atproto.com".to_string(),
    };

    Json(response).into_response()
}
