//! OAuth JWKS (JSON Web Key Set) endpoint.
//!
//! GET /oauth/jwks
//!
//! Returns the public keys that can be used to verify tokens issued by this server.
//! Currently returns an empty key set since we use symmetric signing.

use std::sync::Arc;

use axum::{Json, extract::State, http::{HeaderMap, StatusCode}, response::IntoResponse};
use serde::Serialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::helpers::{get_caller_info, is_oauth_enabled};

/// JWKS response.
#[derive(Serialize)]
struct JwksResponse {
    /// Array of JWK objects.
    keys: Vec<serde_json::Value>,
}

/// GET /oauth/jwks
///
/// Returns the JSON Web Key Set for this authorization server.
/// Currently returns an empty set since we use symmetric (HS256) signing.
pub async fn oauth_jwks(
    State(state): State<Arc<PdsState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Check if OAuth is enabled
    if !is_oauth_enabled(&state.db) {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({}))).into_response();
    }

    // Increment statistics
    let (ip_address, user_agent) = get_caller_info(&headers);
    let stat_key = StatisticKey {
        name: "oauth/jwks".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Return empty JWKS - we use symmetric signing
    let response = JwksResponse { keys: vec![] };

    Json(response).into_response()
}
