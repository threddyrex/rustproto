//! com.atproto.identity.resolveHandle endpoint.
//!
//! Resolves a handle (e.g., "alice.bsky.social") to a DID.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::pds::xrpc::auth_helpers::get_caller_info;
use crate::ws::{ActorQueryOptions, BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};

/// Query parameters for resolveHandle.
#[derive(Deserialize)]
pub struct ResolveHandleParams {
    /// The handle to resolve.
    handle: Option<String>,
}

/// Successful response for resolveHandle.
#[derive(Serialize)]
pub struct ResolveHandleResponse {
    /// The resolved DID.
    did: String,
}

/// Error response for resolveHandle.
#[derive(Serialize)]
pub struct ResolveHandleError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.identity.resolveHandle - Handle resolution endpoint.
///
/// Resolves an AT Protocol handle to its DID.
///
/// # Parameters
///
/// * `handle` - The handle to resolve (e.g., "alice.bsky.social")
///
/// # Returns
///
/// * `200 OK` with `{ "did": "did:plc:..." }` on success
/// * `400 Bad Request` if handle parameter is missing
/// * `404 Not Found` if handle cannot be resolved
pub async fn resolve_handle(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<ResolveHandleParams>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.identity.resolveHandle".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Validate handle parameter
    let handle = match params.handle {
        Some(h) if !h.is_empty() => h,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ResolveHandleError {
                    error: "InvalidRequest".to_string(),
                    message: "Error: Params must have the property \"handle\"".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Resolve the handle to a DID
    let app_view_host_name = state.db.get_config_property("AppViewHostName")
        .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
    let client = BlueskyClient::new(&app_view_host_name);
    let options = ActorQueryOptions::default().with_did_doc(false);

    let actor_info = match client.resolve_actor_info(&handle, Some(options)).await {
        Ok(info) => info,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ResolveHandleError {
                    error: "NotFound".to_string(),
                    message: "Error: Handle not found".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Check if DID was resolved
    match actor_info.did {
        Some(did) if did.starts_with("did:") => {
            (StatusCode::OK, Json(ResolveHandleResponse { did })).into_response()
        }
        _ => (
            StatusCode::NOT_FOUND,
            Json(ResolveHandleError {
                error: "NotFound".to_string(),
                message: "Error: Handle not found".to_string(),
            }),
        )
            .into_response(),
    }
}
