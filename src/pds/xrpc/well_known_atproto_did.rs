//! AT Protocol DID endpoint.
//!
//! GET /.well-known/atproto-did
//!
//! Returns the user's DID as plain text. This endpoint does not require
//! authentication and sets CORS headers to allow any origin.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::pds::xrpc::auth_helpers::get_caller_info;

/// GET /.well-known/atproto-did - Returns the user's DID as plain text.
pub async fn well_known_atproto_did(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: ".well-known/atproto-did".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Load UserDid from config
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "UserDid not configured",
            ).into_response();
        }
    };

    (
        [
            (header::CONTENT_TYPE, "text/plain"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
        ],
        user_did,
    ).into_response()
}
