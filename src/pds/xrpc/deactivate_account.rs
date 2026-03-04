//! com.atproto.server.deactivateAccount endpoint.
//!
//! Deactivates the authenticated user's account.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

use crate::pds::db::StatisticKey;
use crate::pds::firehose_event_generator::FirehoseEventGenerator;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_legacy_auth, get_caller_info};

/// POST /xrpc/com.atproto.server.deactivateAccount - Deactivate account endpoint.
///
/// Deactivates the authenticated user's account.
/// Note: This endpoint only accepts Legacy auth (not OAuth).
///
/// # Headers
///
/// * `Authorization: Bearer <access_jwt>` - Required (Legacy auth only)
///
/// # Returns
///
/// * `200 OK` on success (no body)
/// * `400 Bad Request` if token is expired
/// * `401 Unauthorized` if not authenticated
pub async fn deactivate_account(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.deactivateAccount".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication (Legacy auth only for deactivate)
    let auth_result = check_legacy_auth(&state, &headers);
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Deactivate the account
    if let Err(e) = state.db.set_config_property_bool("UserIsActive", false) {
        state.log.error(&format!(
            "[ACCOUNT] Failed to deactivate account: {}",
            e
        ));
    }

    // Generate firehose events
    let generator = FirehoseEventGenerator::new(&state.db);
    if let Err(e) = generator.generate_deactivation_events() {
        state.log.error(&format!(
            "[ACCOUNT] Account deactivated but firehose event failed: {}",
            e
        ));
    }

    state.log.info("[ACCOUNT] Account deactivated");

    StatusCode::OK.into_response()
}
