//! com.atproto.server.activateAccount endpoint.
//!
//! Activates the authenticated user's account.

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

use crate::pds::db::StatisticKey;
use crate::pds::firehose_event_generator::FirehoseEventGenerator;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth};

/// POST /xrpc/com.atproto.server.activateAccount - Activate account endpoint.
///
/// Activates the authenticated user's account.
///
/// # Headers
///
/// * `Authorization: Bearer <access_jwt>` - Required
///
/// # Returns
///
/// * `200 OK` on success (no body)
/// * `400 Bad Request` if token is expired
/// * `401 Unauthorized` if not authenticated
pub async fn activate_account(
    State(state): State<Arc<PdsState>>,
    headers: HeaderMap,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.activateAccount".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication (supports Legacy and OAuth)
    let auth_result = check_user_auth(
        &state,
        &headers,
        None,
        "POST",
        "/xrpc/com.atproto.server.activateAccount",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Activate the account
    if let Err(e) = state.db.set_config_property_bool("UserIsActive", true) {
        state.log.error(&format!(
            "[ACCOUNT] Failed to activate account: {}",
            e
        ));
    }

    // Generate firehose events
    let generator = FirehoseEventGenerator::new(&state.db);
    if let Err(e) = generator.generate_activation_events(true) {
        state.log.error(&format!(
            "[ACCOUNT] Account activated but firehose event failed: {}",
            e
        ));
    }

    state.log.info("[ACCOUNT] Account activated");

    StatusCode::OK.into_response()
}
