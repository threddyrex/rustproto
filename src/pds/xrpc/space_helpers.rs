//! Shared helpers for the permissioned-space XRPC endpoints.
//!
//! These utilities are common to the `com.atproto.space.*` /
//! `com.atproto.simplespace.*` handlers, such as gating them behind the
//! `FeatureEnabled_Spaces` config property.

use std::sync::Arc;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::pds::server::PdsState;

/// Error body returned by space XRPC endpoints when the feature is unavailable.
#[derive(Serialize)]
struct SpaceFeatureError {
    error: String,
    message: String,
}

/// Check if the spaces feature is enabled in the PDS configuration.
///
/// Spaces are enabled if the `FeatureEnabled_Spaces` config property is set to
/// true. Defaults to `false` when the property is unset.
pub fn is_spaces_enabled(state: &Arc<PdsState>) -> bool {
    state
        .db
        .get_config_property_bool("FeatureEnabled_Spaces")
        .unwrap_or(false)
}

/// Standard error response returned by space XRPC endpoints when the spaces
/// feature is disabled.
pub fn spaces_disabled_response() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(SpaceFeatureError {
            error: "SpacesDisabled".to_string(),
            message: "The spaces feature is not enabled on this PDS.".to_string(),
        }),
    )
        .into_response()
}
