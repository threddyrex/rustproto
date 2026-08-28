//! com.atproto.simplespace.getSpace endpoint.
//!
//! Describes a permissioned space managed by the `simplespace` implementation,
//! including its configuration. Served by the space host (this authority).
//!
//! A space is not an implicit default: it must have been created through
//! `com.atproto.simplespace.createSpace`. If no record exists for the requested
//! space, `SpaceNotFound` is returned.
//!
//! Authentication accepts either:
//!
//! * OAuth / Legacy user auth, for the account that owns the space on this host, or
//! * a DPoP-bound space credential (`atproto-space-credential+jwt`), for a member
//!   hosted elsewhere. Either way, the caller must be authorized for the space.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::uri::SpaceUri;

use crate::pds::xrpc::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info, AuthType};
use crate::pds::spaces::{is_spaces_enabled, spaces_disabled_response};

/// Query parameters for getSpace.
#[derive(Deserialize)]
pub struct GetSpaceParams {
    /// Reference to the space (`at://{authority}/space/{spaceType}/{skey}`).
    space: Option<String>,
}

/// Successful response for getSpace.
#[derive(Serialize)]
pub struct GetSpaceResponse {
    /// URI of the space.
    uri: String,
    /// User-access policy union.
    policy: Value,
    /// App-access policy union.
    #[serde(rename = "appAccess")]
    app_access: Value,
}

/// Error response for getSpace.
#[derive(Serialize)]
pub struct GetSpaceError {
    error: String,
    message: String,
}


fn error_response(status: StatusCode, error: &str, message: &str) -> Response {
    (
        status,
        Json(GetSpaceError {
            error: error.to_string(),
            message: message.to_string(),
        }),
    )
        .into_response()
}

/// GET /xrpc/com.atproto.simplespace.getSpace - Describe a permissioned space.
///
/// # Query Parameters
///
/// * `space` - Required. Reference to the space.
///
/// # Returns
///
/// * `200 OK` with `{uri, policy, appAccess}` on success
/// * `400 Bad Request` for malformed input, an invalid credential, or a space
///   this host is not the authority for
/// * `401 Unauthorized` if authentication is missing
/// * `404`/`SpaceNotFound` if no such space exists
pub async fn get_space(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<GetSpaceParams>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.simplespace.getSpace".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Ensure the spaces feature is enabled.
    if !is_spaces_enabled(&state) {
        return spaces_disabled_response();
    }

    // Validate and parse the required space parameter.
    let space_param = match params.space {
        Some(space) if !space.is_empty() => space,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing required parameter: space",
            );
        }
    };
    let space_uri = match SpaceUri::from_string(&space_param) {
        Some(space_uri) => space_uri,
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                &format!("Invalid space uri: {}", space_param),
            );
        }
    };
    let canonical_uri = space_uri.to_string();

    // This host is only the authority for spaces anchored on its own account.
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "User DID not configured",
            );
        }
    };
    if space_uri.authority != user_did {
        return error_response(
            StatusCode::BAD_REQUEST,
            "SpaceNotFound",
            "This service is not the authority for the requested space",
        );
    }

    // Authenticate the caller: OAuth (the account that owns the space on this
    // host) or a DPoP-bound space credential (a member hosted elsewhere).
    let auth_result = check_user_auth(
        &state,
        &headers,
        Some(&[AuthType::Oauth, AuthType::SpaceCredential]),
        "GET",
        "/xrpc/com.atproto.simplespace.getSpace",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }
    // A space credential is scoped to a single space (its `sub`); ensure it
    // grants access to the space actually being requested.
    if let Some(cred_space) = auth_result.space_uri.as_deref() {
        if cred_space != canonical_uri {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidToken",
                "Space credential does not grant access to the requested space",
            );
        }
    }

    // Look up the persisted space. Spaces are not implicit defaults.
    let space = match state.db.get_space(&canonical_uri) {
        Ok(space) => space,
        Err(crate::pds::db::PdsDbError::SpaceNotFound(_)) => {
            return error_response(
                StatusCode::NOT_FOUND,
                "SpaceNotFound",
                "No such space exists",
            );
        }
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] [GET] Failed to load space: {}", e));
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to load space",
            );
        }
    };

    // Deserialize the stored config unions.
    let policy: Value = match serde_json::from_str(&space.policy_json) {
        Ok(value) => value,
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] [GET] Corrupt policy json: {}", e));
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Stored space policy is corrupt",
            );
        }
    };
    let app_access: Value = match serde_json::from_str(&space.app_access_json) {
        Ok(value) => value,
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] [GET] Corrupt appAccess json: {}", e));
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Stored space appAccess is corrupt",
            );
        }
    };

    Json(GetSpaceResponse {
        uri: space.uri,
        policy,
        app_access,
    })
    .into_response()
}
