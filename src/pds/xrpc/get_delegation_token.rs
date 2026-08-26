//! com.atproto.space.getDelegationToken endpoint.
//!
//! Mints a delegation token for an AT Protocol permissioned space, proving the
//! application is acting on the user's behalf when it asks the space authority
//! for a space credential. The token is single-use, short-lived, and addressed
//! to the space authority (`{authority}#atproto_space_host`). It is served by
//! the requesting user's PDS and signed with the account's signing key.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::auth::{sign_delegation_token, DELEGATION_TOKEN_TTL_SECS};
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info};

/// The fixed marker segment identifying a permissioned-space URI.
const SPACE_MARKER: &str = "space";

/// Query parameters for getDelegationToken.
#[derive(Deserialize)]
pub struct GetDelegationTokenParams {
    /// Permissioned space URI, in the form `at://{authority}/space/{spaceType}/{skey}`.
    space: Option<String>,
}

/// Successful response for getDelegationToken.
#[derive(Serialize)]
pub struct GetDelegationTokenResponse {
    /// The delegation token (JWT) signed by the account's signing key.
    token: String,
}

/// Error response for getDelegationToken.
#[derive(Serialize)]
pub struct GetDelegationTokenError {
    error: String,
    message: String,
}

/// A parsed permissioned-space identity.
struct SpaceId {
    authority: String,
    space_type: String,
    skey: String,
}

impl SpaceId {
    /// `at://{authority}/space/{spaceType}/{skey}`
    fn uri(&self) -> String {
        format!(
            "at://{}/{}/{}/{}",
            self.authority, SPACE_MARKER, self.space_type, self.skey
        )
    }
}

/// Parse a permissioned-space URI (`at://{authority}/space/{spaceType}/{skey}`).
///
/// This is not a standard at-uri: the literal `space` marker sits where a
/// collection NSID would appear.
fn parse_space_uri(uri: &str) -> Option<SpaceId> {
    let rest = uri.strip_prefix("at://")?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 4 || parts[1] != SPACE_MARKER {
        return None;
    }
    if parts[0].is_empty() || parts[2].is_empty() || parts[3].is_empty() {
        return None;
    }
    Some(SpaceId {
        authority: parts[0].to_string(),
        space_type: parts[2].to_string(),
        skey: parts[3].to_string(),
    })
}

/// GET /xrpc/com.atproto.space.getDelegationToken - Space delegation token endpoint.
///
/// Returns a signed JWT delegation token that a client presents to a space
/// authority in exchange for a space credential.
///
/// # Headers
///
/// * `Authorization` - Required (Legacy or OAuth).
///
/// # Query Parameters
///
/// * `space` - Required. Permissioned space URI (`at://{authority}/space/{spaceType}/{skey}`).
///
/// # Returns
///
/// * `200 OK` with the signed delegation token on success
/// * `400 Bad Request` if the space parameter is missing or malformed
/// * `401 Unauthorized` if not authenticated
pub async fn get_delegation_token(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<GetDelegationTokenParams>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.space.getDelegationToken".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Check authentication (supports Legacy and OAuth)
    let auth_result = check_user_auth(
        &state,
        &headers,
        None,
        "GET",
        "/xrpc/com.atproto.space.getDelegationToken",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Validate and parse required space parameter
    let space_uri = match params.space {
        Some(space) if !space.is_empty() => space,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetDelegationTokenError {
                    error: "InvalidRequest".to_string(),
                    message: "Missing required parameter: space".to_string(),
                }),
            )
                .into_response();
        }
    };

    let space_id = match parse_space_uri(&space_uri) {
        Some(space_id) => space_id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetDelegationTokenError {
                    error: "InvalidRequest".to_string(),
                    message: format!("Invalid space uri: {}", space_uri),
                }),
            )
                .into_response();
        }
    };

    // Get signing key from config
    let private_key = match state.db.get_config_property("UserPrivateKeyMultibase") {
        Ok(key) => key,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetDelegationTokenError {
                    error: "ServerError".to_string(),
                    message: "Signing key not configured".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Get user DID (issuer)
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetDelegationTokenError {
                    error: "ServerError".to_string(),
                    message: "User DID not configured".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Mint the delegation token, using the canonicalized space URI as the subject.
    let token = match sign_delegation_token(
        &private_key,
        &user_did,
        &space_id.uri(),
        &space_id.authority,
        DELEGATION_TOKEN_TTL_SECS,
    ) {
        Ok(token) => token,
        Err(e) => {
            state.log.error(&format!(
                "[SPACE] [DELEGATION] Failed to sign delegation token: {}",
                e
            ));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetDelegationTokenError {
                    error: "ServerError".to_string(),
                    message: "Failed to sign delegation token".to_string(),
                }),
            )
                .into_response();
        }
    };

    Json(GetDelegationTokenResponse { token }).into_response()
}
