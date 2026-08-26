//! com.atproto.space.unregisterNotify endpoint.
//!
//! Removes a registration created by `com.atproto.space.registerNotify`, so that
//! a service is no longer recorded as wanting to be notified about activity in a
//! permissioned space managed by this authority.
//!
//! The space must be one this host is the authority for (anchored on its own
//! account). Authentication is OAuth: the account that owns the space on this
//! host.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info, AuthType};

/// The fixed marker segment identifying a permissioned-space URI.
const SPACE_MARKER: &str = "space";

/// Request body for unregisterNotify.
#[derive(Deserialize)]
pub struct UnregisterNotifyRequest {
    /// Identifier of the service to stop notifying (e.g. `did:web:bulletin.my#bulletin`).
    service: Option<String>,
    /// Reference to the space (`at://{authority}/space/{spaceType}/{skey}`).
    space: Option<String>,
}

/// Successful response for unregisterNotify.
#[derive(Serialize)]
pub struct UnregisterNotifyResponse {
    /// Canonical URI of the space the registration was removed for.
    space: String,
    /// The service that was unregistered.
    service: String,
}

/// Error response for unregisterNotify.
#[derive(Serialize)]
pub struct UnregisterNotifyError {
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

/// Validate a notify service identifier. Services are addressed by a DID with an
/// optional service fragment (e.g. `did:web:bulletin.my#bulletin`).
fn is_valid_service(service: &str) -> bool {
    !service.is_empty() && service.starts_with("did:")
}

fn error_response(status: StatusCode, error: &str, message: &str) -> Response {
    (
        status,
        Json(UnregisterNotifyError {
            error: error.to_string(),
            message: message.to_string(),
        }),
    )
        .into_response()
}

/// POST /xrpc/com.atproto.space.unregisterNotify - Remove a service's
/// notification registration for a permissioned space.
///
/// # Headers
///
/// * `Authorization` - Required (OAuth).
///
/// # Request Body
///
/// * `service` - Required. Identifier of the service to stop notifying.
/// * `space` - Required. Reference to the space.
///
/// # Returns
///
/// * `200 OK` with `{space, service}` on success (idempotent; succeeds even if
///   no registration existed)
/// * `400 Bad Request` for malformed input or a space this host is not the
///   authority for
/// * `401 Unauthorized` if authentication is missing
pub async fn unregister_notify(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<UnregisterNotifyRequest>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.space.unregisterNotify".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Validate and parse the required space parameter.
    let space_uri = match body.space {
        Some(space) if !space.is_empty() => space,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing required parameter: space",
            );
        }
    };
    let space_id = match parse_space_uri(&space_uri) {
        Some(space_id) => space_id,
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                &format!("Invalid space uri: {}", space_uri),
            );
        }
    };
    let canonical_uri = space_id.uri();

    // Validate the required service parameter.
    let service = match body.service {
        Some(service) if is_valid_service(&service) => service,
        Some(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Invalid service identifier (must be a DID)",
            );
        }
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing required parameter: service",
            );
        }
    };

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
    if space_id.authority != user_did {
        return error_response(
            StatusCode::BAD_REQUEST,
            "SpaceNotFound",
            "This service is not the authority for the requested space",
        );
    }

    // Authenticate the caller: OAuth (the account that owns the space on this
    // host).
    let auth_result = check_user_auth(
        &state,
        &headers,
        Some(&[AuthType::Oauth]),
        "POST",
        "/xrpc/com.atproto.space.unregisterNotify",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Remove the notify registration. This is idempotent: deleting a
    // non-existent registration is not an error.
    if let Err(e) = state
        .db
        .delete_space_notify_registration(&canonical_uri, &service)
    {
        state.log.error(&format!(
            "[SPACE] [NOTIFY] Failed to unregister notify: {}",
            e
        ));
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ServerError",
            "Failed to remove notify registration",
        );
    }

    state.log.info(&format!(
        "[SPACE] [NOTIFY] Unregistered notify for service {} on space {}",
        service, canonical_uri
    ));

    Json(UnregisterNotifyResponse {
        space: canonical_uri,
        service,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_space_uri() {
        let space =
            parse_space_uri("at://did:web:testuser.rustproto.com/space/my.bulletin.board/self")
                .expect("valid space uri");
        assert_eq!(space.authority, "did:web:testuser.rustproto.com");
        assert_eq!(space.space_type, "my.bulletin.board");
        assert_eq!(space.skey, "self");
        assert_eq!(
            space.uri(),
            "at://did:web:testuser.rustproto.com/space/my.bulletin.board/self"
        );
    }

    #[test]
    fn rejects_malformed_space_uri() {
        assert!(parse_space_uri("at://did:plc:abc/app.bsky.feed.post/3kabc").is_none());
        assert!(parse_space_uri("at://did:plc:abc/space/my.type").is_none());
        assert!(parse_space_uri("at://did:plc:abc/space/my.type/self/extra").is_none());
        assert!(parse_space_uri("did:plc:abc/space/my.type/self").is_none());
    }

    #[test]
    fn validates_service() {
        assert!(is_valid_service("did:web:bulletin.my#bulletin"));
        assert!(is_valid_service("did:plc:abc123"));
        assert!(!is_valid_service(""));
        assert!(!is_valid_service("bulletin.my#bulletin"));
    }
}
