//! com.atproto.space.getDelegationToken endpoint.
//!
//! Mints a short-lived, single-use delegation token that proves a user has
//! delegated a client application to act on their behalf when it asks a
//! space authority for a space credential. The token says nothing about
//! whether the user is a member of the space - that is up to the authority
//! to determine.
//!
//! Delegation tokens are stateless: unlike sessions, they are never
//! persisted. They are signed with the account's ES256 signing key and are
//! verified by the recipient (the space authority) purely from the
//! signature plus the short `exp` (default 60 seconds); the issuing PDS has
//! no need to track or revoke them.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::auth::sign_space_delegation_token;
use crate::pds::db::StatisticKey;
use crate::pds::oauth::get_allowed_permission_sets;
use crate::pds::server::PdsState;

use super::auth_helpers::{
    check_oauth_auth_with_scope, get_caller_info, scope_grants, scope_includes_permission_set,
};

/// Query parameters for getDelegationToken.
#[derive(Deserialize)]
pub struct GetDelegationTokenParams {
    /// AT URI of the target space (required).
    space: Option<String>,
}

/// Successful response for getDelegationToken.
#[derive(Serialize)]
pub struct GetDelegationTokenResponse {
    /// The signed delegation token (JWT).
    token: String,
}

/// Error response for getDelegationToken.
#[derive(Serialize)]
pub struct GetDelegationTokenError {
    error: String,
    message: String,
}

/// The identifying path segment used for space AT URIs: `at://<authority>/space/<space-type>/<space-key>`.
const SPACE_URI_SEGMENT: &str = "space";

/// Fixed lifetime of a delegation token, per the delegation token spec (single-use, short-lived).
const DELEGATION_TOKEN_LIFETIME_SECONDS: i64 = 60;

/// A parsed `at://` space URI: `at://<authority>/space/<space-type>/<space-key>`.
struct SpaceUri<'a> {
    authority: &'a str,
    space_type: &'a str,
}

/// Parse a space AT URI of the form `at://<authority>/space/<space-type>/<space-key>`.
///
/// This differs from a normal record AT URI (`at://<authority>/<collection>/<rkey>`)
/// in that it has an extra fixed `space` path segment ahead of the space type.
fn parse_space_uri(uri: &str) -> Option<SpaceUri<'_>> {
    let rest = uri.strip_prefix("at://")?;
    let mut parts = rest.splitn(4, '/');
    let authority = parts.next()?;
    let segment = parts.next()?;
    let space_type = parts.next()?;
    let space_key = parts.next()?;

    if authority.is_empty()
        || segment != SPACE_URI_SEGMENT
        || space_type.is_empty()
        || space_key.is_empty()
    {
        return None;
    }

    Some(SpaceUri {
        authority,
        space_type,
    })
}

/// Check whether a granted OAuth scope string covers a read grant for the given space type.
///
/// Checks two forms of grant, either of which is sufficient:
/// 1. A literal resource-scope token via the shared [`scope_grants`] helper
///    (`space:<type>?action=read` or `space:*?action=read`), matching the
///    resource-scope grammar used by other `com.atproto.space.*` endpoints
///    and any future `repo:`/`rpc:` checks.
/// 2. An `include:<nsid>` permission-set reference, where `<nsid>` is on the
///    operator-controlled allowlist in `OauthAllowedPermissionSets`
///    (see [`scope_includes_permission_set`]) - this is how real-world OAuth
///    clients request grouped permissions in practice.
fn scope_grants_space_read(
    scope: &str,
    space_type: &str,
    allowed_permission_sets: &std::collections::HashSet<String>,
) -> bool {
    scope_grants(scope, "space", space_type, "read")
        || scope_includes_permission_set(scope, allowed_permission_sets)
}

/// GET /xrpc/com.atproto.space.getDelegationToken - Space delegation token endpoint.
///
/// Mints a delegation token asserting that the authenticated user has
/// delegated the calling application (identified by its OAuth `space:` scope
/// grant) to request a space credential for the given space.
///
/// # Headers
///
/// * `Authorization: DPoP <token>` + `DPoP: <proof>` - Required. Must be an OAuth session.
///
/// # Query Parameters
///
/// * `space` - Required. AT URI of the target space (`at://<authority>/space/<type>/<key>`).
///
/// # Returns
///
/// * `200 OK` with signed token on success
/// * `400 Bad Request` if parameters are invalid
/// * `401 Unauthorized` if not authenticated with a valid OAuth session
/// * `403 Forbidden` if the session's scope does not cover this space
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

    // Validate required space parameter
    let space_uri_str = match params.space {
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

    let space_uri = match parse_space_uri(&space_uri_str) {
        Some(uri) => uri,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetDelegationTokenError {
                    error: "InvalidRequest".to_string(),
                    message: "space must be an AT URI of the form at://<authority>/space/<type>/<key>"
                        .to_string(),
                }),
            )
                .into_response();
        }
    };

    // This endpoint is only reachable via a client app's OAuth session - the delegation
    // asserts app-to-user delegation, and only OAuth sessions carry the granular
    // `space:` scope grants required to authorize a specific space.
    let oauth_result = check_oauth_auth_with_scope(
        &state,
        &headers,
        "GET",
        "/xrpc/com.atproto.space.getDelegationToken",
    );

    if oauth_result.is_expired {
        return (
            StatusCode::BAD_REQUEST,
            Json(GetDelegationTokenError {
                error: "ExpiredToken".to_string(),
                message: "Please refresh the token.".to_string(),
            }),
        )
            .into_response();
    }

    if !oauth_result.is_authenticated {
        return (
            StatusCode::UNAUTHORIZED,
            Json(GetDelegationTokenError {
                error: "AuthRequired".to_string(),
                message: oauth_result
                    .error
                    .unwrap_or_else(|| "User is not authorized.".to_string()),
            }),
        )
            .into_response();
    }

    // Confirm the session's granted scope covers a read grant for this space type,
    // either via a literal resource-scope token or an allowlisted permission set.
    let scope = oauth_result.scope.unwrap_or_default();
    let allowed_permission_sets = get_allowed_permission_sets(&state.db);
    if !scope_grants_space_read(&scope, space_uri.space_type, &allowed_permission_sets) {
        return (
            StatusCode::FORBIDDEN,
            Json(GetDelegationTokenError {
                error: "InvalidToken".to_string(),
                message: format!(
                    "Session is missing a covering 'space:{}?action=read' scope grant (or an allowlisted 'include:' permission set)",
                    space_uri.space_type
                ),
            }),
        )
            .into_response();
    }

    let user_did = match oauth_result.user_did {
        Some(did) => did,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetDelegationTokenError {
                    error: "ServerError".to_string(),
                    message: "Token missing subject".to_string(),
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

    // The space host is identified by the space authority's own DID plus its
    // `#atproto_space_host` service fragment - no separate resolution step is needed.
    let audience = format!("{}#atproto_space_host", space_uri.authority);

    let token = match sign_space_delegation_token(
        &private_key,
        &user_did,
        &space_uri_str,
        &audience,
        DELEGATION_TOKEN_LIFETIME_SECONDS,
    ) {
        Ok(token) => token,
        Err(e) => {
            state
                .log
                .error(&format!("[AUTH] [SPACE] Failed to sign delegation token: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetDelegationTokenError {
                    error: "ServerError".to_string(),
                    message: "Failed to sign token".to_string(),
                }),
            )
                .into_response();
        }
    };

    Json(GetDelegationTokenResponse { token }).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_space_uri_valid() {
        let uri = "at://did:web:testuser.rustproto.com/space/my.bulletin.board/self";
        let parsed = parse_space_uri(uri).expect("should parse");
        assert_eq!(parsed.authority, "did:web:testuser.rustproto.com");
        assert_eq!(parsed.space_type, "my.bulletin.board");
    }

    #[test]
    fn test_parse_space_uri_missing_space_segment() {
        // Regular record AT URIs (no "space" segment) must not parse as space URIs.
        let uri = "at://did:plc:abc123/app.bsky.feed.post/xyz789";
        assert!(parse_space_uri(uri).is_none());
    }

    #[test]
    fn test_parse_space_uri_missing_key() {
        let uri = "at://did:web:testuser.rustproto.com/space/my.bulletin.board";
        assert!(parse_space_uri(uri).is_none());
    }

    #[test]
    fn test_scope_grants_space_read_exact_match() {
        let scope = "atproto transition:generic space:my.bulletin.board?action=read";
        let allowed = std::collections::HashSet::new();
        assert!(scope_grants_space_read(scope, "my.bulletin.board", &allowed));
        assert!(!scope_grants_space_read(scope, "other.type", &allowed));
    }

    #[test]
    fn test_scope_grants_space_read_wildcard() {
        let scope = "atproto space:*?action=read";
        let allowed = std::collections::HashSet::new();
        assert!(scope_grants_space_read(scope, "my.bulletin.board", &allowed));
    }

    #[test]
    fn test_scope_grants_space_read_wrong_action() {
        let scope = "atproto space:my.bulletin.board?action=write";
        let allowed = std::collections::HashSet::new();
        assert!(!scope_grants_space_read(scope, "my.bulletin.board", &allowed));
    }

    #[test]
    fn test_scope_grants_space_read_no_grant() {
        let scope = "atproto transition:generic";
        let allowed = std::collections::HashSet::new();
        assert!(!scope_grants_space_read(scope, "my.bulletin.board", &allowed));
    }

    #[test]
    fn test_scope_grants_space_read_allowlisted_permission_set() {
        // Mirrors a real-world grant: "atproto blob?... include:my.bulletin.permissions"
        let scope = "atproto blob?accept=image/jpeg include:my.bulletin.permissions";
        let mut allowed = std::collections::HashSet::new();
        allowed.insert("my.bulletin.permissions".to_string());
        assert!(scope_grants_space_read(scope, "my.bulletin.board", &allowed));
    }

    #[test]
    fn test_scope_grants_space_read_permission_set_not_allowlisted() {
        let scope = "atproto include:my.bulletin.permissions";
        let allowed = std::collections::HashSet::new();
        assert!(!scope_grants_space_read(scope, "my.bulletin.board", &allowed));
    }
}
