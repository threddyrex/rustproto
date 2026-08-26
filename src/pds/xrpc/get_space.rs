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
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::pds::auth::{verify_service_auth_token, SPACE_CREDENTIAL_TYP};
use crate::pds::db::StatisticKey;
use crate::pds::oauth::{get_hostname, validate_dpop};
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info};

/// The fixed marker segment identifying a permissioned-space URI.
const SPACE_MARKER: &str = "space";

/// Maximum age of an accepted DPoP proof, in seconds.
const DPOP_MAX_AGE_SECS: i64 = 300;

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

/// Extract the authorization token, accepting either the `DPoP` scheme (used by
/// the credential flow) or `Bearer`.
fn extract_auth_token(headers: &HeaderMap) -> Option<String> {
    let auth_str = headers.get("Authorization")?.to_str().ok()?;
    for scheme in ["DPoP ", "Bearer "] {
        if let Some(rest) = auth_str.strip_prefix(scheme) {
            let token = rest.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }
    None
}

/// Decode a base64url JWT segment into JSON.
fn decode_jwt_segment(segment: &str) -> Option<Value> {
    let bytes = URL_SAFE_NO_PAD.decode(segment).ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Whether the presented token is a space credential (by its JWT `typ` header).
fn is_space_credential(token: &str) -> bool {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    decode_jwt_segment(parts[0])
        .and_then(|h| h.get("typ").and_then(|v| v.as_str()).map(str::to_string))
        .as_deref()
        == Some(SPACE_CREDENTIAL_TYP)
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

    // Validate and parse the required space parameter.
    let space_uri = match params.space {
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

    // Authenticate the caller. A space credential (member hosted elsewhere) is
    // detected by its JWT typ; otherwise fall back to OAuth/Legacy user auth
    // (the account that owns the space on this host).
    let token = extract_auth_token(&headers);
    if let Some(token) = token.as_deref().filter(|t| is_space_credential(t)) {
        if let Err(message) =
            verify_space_credential(&state, token, &headers, &canonical_uri, &user_did)
        {
            return error_response(StatusCode::BAD_REQUEST, "InvalidToken", &message);
        }
    } else {
        let auth_result = check_user_auth(
            &state,
            &headers,
            None,
            "GET",
            "/xrpc/com.atproto.simplespace.getSpace",
        );
        if !auth_result.is_authenticated {
            return auth_failure_response(&auth_result);
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

/// Verify a space credential presented for `space_uri` whose authority is
/// `authority_did` (this host's account). The credential must be signed by the
/// authority, name this space as its subject, be unexpired, and be bound (via
/// `cnf.jkt`) to the key that signed the accompanying DPoP proof.
///
/// Returns `Err(message)` on any failure.
fn verify_space_credential(
    state: &Arc<PdsState>,
    token: &str,
    headers: &HeaderMap,
    space_uri: &str,
    authority_did: &str,
) -> Result<(), String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("Space credential is not a valid JWT".to_string());
    }

    // Header: must declare the space-credential type.
    let header = decode_jwt_segment(parts[0]).ok_or("Invalid space credential header")?;
    if header.get("typ").and_then(|v| v.as_str()) != Some(SPACE_CREDENTIAL_TYP) {
        return Err("Unexpected space credential typ".to_string());
    }

    // Payload: validate the credential claims.
    let payload = decode_jwt_segment(parts[1]).ok_or("Invalid space credential payload")?;

    let sub = payload.get("sub").and_then(|v| v.as_str()).unwrap_or_default();
    if sub != space_uri {
        return Err("Space credential subject does not match the requested space".to_string());
    }

    let iss = payload.get("iss").and_then(|v| v.as_str()).unwrap_or_default();
    if iss != authority_did {
        return Err("Space credential was not issued by this authority".to_string());
    }

    // Expiry.
    let now = chrono::Utc::now().timestamp();
    match payload.get("exp").and_then(|v| v.as_i64()) {
        Some(exp) if exp > now => {}
        Some(_) => return Err("Space credential has expired".to_string()),
        None => return Err("Space credential missing exp claim".to_string()),
    }

    // Signature: verify against the authority's (this account's) signing key.
    let public_key = state
        .db
        .get_config_property("UserPublicKeyMultibase")
        .map_err(|_| "Signing key not configured".to_string())?;
    match verify_service_auth_token(token, &public_key) {
        Ok(true) => {}
        Ok(false) => return Err("Space credential signature is invalid".to_string()),
        Err(e) => return Err(format!("Failed to verify space credential: {}", e)),
    }

    // DPoP binding: the credential is bound to a key via cnf.jkt; the caller must
    // prove possession of that key with a DPoP proof over this request.
    let cnf_jkt = payload
        .get("cnf")
        .and_then(|c| c.get("jkt"))
        .and_then(|v| v.as_str())
        .ok_or("Space credential missing cnf.jkt")?;

    let dpop_header = headers
        .get("DPoP")
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty())
        .ok_or("Missing DPoP proof")?;
    let request_uri = format!(
        "https://{}/xrpc/com.atproto.simplespace.getSpace",
        get_hostname(state)
    );
    let dpop_result = validate_dpop(Some(dpop_header), "GET", &request_uri, DPOP_MAX_AGE_SECS);
    match (dpop_result.is_valid, dpop_result.jwk_thumbprint) {
        (true, Some(jkt)) if jkt == cnf_jkt => Ok(()),
        (true, Some(_)) => {
            Err("DPoP proof key does not match the credential binding".to_string())
        }
        _ => Err(dpop_result
            .error
            .unwrap_or_else(|| "DPoP proof validation failed".to_string())),
    }
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
    fn extracts_token_from_dpop_and_bearer_schemes() {
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "DPoP abc.def.ghi".parse().unwrap());
        assert_eq!(extract_auth_token(&headers).as_deref(), Some("abc.def.ghi"));

        let mut headers = HeaderMap::new();
        headers.insert("Authorization", "Bearer xyz.123.456".parse().unwrap());
        assert_eq!(extract_auth_token(&headers).as_deref(), Some("xyz.123.456"));

        let headers = HeaderMap::new();
        assert!(extract_auth_token(&headers).is_none());
    }
}
