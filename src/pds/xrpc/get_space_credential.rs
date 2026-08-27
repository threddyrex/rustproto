//! com.atproto.space.getSpaceCredential endpoint.
//!
//! Exchanges a delegation token for a space credential. This is the *host* side
//! of the AT Protocol permissioned-spaces credential flow: an application that
//! already holds a delegation token minted by a user's PDS
//! (`com.atproto.space.getDelegationToken`) presents it here, to the space
//! authority, together with a DPoP proof signed by the key it wants the
//! credential bound to. On success the authority returns a short-lived space
//! credential signed with its signing key and bound to that key via `cnf.jkt`.
//!
//! This PDS is the authority only for spaces anchored on its own account's DID
//! (personal-data spaces such as bookmarks). Requests for spaces under any other
//! authority are answered with `SpaceNotFound`.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::pds::auth::{
    sign_space_credential, verify_service_auth_token, DELEGATION_TOKEN_TYP,
    SPACE_CREDENTIAL_TTL_SECS,
};
use crate::pds::db::{
    StatisticKey,
};
use crate::pds::oauth::{get_hostname, validate_dpop};
use crate::pds::server::PdsState;

use super::auth_helpers::get_caller_info;
use super::space_helpers::{is_spaces_enabled, spaces_disabled_response};

/// The fixed marker segment identifying a permissioned-space URI.
const SPACE_MARKER: &str = "space";

/// Maximum age of an accepted DPoP proof, in seconds.
const DPOP_MAX_AGE_SECS: i64 = 300;

/// Request body for getSpaceCredential.
#[derive(Deserialize)]
pub struct GetSpaceCredentialRequest {
    /// Permissioned space URI (`at://{authority}/space/{spaceType}/{skey}`).
    space: Option<String>,
    /// Optional client attestation JWT establishing the app's identity.
    ///
    /// Required only when a space gates on app identity. `simplespace` spaces
    /// default to open app access, so it is accepted but not required here.
    #[serde(rename = "clientAttestation")]
    #[allow(dead_code)]
    client_attestation: Option<String>,
}

/// Successful response for getSpaceCredential.
#[derive(Serialize)]
pub struct GetSpaceCredentialResponse {
    /// The signed JWT space credential, bound to the DPoP key via `cnf.jkt`.
    credential: String,
}

/// Error response for getSpaceCredential.
#[derive(Serialize)]
pub struct GetSpaceCredentialError {
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
fn decode_jwt_segment(segment: &str) -> Option<serde_json::Value> {
    let bytes = URL_SAFE_NO_PAD.decode(segment).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn error_response(status: StatusCode, error: &str, message: &str) -> Response {
    (
        status,
        Json(GetSpaceCredentialError {
            error: error.to_string(),
            message: message.to_string(),
        }),
    )
        .into_response()
}

/// POST /xrpc/com.atproto.space.getSpaceCredential - Exchange a delegation token
/// for a space credential.
///
/// # Headers
///
/// * `Authorization` - Required. The delegation token, under the `DPoP` scheme.
/// * `DPoP` - Required. A proof signed by the key to bind the credential to.
///
/// # Request Body
///
/// * `space` - Required. Permissioned space URI.
/// * `clientAttestation` - Optional client attestation JWT.
///
/// # Returns
///
/// * `200 OK` with the signed space credential on success
/// * `400 Bad Request` for malformed input, an invalid delegation token, or a
///   space this PDS is not the authority for
/// * `401 Unauthorized` if the delegation token or DPoP proof is missing
pub async fn get_space_credential(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<GetSpaceCredentialRequest>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.space.getSpaceCredential".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Ensure the spaces feature is enabled.
    if !is_spaces_enabled(&state) {
        return spaces_disabled_response();
    }

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

    // This PDS is only the authority for spaces anchored on its own account.
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

    // Extract the delegation token (the request's authorization token).
    let delegation_token = match extract_auth_token(&headers) {
        Some(token) => token,
        None => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "InvalidDelegationToken",
                "Missing delegation token in Authorization header",
            );
        }
    };

    // Validate the DPoP proof and derive the thumbprint to bind the credential to.
    let dpop_header = match headers.get("DPoP").and_then(|v| v.to_str().ok()) {
        Some(h) if !h.is_empty() => h.to_string(),
        _ => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                "InvalidRequest",
                "Missing DPoP proof",
            );
        }
    };
    let request_uri = format!(
        "https://{}/xrpc/com.atproto.space.getSpaceCredential",
        get_hostname(&state)
    );
    let dpop_result = validate_dpop(Some(&dpop_header), "POST", &request_uri, DPOP_MAX_AGE_SECS);
    let dpop_jkt = match (dpop_result.is_valid, dpop_result.jwk_thumbprint) {
        (true, Some(jkt)) => jkt,
        _ => {
            let message = dpop_result
                .error
                .unwrap_or_else(|| "DPoP proof validation failed".to_string());
            return error_response(StatusCode::BAD_REQUEST, "InvalidRequest", &message);
        }
    };

    // Verify the delegation token: structure, claims, and signature.
    if let Err(message) =
        verify_delegation_token(&state, &delegation_token, &space_id, &user_did)
    {
        return error_response(StatusCode::BAD_REQUEST, "InvalidDelegationToken", &message);
    }

    // The user-to-app delegation is proven. Authorization of the user and app
    // against the space's policy is deferred to a space-management implementation
    // (`simplespace`), which is not yet configured; personal-data spaces anchored
    // on the account's own DID are authorized by default.

    // Mint the space credential, signed by the authority's signing key and bound
    // to the application's DPoP key.
    let private_key = match state.db.get_config_property("UserPrivateKeyMultibase") {
        Ok(key) => key,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Signing key not configured",
            );
        }
    };
    let credential = match sign_space_credential(
        &private_key,
        &user_did,
        &space_id.uri(),
        &dpop_jkt,
        SPACE_CREDENTIAL_TTL_SECS,
    ) {
        Ok(credential) => credential,
        Err(e) => {
            state.log.error(&format!(
                "[SPACE] [CREDENTIAL] Failed to sign space credential: {}",
                e
            ));
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to sign space credential",
            );
        }
    };

    Json(GetSpaceCredentialResponse { credential }).into_response()
}

/// Verify a delegation token presented for `space_id` whose authority is
/// `authority_did` (this PDS's account). Returns `Err(message)` on any failure.
fn verify_delegation_token(
    state: &Arc<PdsState>,
    token: &str,
    space_id: &SpaceId,
    authority_did: &str,
) -> Result<(), String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("Delegation token is not a valid JWT".to_string());
    }

    // Header: must declare the delegation token type.
    let header = decode_jwt_segment(parts[0]).ok_or("Invalid delegation token header")?;
    if header.get("typ").and_then(|v| v.as_str()) != Some(DELEGATION_TOKEN_TYP) {
        return Err(format!(
            "Unexpected delegation token typ (expected '{}')",
            DELEGATION_TOKEN_TYP
        ));
    }

    // Payload: validate the delegation claims.
    let payload = decode_jwt_segment(parts[1]).ok_or("Invalid delegation token payload")?;

    let sub = payload.get("sub").and_then(|v| v.as_str()).unwrap_or_default();
    if sub != space_id.uri() {
        return Err("Delegation token subject does not match the requested space".to_string());
    }

    // The audience is the space host of this authority.
    let expected_aud = format!("{}#atproto_space_host", authority_did);
    let aud = payload.get("aud").and_then(|v| v.as_str()).unwrap_or_default();
    if aud != expected_aud {
        return Err("Delegation token audience does not match this space host".to_string());
    }

    // The issuer is the delegating user. For personal-data spaces the authority
    // is the account's own DID, and the token is signed by that account's key.
    let iss = payload.get("iss").and_then(|v| v.as_str()).unwrap_or_default();
    if iss != authority_did {
        return Err("Delegation token issuer is not served by this authority".to_string());
    }

    // Expiry.
    let now = chrono::Utc::now().timestamp();
    match payload.get("exp").and_then(|v| v.as_i64()) {
        Some(exp) if exp > now => {}
        Some(_) => return Err("Delegation token has expired".to_string()),
        None => return Err("Delegation token missing exp claim".to_string()),
    }

    // Signature: verify against the issuer's (this account's) signing key.
    let public_key = state
        .db
        .get_config_property("UserPublicKeyMultibase")
        .map_err(|_| "Signing key not configured".to_string())?;
    match verify_service_auth_token(token, &public_key) {
        Ok(true) => Ok(()),
        Ok(false) => Err("Delegation token signature is invalid".to_string()),
        Err(e) => Err(format!("Failed to verify delegation token: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_space_uri() {
        let space = parse_space_uri("at://did:web:testuser.rustproto.com/space/my.bulletin.board/self")
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
