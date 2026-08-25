//! `com.atproto.simplespace.getSpace` endpoint.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::pds::{
    auth::verify_service_auth_token,
    db::StatisticKey,
    oauth::{get_allowed_permission_sets, get_hostname, validate_dpop},
    server::PdsState,
};

use super::auth_helpers::{
    check_oauth_auth_with_scope, get_caller_info, scope_grants, scope_includes_permission_set,
};

const GET_SPACE_PATH: &str = "/xrpc/com.atproto.simplespace.getSpace";

#[derive(Deserialize)]
pub struct GetSpaceParams {
    space: String,
}

#[derive(Serialize)]
struct GetSpaceResponse {
    uri: String,
    policy: Value,
    #[serde(rename = "appAccess")]
    app_access: Value,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

fn error(status: StatusCode, name: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: name.to_string(),
            message: message.into(),
        }),
    )
        .into_response()
}

fn parse_space_uri(uri: &str) -> Option<(&str, &str, &str)> {
    let mut parts = uri.strip_prefix("at://")?.split('/');
    let authority = parts.next()?;
    let segment = parts.next()?;
    let space_type = parts.next()?;
    let skey = parts.next()?;
    if authority.is_empty()
        || segment != "space"
        || space_type.is_empty()
        || skey.is_empty()
        || parts.next().is_some()
    {
        return None;
    }
    Some((authority, space_type, skey))
}

fn extract_dpop_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("Authorization")?
        .to_str()
        .ok()?
        .strip_prefix("DPoP ")
        .map(str::trim)
}

fn credential_subject_and_jkt(token: &str) -> Option<(String, String, String, i64)> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).ok()?).ok()?;
    if header.get("typ")?.as_str()? != "atproto-space-credential+jwt" {
        return None;
    }
    let payload: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).ok()?).ok()?;
    Some((
        payload.get("iss")?.as_str()?.to_string(),
        payload.get("sub")?.as_str()?.to_string(),
        payload.get("cnf")?.get("jkt")?.as_str()?.to_string(),
        payload.get("exp")?.as_i64()?,
    ))
}

fn oauth_can_read_space(
    scope: &str,
    space_type: &str,
    allowed_permission_sets: &std::collections::HashSet<String>,
) -> bool {
    scope_grants(scope, "space", space_type, "read")
        || scope_grants(scope, "space", space_type, "read_self")
        || scope_includes_permission_set(scope, allowed_permission_sets)
}

/// GET /xrpc/com.atproto.simplespace.getSpace.
pub async fn get_space(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<GetSpaceParams>,
) -> Response {
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.simplespace.getSpace".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    let (authority, space_type, skey) = match parse_space_uri(&params.space) {
        Some(parts) => parts,
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Invalid space reference",
            );
        }
    };
    let configured_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "User DID not configured",
            );
        }
    };
    if authority != configured_did {
        return error(
            StatusCode::NOT_FOUND,
            "SpaceNotFound",
            "Space is not hosted here",
        );
    }

    let oauth = check_oauth_auth_with_scope(&state, &headers, "GET", GET_SPACE_PATH);
    let authorized = if oauth.is_authenticated {
        let allowed_permission_sets = get_allowed_permission_sets(&state.db);
        oauth_can_read_space(
            oauth.scope.as_deref().unwrap_or_default(),
            space_type,
            &allowed_permission_sets,
        )
    } else {
        let Some(token) = extract_dpop_token(&headers) else {
            return error(
                StatusCode::UNAUTHORIZED,
                "AuthRequired",
                "OAuth or space credential is required",
            );
        };
        let Some((issuer, subject, expected_jkt, exp)) = credential_subject_and_jkt(token) else {
            return error(
                StatusCode::UNAUTHORIZED,
                "AuthRequired",
                "Invalid space credential",
            );
        };
        if issuer != configured_did
            || subject != params.space
            || exp <= chrono::Utc::now().timestamp()
        {
            return error(
                StatusCode::UNAUTHORIZED,
                "AuthRequired",
                "Invalid space credential",
            );
        }
        let public_key = match state.db.get_config_property("UserPublicKeyMultibase") {
            Ok(key) => key,
            Err(_) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "ServerError",
                    "Signing key not configured",
                );
            }
        };
        if !verify_service_auth_token(token, &public_key).unwrap_or(false) {
            return error(
                StatusCode::UNAUTHORIZED,
                "AuthRequired",
                "Invalid space credential",
            );
        }
        let request_uri = format!("https://{}{}", get_hostname(&state), GET_SPACE_PATH);
        let proof = validate_dpop(
            headers.get("DPoP").and_then(|value| value.to_str().ok()),
            "GET",
            &request_uri,
            300,
        );
        proof.is_valid && proof.jwk_thumbprint.as_deref() == Some(expected_jkt.as_str())
    };
    if !authorized {
        return error(
            StatusCode::FORBIDDEN,
            "InvalidToken",
            "The credential is not authorized for this space",
        );
    }

    let space = match state.db.get_simple_space(authority, space_type, skey) {
        Ok(Some(space)) => space,
        Ok(None) => return error(StatusCode::NOT_FOUND, "SpaceNotFound", "Space not found"),
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] Failed to read space: {e}"));
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to read space",
            );
        }
    };
    let policy = match serde_json::from_str(&space.policy_json) {
        Ok(policy) => policy,
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Stored policy is invalid",
            );
        }
    };
    let app_access = match serde_json::from_str(&space.app_access_json) {
        Ok(app_access) => app_access,
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Stored appAccess is invalid",
            );
        }
    };

    Json(GetSpaceResponse {
        uri: params.space,
        policy,
        app_access,
    })
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_space_reference() {
        assert_eq!(
            parse_space_uri("at://did:web:example.com/space/my.bulletin.board/self"),
            Some(("did:web:example.com", "my.bulletin.board", "self"))
        );
    }

    #[test]
    fn rejects_record_and_incomplete_references() {
        assert!(parse_space_uri("at://did:web:example.com/app.bsky.feed.post/abc").is_none());
        assert!(parse_space_uri("at://did:web:example.com/space/my.bulletin.board").is_none());
        assert!(
            parse_space_uri("at://did:web:example.com/space/my.bulletin.board/self/extra").is_none()
        );
    }

    #[test]
    fn recognizes_read_and_read_self_grants() {
        let allowed = std::collections::HashSet::new();
        assert!(oauth_can_read_space(
            "atproto space:my.bulletin.board?action=read_self",
            "my.bulletin.board",
            &allowed
        ));
        assert!(!oauth_can_read_space(
            "atproto space:my.bulletin.board?action=create",
            "my.bulletin.board",
            &allowed
        ));
    }
}
