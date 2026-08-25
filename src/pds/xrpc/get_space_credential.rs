//! com.atproto.space.getSpaceCredential endpoint.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::pds::{
    auth::{sign_space_credential, verify_service_auth_token},
    db::StatisticKey,
    oauth::{get_hostname, validate_dpop},
    server::PdsState,
};

use super::auth_helpers::{extract_bearer_token, get_caller_info};

const CREDENTIAL_LIFETIME_SECONDS: i64 = 2 * 60 * 60;

#[derive(Deserialize)]
pub struct GetSpaceCredentialRequest {
    /// Space reference, encoded as `at://<did>/space/<type>/<key>`.
    space: String,
    #[serde(rename = "clientAttestation")]
    _client_attestation: Option<String>,
}

#[derive(Serialize)]
pub struct GetSpaceCredentialResponse {
    credential: String,
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

fn delegation_claims(token: &str) -> Option<(String, String, String, i64)> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    let header: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).ok()?).ok()?;
    if header.get("typ")?.as_str()? != "atproto-space-delegation+jwt" {
        return None;
    }
    let payload: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).ok()?).ok()?;
    Some((
        payload.get("iss")?.as_str()?.to_string(),
        payload.get("sub")?.as_str()?.to_string(),
        payload.get("aud")?.as_str()?.to_string(),
        payload.get("exp")?.as_i64()?,
    ))
}

/// POST /xrpc/com.atproto.space.getSpaceCredential.
///
/// Exchanges a valid delegation token for a two-hour, DPoP-bound credential.
/// Credentials are deliberately not persisted; recipients validate their
/// signature and expiration independently.
pub async fn get_space_credential(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<GetSpaceCredentialRequest>,
) -> Response {
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.space.getSpaceCredential".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    let uri_parts: Vec<&str> = match body.space.strip_prefix("at://") {
        Some(uri) => uri.split('/').collect(),
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Invalid space reference",
            );
        }
    };
    if uri_parts.len() != 4
        || uri_parts[0].is_empty()
        || uri_parts[1] != "space"
        || uri_parts[2].is_empty()
        || uri_parts[3].is_empty()
    {
        return error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "Invalid space reference",
        );
    }

    let authority = uri_parts[0];
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
            StatusCode::FORBIDDEN,
            "SpaceNotFound",
            "Space is not hosted here",
        );
    }

    let delegation = match extract_bearer_token(&headers)
        .and_then(|token| delegation_claims(&token).map(|claims| (token, claims)))
    {
        Some(value) => value,
        None => {
            return error(
                StatusCode::UNAUTHORIZED,
                "InvalidDelegationToken",
                "Invalid delegation token",
            );
        }
    };
    let (token, (issuer, delegated_space, audience, exp)) = delegation;
    if issuer != configured_did
        || delegated_space != body.space
        || audience != format!("{}#atproto_space_host", authority)
        || exp <= chrono::Utc::now().timestamp()
    {
        return error(
            StatusCode::UNAUTHORIZED,
            "InvalidDelegationToken",
            "Delegation token does not match this space",
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
    if !verify_service_auth_token(&token, &public_key).unwrap_or(false) {
        return error(
            StatusCode::UNAUTHORIZED,
            "InvalidDelegationToken",
            "Delegation token signature is invalid",
        );
    }

    let dpop = match headers.get("DPoP").and_then(|value| value.to_str().ok()) {
        Some(value) => value,
        None => {
            return error(
                StatusCode::UNAUTHORIZED,
                "InvalidDelegationToken",
                "Missing DPoP proof",
            );
        }
    };
    let request_uri = format!(
        "https://{}{}",
        get_hostname(&state),
        "/xrpc/com.atproto.space.getSpaceCredential"
    );
    let dpop_result = validate_dpop(Some(dpop), "POST", &request_uri, 300);
    let jkt = match (dpop_result.is_valid, dpop_result.jwk_thumbprint) {
        (true, Some(jkt)) => jkt,
        _ => {
            return error(
                StatusCode::UNAUTHORIZED,
                "InvalidDelegationToken",
                "Invalid DPoP proof",
            );
        }
    };

    let private_key = match state.db.get_config_property("UserPrivateKeyMultibase") {
        Ok(key) => key,
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Signing key not configured",
            );
        }
    };
    let credential = match sign_space_credential(
        &private_key,
        authority,
        &body.space,
        &jkt,
        CREDENTIAL_LIFETIME_SECONDS,
    ) {
        Ok(credential) => credential,
        Err(e) => {
            state
                .log
                .error(&format!("[AUTH] [SPACE] Failed to sign credential: {}", e));
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to sign credential",
            );
        }
    };

    let _ = issuer;
    Json(GetSpaceCredentialResponse { credential }).into_response()
}
