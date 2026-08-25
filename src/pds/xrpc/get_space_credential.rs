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
use serde_json::Value;

use crate::pds::{
    auth::{sign_service_auth_token, sign_space_credential, verify_service_auth_token},
    db::StatisticKey,
    oauth::{get_hostname, validate_dpop},
    server::PdsState,
    xrpc::app_bsky_proxy::is_valid_outbound_url,
};
use crate::ws::{BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};

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

async fn check_managing_app_access(
    state: &Arc<PdsState>,
    managing_app: &str,
    space: &str,
    user: &str,
) -> Result<bool, String> {
    let (app_did, service_fragment) = managing_app
        .split_once('#')
        .ok_or_else(|| "managingApp must include a service fragment".to_string())?;
    if app_did.is_empty() || service_fragment.is_empty() {
        return Err("managingApp must include a DID and service fragment".to_string());
    }
    let app_view_host_name = state.db.get_config_property("AppViewHostName")
        .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
    let actor = state.lfs.resolve_actor_info(app_did, Some(60 * 3), &app_view_host_name)
        .await
        .map_err(|e| format!("failed to resolve managing app DID: {e}"))?;
    let did_doc = actor.did_doc
        .ok_or_else(|| "managing app DID has no DID document".to_string())?;
    let did_doc: Value = serde_json::from_str(&did_doc)
        .map_err(|_| "managing app DID document is invalid".to_string())?;
    let fragment_id = format!("#{service_fragment}");
    let endpoint = did_doc.get("service").and_then(Value::as_array).and_then(|services| {
        services.iter().find_map(|service| {
            let service_id = service.get("id").and_then(Value::as_str)?;
            (service_id == fragment_id || service_id == managing_app)
                .then(|| service.get("serviceEndpoint").and_then(Value::as_str))
                .flatten()
        })
    }).ok_or_else(|| "managing app service endpoint is missing".to_string())?;
    if !is_valid_outbound_url(endpoint) {
        return Err("managing app service endpoint is not a permitted URL".to_string());
    }
    let authority = state.db.get_config_property("UserDid")
        .map_err(|_| "space authority DID is not configured".to_string())?;
    let private_key = state.db.get_config_property("UserPrivateKeyMultibase")
        .map_err(|_| "space authority signing key is not configured".to_string())?;
    let token = sign_service_auth_token(
        &private_key,
        &authority,
        managing_app,
        Some("com.atproto.simplespace.checkUserAccess"),
        60,
    ).map_err(|e| format!("failed to sign managing-app request: {e}"))?;
    let url = format!("{}/xrpc/com.atproto.simplespace.checkUserAccess", endpoint.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("failed to create managing-app HTTP client: {e}"))?;
    let response = client.get(url).bearer_auth(token)
        .query(&[("space", space), ("user", user)])
        .send().await
        .map_err(|e| format!("managing app request failed: {e}"))?;
    if !response.status().is_success() {
        return Err(format!("managing app rejected the access check with HTTP {}", response.status()));
    }
    let body: Value = response.json().await
        .map_err(|e| format!("managing app returned invalid JSON: {e}"))?;
    body.get("authorized").and_then(Value::as_bool)
        .ok_or_else(|| "managing app response is missing authorized".to_string())
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
    if delegated_space != body.space
        || audience != format!("{}#atproto_space_host", authority)
        || exp <= chrono::Utc::now().timestamp()
    {
        return error(
            StatusCode::UNAUTHORIZED,
            "InvalidDelegationToken",
            "Delegation token does not match this space",
        );
    }

    let public_key = if issuer == configured_did {
        match state.db.get_config_property("UserPublicKeyMultibase") {
            Ok(key) => key,
            Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "ServerError", "Signing key not configured"),
        }
    } else {
        let app_view_host_name = state.db.get_config_property("AppViewHostName")
            .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
        let actor = match state.lfs.resolve_actor_info(&issuer, Some(60 * 3), &app_view_host_name).await {
            Ok(actor) => actor,
            Err(_) => return error(StatusCode::UNAUTHORIZED, "InvalidDelegationToken", "Delegation issuer could not be resolved"),
        };
        let did_doc = match actor.did_doc {
            Some(doc) => doc,
            None => return error(StatusCode::UNAUTHORIZED, "InvalidDelegationToken", "Delegation issuer has no DID document"),
        };
        match BlueskyClient::extract_public_key_from_did_doc(&did_doc) {
            Ok(key) => key,
            Err(_) => return error(StatusCode::UNAUTHORIZED, "InvalidDelegationToken", "Delegation issuer has no atproto key"),
        }
    };
    if !verify_service_auth_token(&token, &public_key).unwrap_or(false) {
        return error(
            StatusCode::UNAUTHORIZED,
            "InvalidDelegationToken",
            "Delegation token signature is invalid",
        );
    }

    let space = match state.db.get_simple_space(authority, uri_parts[2], uri_parts[3]) {
        Ok(Some(space)) => space,
        Ok(None) => return error(StatusCode::NOT_FOUND, "SpaceNotFound", "Space not found"),
        Err(e) => {
            state.log.error(&format!("[AUTH] [SPACE] Failed to read space: {e}"));
            return error(StatusCode::INTERNAL_SERVER_ERROR, "ServerError", "Failed to read space");
        }
    };
    let policy: Value = match serde_json::from_str(&space.policy_json) {
        Ok(policy) => policy,
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "ServerError", "Stored policy is invalid"),
    };
    let app_access: Value = match serde_json::from_str(&space.app_access_json) {
        Ok(app_access) => app_access,
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "ServerError", "Stored appAccess is invalid"),
    };
    if app_access.get("$type").and_then(Value::as_str) != Some("com.atproto.simplespace.defs#open") {
        return error(StatusCode::BAD_REQUEST, "UnsupportedAppAccess", "This server cannot enforce the stored appAccess policy");
    }
    match policy.get("$type").and_then(Value::as_str) {
        Some("com.atproto.simplespace.defs#publicPolicy") => {}
        Some("com.atproto.simplespace.defs#managingAppPolicy") => {
            let managing_app = match policy.get("managingApp").and_then(Value::as_str) {
                Some(app) => app,
                None => return error(StatusCode::INTERNAL_SERVER_ERROR, "ServerError", "Stored managingApp policy is invalid"),
            };
            match check_managing_app_access(&state, managing_app, &body.space, &issuer).await {
                Ok(true) => {}
                Ok(false) => return error(StatusCode::FORBIDDEN, "AccessDenied", "User is not authorized for this space"),
                Err(e) => {
                    state.log.warning(&format!("[AUTH] [SPACE] Managing app check failed: {e}"));
                    return error(StatusCode::BAD_GATEWAY, "ManagingAppUnavailable", "Unable to authorize access with the managing app");
                }
            }
        }
        _ => return error(StatusCode::BAD_REQUEST, "UnsupportedPolicy", "This server cannot enforce the stored space policy"),
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
