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
use serde_json::Value;

use crate::pds::auth::{
    sign_service_auth_token, sign_space_credential, verify_service_auth_token,
    DELEGATION_TOKEN_TYP, SPACE_CREDENTIAL_TTL_SECS,
};
use crate::pds::db::{
    PdsDbError, StatisticKey,
};
use crate::pds::oauth::{get_hostname, validate_dpop};
use crate::pds::server::PdsState;
use crate::pds::spaces::{SpaceUri};
use crate::ws::{DEFAULT_APP_VIEW_HOST_NAME};

use crate::pds::xrpc::is_valid_outbound_url;
use crate::pds::auth::{extract_atproto_public_key};
use crate::pds::spaces::{is_spaces_enabled, spaces_disabled_response};
use crate::pds::xrpc::{get_caller_info};

/// Known `policy` union variants (user-access policy), mirroring
/// `com.atproto.simplespace.createSpace`.
const POLICY_PUBLIC: &str = "com.atproto.simplespace.defs#publicPolicy";
const POLICY_MEMBER_LIST: &str = "com.atproto.simplespace.defs#memberListPolicy";
const POLICY_MANAGING_APP: &str = "com.atproto.simplespace.defs#managingAppPolicy";

/// Lexicon method the authority calls on a space's managing app to ask whether
/// a user is authorized, under `managingAppPolicy`.
const CHECK_USER_ACCESS_LXM: &str = "com.atproto.simplespace.checkUserAccess";

/// Lifetime of the service-auth token minted for a `checkUserAccess` call.
const CHECK_USER_ACCESS_AUTH_TTL_SECS: i64 = 60;

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
    let space_param = match body.space {
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
    if space_uri.authority != user_did {
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

    // Verify the delegation token: structure, claims, and signature. The
    // signature is checked against the *delegating user's* own signing key
    // (resolved from their DID document), not this authority's key, so a user
    // hosted elsewhere can delegate access to this authority's space. Returns
    // the delegating user's DID.
    let delegating_did =
        match verify_delegation_token(&state, &delegation_token, &space_uri).await {
            Ok(did) => did,
            Err(message) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "InvalidDelegationToken",
                    &message,
                );
            }
        };

    // The user-to-app delegation is proven. Authorize the delegating user
    // against the space's user-access policy. Personal-data spaces anchored on
    // this account's own DID (owner == delegating user) are authorized by
    // default; any other user is admitted only if the space's persisted policy
    // permits it.
    if let Err(response) =
        authorize_user_for_space(&state, &space_uri, &user_did, &delegating_did).await
    {
        return response;
    }

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
        &space_uri.to_string(),
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

/// Verify a delegation token presented for `space_id`. The token's claims are
/// validated and its signature is checked against the *issuer's* (delegating
/// user's) `#atproto` signing key. Returns the issuer DID on success, or
/// `Err(message)` on any failure.
///
/// The issuer key is resolved from the issuer's DID document. As a fast path
/// (and so the flow works before the account is federated), when the issuer is
/// this account itself the locally configured public key is used instead of a
/// network resolution.
async fn verify_delegation_token(
    state: &Arc<PdsState>,
    token: &str,
    space_uri: &SpaceUri,
) -> Result<String, String> {
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
    // The signature is verified against the issuer's `#atproto` key, so the
    // token must name that key. The spec fixes `kid` to `#atproto`; rejecting
    // anything else prevents a token that claims some other key from being
    // waved through by a verification it never actually committed to.
    if header.get("kid").and_then(|v| v.as_str()) != Some("#atproto") {
        return Err("Delegation token kid must be '#atproto'".to_string());
    }

    // Payload: validate the delegation claims.
    let payload = decode_jwt_segment(parts[1]).ok_or("Invalid delegation token payload")?;

    let sub = payload.get("sub").and_then(|v| v.as_str()).unwrap_or_default();
    if sub != space_uri.to_string() {
        return Err("Delegation token subject does not match the requested space".to_string());
    }

    // The audience is the space host of this authority.
    let expected_aud = format!("{}#atproto_space_host", space_uri.authority);
    let aud = payload.get("aud").and_then(|v| v.as_str()).unwrap_or_default();
    if aud != expected_aud {
        return Err("Delegation token audience does not match this space host".to_string());
    }

    // The issuer is the delegating user. It may be any user (not just this
    // authority): cross-account access is exactly the case this endpoint serves.
    // It must be a DID: the value later drives the local-key fast path and the
    // owner short-circuit (`delegating_did == owner_did`), so a degenerate or
    // empty issuer must never flow through.
    let issuer = payload.get("iss").and_then(|v| v.as_str()).unwrap_or_default();
    if !issuer.starts_with("did:") {
        return Err("Delegation token issuer must be a DID".to_string());
    }

    // Expiry.
    let now = chrono::Utc::now().timestamp();
    match payload.get("exp").and_then(|v| v.as_i64()) {
        Some(exp) if exp > now => {}
        Some(_) => return Err("Delegation token has expired".to_string()),
        None => return Err("Delegation token missing exp claim".to_string()),
    }

    // Resolve the issuer's `#atproto` public key and verify the signature.
    let public_key = resolve_issuer_public_key(state, issuer).await?;
    match verify_service_auth_token(token, &public_key) {
        Ok(true) => Ok(issuer.to_string()),
        Ok(false) => Err("Delegation token signature is invalid".to_string()),
        Err(e) => Err(format!("Failed to verify delegation token: {}", e)),
    }
}

/// Resolve the `#atproto` signing public key (multibase) for `issuer`.
///
/// When `issuer` is this account, the locally configured key is returned
/// directly; otherwise the issuer's DID document is resolved to extract it.
async fn resolve_issuer_public_key(
    state: &Arc<PdsState>,
    issuer: &str,
) -> Result<String, String> {
    if let Ok(user_did) = state.db.get_config_property("UserDid") {
        if !user_did.is_empty() && issuer == user_did {
            return state
                .db
                .get_config_property("UserPublicKeyMultibase")
                .map_err(|_| "Signing key not configured".to_string());
        }
    }

    let app_view_host_name = state
        .db
        .get_config_property("AppViewHostName")
        .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
    let did_doc = state
        .lfs
        .resolve_actor_info(issuer, None, &app_view_host_name)
        .await
        .ok()
        .and_then(|info| info.did_doc)
        .ok_or_else(|| {
            format!("Failed to resolve DID document for issuer '{}'", issuer)
        })?;

    extract_atproto_public_key(&did_doc).ok_or_else(|| {
        format!(
            "No atproto signing key in DID document for issuer '{}'",
            issuer
        )
    })
}

/// Authorize the delegating user (`delegating_did`) against the space's
/// user-access policy. Returns `Err(response)` with a ready-to-return error
/// response when the user is not permitted.
///
/// The owner of a space (personal-data spaces anchored on this account's own
/// DID) is always authorized without consulting a persisted policy. For any
/// other user the space must exist and its user-access `policy` must admit them.
/// Under `managingAppPolicy` the decision is delegated to the space's managing
/// app via a `com.atproto.simplespace.checkUserAccess` call.
async fn authorize_user_for_space(
    state: &Arc<PdsState>,
    space_uri: &SpaceUri,
    owner_did: &str,
    delegating_did: &str,
) -> Result<(), Response> {
    // The owner accessing their own space needs no persisted policy. Guard
    // against an empty owner DID (misconfiguration) so an equally empty value
    // can never satisfy this bypass.
    if !owner_did.is_empty() && delegating_did == owner_did {
        return Ok(());
    }

    // Any other user is admitted only by the space's persisted user-access policy.
    let space = match state.db.get_space(&space_uri.to_string()) {
        Ok(space) => space,
        Err(PdsDbError::SpaceNotFound(_)) => {
            return Err(error_response(
                StatusCode::NOT_FOUND,
                "SpaceNotFound",
                "No such space exists",
            ));
        }
        Err(e) => {
            state.log.error(&format!(
                "[SPACE] [CREDENTIAL] Failed to load space: {}",
                e
            ));
            return Err(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to load space",
            ));
        }
    };

    let policy: Value = serde_json::from_str(&space.policy_json).map_err(|e| {
        state.log.error(&format!(
            "[SPACE] [CREDENTIAL] Corrupt policy json: {}",
            e
        ));
        error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ServerError",
            "Stored space policy is corrupt",
        )
    })?;

    match evaluate_user_policy(&policy, delegating_did) {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::Deny(message) => {
            Err(error_response(StatusCode::FORBIDDEN, "NotAuthorized", &message))
        }
        PolicyDecision::AskManagingApp(managing_app) => {
            // Defer the decision to the space's managing app. The requesting
            // client (attested `client_id`) is not yet verified here, so it is
            // omitted; managing apps that gate solely on the user (e.g.
            // follower-gating) do not need it.
            match check_managing_app_access(
                state,
                &managing_app,
                &space_uri.to_string(),
                delegating_did,
                None,
            )
            .await
            {
                Ok(true) => Ok(()),
                Ok(false) => Err(error_response(
                    StatusCode::FORBIDDEN,
                    "NotAuthorized",
                    "The space's managing app denied access",
                )),
                Err(message) => {
                    state.log.error(&format!(
                        "[SPACE] [CREDENTIAL] Managing app access check failed: {}",
                        message
                    ));
                    Err(error_response(
                        StatusCode::BAD_GATEWAY,
                        "ManagingAppUnreachable",
                        "Failed to consult the space's managing app",
                    ))
                }
            }
        }
    }
}

/// The outcome of evaluating a space's user-access `policy` union.
enum PolicyDecision {
    /// The user is admitted outright.
    Allow,
    /// The user is denied, with an explanatory message.
    Deny(String),
    /// The decision must be delegated to the named managing app
    /// (`did#service` identifier) via `checkUserAccess`.
    AskManagingApp(String),
}

/// Evaluate a space's user-access `policy` union for `delegating_did`.
fn evaluate_user_policy(policy: &Value, delegating_did: &str) -> PolicyDecision {
    let ty = policy
        .get("$type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match ty {
        // Open to any authenticated user.
        POLICY_PUBLIC => PolicyDecision::Allow,
        // Admit users named in the policy's `members` list. A space with no
        // members list admits no one but its owner.
        POLICY_MEMBER_LIST => {
            let is_member = policy
                .get("members")
                .and_then(Value::as_array)
                .map(|members| {
                    members
                        .iter()
                        .filter_map(Value::as_str)
                        .any(|did| did == delegating_did)
                })
                .unwrap_or(false);
            if is_member {
                PolicyDecision::Allow
            } else {
                PolicyDecision::Deny("User is not a member of this space".to_string())
            }
        }
        // Membership is decided at mint time by an external managing app.
        POLICY_MANAGING_APP => match policy.get("managingApp").and_then(Value::as_str) {
            Some(app) if !app.is_empty() => PolicyDecision::AskManagingApp(app.to_string()),
            _ => PolicyDecision::Deny(
                "managingAppPolicy is missing a managingApp identifier".to_string(),
            ),
        },
        other => PolicyDecision::Deny(format!("Unsupported space policy: {}", other)),
    }
}

/// Ask a space's managing app whether `user_did` is authorized for `space_uri`,
/// via `com.atproto.simplespace.checkUserAccess`.
///
/// `managing_app` is a service identifier (`did#service`). The managing app's
/// service endpoint is resolved from its DID document, and the call is
/// authenticated with a service-auth token issued by this authority (`iss` =
/// this account, `aud` = the managing app), so the app can verify the request
/// genuinely originates from the space authority.
///
/// Returns `Ok(true)`/`Ok(false)` with the app's decision, or `Err(message)` if
/// the app could not be consulted.
async fn check_managing_app_access(
    state: &Arc<PdsState>,
    managing_app: &str,
    space_uri: &str,
    user_did: &str,
    client_id: Option<&str>,
) -> Result<bool, String> {
    // Split the `did#service` service identifier.
    let (app_did, service_fragment) = managing_app
        .split_once('#')
        .filter(|(did, frag)| did.starts_with("did:") && !frag.is_empty())
        .ok_or_else(|| {
            format!(
                "managingApp is not a valid service identifier: {}",
                managing_app
            )
        })?;

    // Resolve the managing app's DID document and service endpoint.
    let app_view_host_name = state
        .db
        .get_config_property("AppViewHostName")
        .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
    let did_doc = state
        .lfs
        .resolve_actor_info(app_did, None, &app_view_host_name)
        .await
        .ok()
        .and_then(|info| info.did_doc)
        .ok_or_else(|| format!("Failed to resolve DID document for managing app '{}'", app_did))?;

    let service_endpoint = space_service_endpoint(&did_doc, service_fragment)
        .ok_or_else(|| {
            format!(
                "No '{}' service endpoint in DID document for '{}'",
                service_fragment, app_did
            )
        })?;

    // SSRF protection: reject internal / malformed endpoints.
    if !is_valid_outbound_url(&service_endpoint) {
        return Err(format!(
            "Managing app service endpoint is not a valid outbound URL: {}",
            service_endpoint
        ));
    }

    // Mint a service-auth token addressed to the managing app.
    let private_key = state
        .db
        .get_config_property("UserPrivateKeyMultibase")
        .map_err(|_| "Signing key not configured".to_string())?;
    let authority_did = state
        .db
        .get_config_property("UserDid")
        .map_err(|_| "User DID not configured".to_string())?;
    let service_auth = sign_service_auth_token(
        &private_key,
        &authority_did,
        managing_app,
        Some(CHECK_USER_ACCESS_LXM),
        CHECK_USER_ACCESS_AUTH_TTL_SECS,
    )
    .map_err(|e| format!("Failed to sign service-auth token: {}", e))?;

    // Build and issue the query. Parameter and output names follow the
    // `com.atproto.simplespace.checkUserAccess` lexicon.
    let mut url = url::Url::parse(&service_endpoint)
        .map_err(|e| format!("Invalid managing app service endpoint: {}", e))?;
    url.set_path(&format!("/xrpc/{}", CHECK_USER_ACCESS_LXM));
    {
        let mut qp = url.query_pairs_mut();
        qp.append_pair("space", space_uri);
        qp.append_pair("user", user_did);
        if let Some(client_id) = client_id {
            qp.append_pair("clientId", client_id);
        }
    }

    let http_client = reqwest::Client::new();
    // Log the full set of items being sent to the managing app.
    state.log.info(&format!(
        "[SPACE] [CREDENTIAL] Calling managing app '{}' checkUserAccess: url={}, lxm={}, space={}, user={}, clientId={}",
        managing_app,
        url.as_str(),
        CHECK_USER_ACCESS_LXM,
        space_uri,
        user_did,
        client_id.unwrap_or("<none>"),
    ));
    let response = http_client
        .get(url.as_str())
        .header("Authorization", format!("Bearer {}", service_auth))
        .send()
        .await
        .map_err(|e| {
            let msg = format!("Request to managing app failed: {}", e);
            state.log.info(&format!(
                "[SPACE] [CREDENTIAL] Managing app '{}' response: {}",
                managing_app, msg
            ));
            msg
        })?;

    // Capture the status and raw body so the full response can be logged no
    // matter what happens next.
    let status = response.status();
    let body_text = response.text().await.map_err(|e| {
        let msg = format!("Invalid managing app response body: {}", e);
        state.log.info(&format!(
            "[SPACE] [CREDENTIAL] Managing app '{}' response: status={}, error={}",
            managing_app,
            status.as_u16(),
            msg
        ));
        msg
    })?;

    state.log.info(&format!(
        "[SPACE] [CREDENTIAL] Managing app '{}' response: status={}, body={}",
        managing_app,
        status.as_u16(),
        body_text
    ));

    if !status.is_success() {
        return Err(format!(
            "Managing app returned status {}",
            status.as_u16()
        ));
    }

    let body: Value = serde_json::from_str(&body_text)
        .map_err(|e| format!("Invalid managing app response body: {}", e))?;

    let allowed = body
        .get("authorized")
        .and_then(Value::as_bool)
        .ok_or_else(|| "Managing app response missing 'authorized' boolean".to_string())?;

    if !allowed {
        if let Some(reason) = body.get("reason").and_then(Value::as_str) {
            state.log.info(&format!(
                "[SPACE] [CREDENTIAL] Managing app '{}' denied {} for {}: {}",
                managing_app, user_did, space_uri, reason
            ));
        }
    }

    Ok(allowed)
}

/// Find the `serviceEndpoint` for `fragment` in a DID document's `service`
/// array.
///
/// A service's `id` may be written in relative (`#fragment`) or absolute
/// (`did:...#fragment`) form; both are matched on the trailing `#fragment`, with
/// the `#` anchoring the fragment boundary so partial matches are rejected.
fn space_service_endpoint(did_doc: &str, fragment: &str) -> Option<String> {
    let doc: Value = serde_json::from_str(did_doc).ok()?;
    let services = doc.get("service")?.as_array()?;

    let suffix = format!("#{}", fragment);
    for service in services {
        let id = service.get("id").and_then(Value::as_str).unwrap_or_default();
        if id.ends_with(&suffix) {
            if let Some(endpoint) = service.get("serviceEndpoint").and_then(Value::as_str) {
                return Some(endpoint.to_string());
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;


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

    #[test]
    fn public_policy_admits_any_user() {
        let policy = serde_json::json!({ "$type": POLICY_PUBLIC });
        assert!(matches!(
            evaluate_user_policy(&policy, "did:web:threddy.example"),
            PolicyDecision::Allow
        ));
    }

    #[test]
    fn member_list_policy_admits_only_listed_members() {
        let policy = serde_json::json!({
            "$type": POLICY_MEMBER_LIST,
            "members": ["did:web:threddy.example", "did:plc:abc"],
        });
        assert!(matches!(
            evaluate_user_policy(&policy, "did:web:threddy.example"),
            PolicyDecision::Allow
        ));
        assert!(matches!(
            evaluate_user_policy(&policy, "did:web:stranger.example"),
            PolicyDecision::Deny(_)
        ));

        // An empty or absent members list admits no one.
        let empty = serde_json::json!({ "$type": POLICY_MEMBER_LIST });
        assert!(matches!(
            evaluate_user_policy(&empty, "did:web:threddy.example"),
            PolicyDecision::Deny(_)
        ));
    }

    #[test]
    fn managing_app_policy_defers_to_the_named_app() {
        let policy = serde_json::json!({
            "$type": POLICY_MANAGING_APP,
            "managingApp": "did:web:bulletin.my#bulletin",
        });
        match evaluate_user_policy(&policy, "did:web:threddy.example") {
            PolicyDecision::AskManagingApp(app) => {
                assert_eq!(app, "did:web:bulletin.my#bulletin")
            }
            _ => panic!("expected managing-app deferral"),
        }

        // A managing-app policy without an identifier is denied.
        let no_app = serde_json::json!({ "$type": POLICY_MANAGING_APP });
        assert!(matches!(
            evaluate_user_policy(&no_app, "did:web:threddy.example"),
            PolicyDecision::Deny(_)
        ));
    }

    #[test]
    fn unknown_policy_denies() {
        let unknown = serde_json::json!({ "$type": "com.atproto.simplespace.defs#nope" });
        assert!(matches!(
            evaluate_user_policy(&unknown, "did:web:threddy.example"),
            PolicyDecision::Deny(_)
        ));
    }

    #[test]
    fn resolves_service_endpoint_for_relative_and_absolute_ids() {
        // Relative service id (`#bulletin`).
        let relative = serde_json::json!({
            "service": [{
                "id": "#bulletin",
                "type": "AtprotoSpaceService",
                "serviceEndpoint": "https://bulletin.my"
            }]
        })
        .to_string();
        assert_eq!(
            space_service_endpoint(&relative, "bulletin").as_deref(),
            Some("https://bulletin.my")
        );

        // Absolute service id (`did:web:bulletin.my#bulletin`), as emitted by
        // the bulletin app's did:web document.
        let absolute = serde_json::json!({
            "service": [{
                "id": "did:web:bulletin.my#bulletin",
                "type": "AtprotoSpaceService",
                "serviceEndpoint": "https://bulletin.my"
            }]
        })
        .to_string();
        assert_eq!(
            space_service_endpoint(&absolute, "bulletin").as_deref(),
            Some("https://bulletin.my")
        );
    }

    #[test]
    fn service_endpoint_rejects_partial_fragment_match() {
        let doc = serde_json::json!({
            "service": [{
                "id": "did:web:bulletin.my#superbulletin",
                "type": "AtprotoSpaceService",
                "serviceEndpoint": "https://bulletin.my"
            }]
        })
        .to_string();
        assert_eq!(space_service_endpoint(&doc, "bulletin"), None);
    }
}
