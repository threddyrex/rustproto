//! AppView proxy for app.bsky.* and chat.bsky.* endpoints.
//!
//! Many PDS endpoints are forwarded to the AppView.
//! This module handles that proxying for app.bsky.* and chat.bsky.* routes
//! that don't have specific handlers on the PDS.
//!
//! See: <https://docs.bsky.app/docs/advanced-guides/api-directory>

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    body::{Body, Bytes},
    extract::{ConnectInfo, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::Url;
use serde_json::Value as JsonValue;

use crate::pds::auth::sign_service_auth_token;
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info};

/// Default Atproto-Proxy value for the Bluesky AppView.
const DEFAULT_ATPROTO_PROXY: &str = "did:web:api.bsky.app#bsky_appview";

/// Parsed Atproto-Proxy header value.
struct AtprotoProxy {
    /// The DID of the service to proxy to.
    did: String,
    /// The service ID within the DID document (e.g., "bsky_appview").
    service_id: String,
}

impl AtprotoProxy {
    /// Parse an Atproto-Proxy header value.
    ///
    /// Format: `did:web:api.bsky.app#bsky_appview`
    fn from_header(header_value: &str) -> Option<Self> {
        if header_value.is_empty() {
            return None;
        }

        let parts: Vec<&str> = header_value.split('#').collect();
        if parts.len() != 2 {
            return None;
        }

        if !parts[0].starts_with("did:") {
            return None;
        }

        Some(AtprotoProxy {
            did: parts[0].to_string(),
            service_id: parts[1].to_string(),
        })
    }
}

/// Validates that a URL is safe for outbound requests (SSRF protection).
///
/// Blocks localhost, private IPs, cloud metadata endpoints, and non-HTTPS schemes.
pub fn is_valid_outbound_url(url: &str) -> bool {
    let parsed_url = match Url::parse(url) {
        Ok(u) => u,
        Err(_) => return false,
    };

    // Only allow HTTPS
    if parsed_url.scheme() != "https" {
        return false;
    }

    let host = match parsed_url.host_str() {
        Some(h) => h,
        None => return false,
    };

    is_valid_outbound_host(host)
}

/// Validates that a hostname is safe for outbound requests.
pub fn is_valid_outbound_host(hostname: &str) -> bool {
    if hostname.is_empty() {
        return false;
    }

    // Block URL injection characters
    if hostname.contains('?')
        || hostname.contains('#')
        || hostname.contains('/')
        || hostname.contains('@')
        || hostname.contains('\\')
        || hostname.contains(' ')
        || hostname.contains('\t')
        || hostname.contains('\r')
        || hostname.contains('\n')
    {
        return false;
    }

    // Block colon except in IPv6 addresses
    if hostname.contains(':') && !hostname.starts_with('[') {
        return false;
    }

    // Block localhost variants
    if hostname.eq_ignore_ascii_case("localhost") {
        return false;
    }

    // Block internal domain suffixes
    let lower = hostname.to_lowercase();
    if lower.ends_with(".local")
        || lower.ends_with(".internal")
        || lower.ends_with(".localhost")
    {
        return false;
    }

    // Block private/loopback IP addresses
    if let Ok(ip) = hostname.parse::<std::net::IpAddr>() {
        // Loopback
        if ip.is_loopback() {
            return false;
        }

        match ip {
            std::net::IpAddr::V4(v4) => {
                let octets = v4.octets();

                // Cloud metadata endpoint (169.254.169.254)
                if octets[0] == 169
                    && octets[1] == 254
                    && octets[2] == 169
                    && octets[3] == 254
                {
                    return false;
                }

                // 10.0.0.0/8
                if octets[0] == 10 {
                    return false;
                }

                // 172.16.0.0/12
                if octets[0] == 172 && (16..=31).contains(&octets[1]) {
                    return false;
                }

                // 192.168.0.0/16
                if octets[0] == 192 && octets[1] == 168 {
                    return false;
                }

                // Link-local 169.254.0.0/16
                if octets[0] == 169 && octets[1] == 254 {
                    return false;
                }
            }
            std::net::IpAddr::V6(_) => {
                // For IPv6, just reject loopback (already handled above)
                // Could add more checks for link-local, etc.
            }
        }
    }

    true
}

/// Extract the service endpoint from a DID document.
fn extract_service_endpoint_from_did_doc(
    did_doc: &str,
    service_id: &str,
) -> Option<String> {
    let doc: JsonValue = serde_json::from_str(did_doc).ok()?;
    let services = doc.get("service")?.as_array()?;

    let target_id = format!("#{}", service_id);

    for service in services {
        if let Some(id) = service.get("id").and_then(|v| v.as_str()) {
            if id == target_id {
                if let Some(endpoint) = service.get("serviceEndpoint").and_then(|v| v.as_str())
                {
                    return Some(endpoint.to_string());
                }
            }
        }
    }

    None
}

/// Proxy a request to the AppView.
///
/// This handles /xrpc/app.bsky.* and /xrpc/chat.bsky.* endpoints by forwarding
/// them to the appropriate AppView service with service authentication.
///
/// # Arguments
///
/// * `state` - The PDS server state
/// * `method` - The HTTP method
/// * `path` - The request path (e.g., "/xrpc/app.bsky.feed.getTimeline")
/// * `query` - The query string (including leading '?')
/// * `headers` - The request headers
/// * `body` - The request body (for POST requests)
///
/// # Returns
///
/// The proxied response from the AppView.
pub async fn proxy_to_appview(
    State(state): State<Arc<PdsState>>,
    method: Method,
    path: String,
    query: String,
    headers: HeaderMap,
    body: Bytes,
    socket_addr: Option<SocketAddr>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, socket_addr);

    // Increment statistics
    let stat_key = StatisticKey {
        name: format!("xrpc/proxy:{}", path),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication (supports Legacy and OAuth)
    let auth_result = check_user_auth(
        &state,
        &headers,
        None,
        method.as_str(),
        &path,
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Get Atproto-Proxy header or use default
    let proxy_header = headers
        .get("Atproto-Proxy")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(DEFAULT_ATPROTO_PROXY);

    let atproto_proxy = match AtprotoProxy::from_header(proxy_header) {
        Some(p) => p,
        None => {
            state
                .log
                .error("[PROXY] Invalid Atproto-Proxy header value");
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "InvalidRequest",
                    "message": "Invalid Atproto-Proxy header value"
                })),
            )
                .into_response();
        }
    };

    // Check against allow list
    let allow_list: HashSet<String> = state
        .db
        .get_config_property_hash_set("AtprotoProxyAllowedDids")
        .unwrap_or_default();

    if !allow_list.contains(&atproto_proxy.did) {
        state.log.error(&format!(
            "[PROXY] Atproto proxy DID not in allow list: {}",
            atproto_proxy.did
        ));
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "Unauthorized",
                "message": "Proxy DID not authorized"
            })),
        )
            .into_response();
    }

    // Resolve DID document for the proxy DID (LFS cache with 3-hour expiry)
    let cache_expiry_minutes: u64 = 60 * 3;
    let actor_info = match state.lfs.resolve_actor_info(&atproto_proxy.did, Some(cache_expiry_minutes)).await {
        Ok(info) => info,
        Err(e) => {
            state.log.error(&format!(
                "[PROXY] Unable to resolve actor info for DID {}: {}",
                atproto_proxy.did, e
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "InvalidRequest",
                    "message": "Unable to resolve actor info for DID"
                })),
            )
                .into_response();
        }
    };

    let did_doc = match actor_info.did_doc {
        Some(doc) => doc,
        None => {
            state.log.error(&format!(
                "[PROXY] No DID document found for DID: {}",
                atproto_proxy.did
            ));
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "InvalidRequest",
                    "message": "Unable to resolve DID document"
                })),
            )
                .into_response();
        }
    };

    // Extract service endpoint
    let service_endpoint =
        match extract_service_endpoint_from_did_doc(&did_doc, &atproto_proxy.service_id) {
            Some(ep) => ep,
            None => {
                state.log.error(&format!(
                    "[PROXY] Unable to find service endpoint for {}#{}",
                    atproto_proxy.did, atproto_proxy.service_id
                ));
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "InvalidRequest",
                        "message": "Unable to find service endpoint in DID document"
                    })),
                )
                    .into_response();
            }
        };

    // Validate service endpoint URL (SSRF protection)
    if !is_valid_outbound_url(&service_endpoint) {
        state.log.error(&format!(
            "[SECURITY] Blocked invalid or internal service endpoint: {}",
            service_endpoint
        ));
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "InvalidRequest",
                "message": "Invalid service endpoint"
            })),
        )
            .into_response();
    }

    // Build target URL
    let target_url = format!("{}{}{}", service_endpoint, path, query);
    state.log.trace(&format!("[PROXY] Proxying to: {}", target_url));

    // Extract lexicon method from path (remove /xrpc/ prefix)
    let lxm = path
        .strip_prefix("/xrpc/")
        .unwrap_or(&path)
        .to_string();

    // Get signing key for service auth
    let private_key = match state.db.get_config_property("UserPrivateKeyMultibase") {
        Ok(key) => key,
        Err(_) => {
            state.log.error("[PROXY] Signing key not found");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "InternalServerError",
                    "message": "Signing key not found"
                })),
            )
                .into_response();
        }
    };

    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            state.log.error("[PROXY] User DID not found");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "InternalServerError",
                    "message": "User DID not found"
                })),
            )
                .into_response();
        }
    };

    // Create service auth JWT (5 minute expiry)
    let service_auth_jwt = match sign_service_auth_token(
        &private_key,
        &user_did,
        &atproto_proxy.did,
        Some(&lxm),
        300,
    ) {
        Ok(token) => token,
        Err(e) => {
            state
                .log
                .error(&format!("[PROXY] Failed to sign service auth token: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": "InternalServerError",
                    "message": "Failed to create service authentication"
                })),
            )
                .into_response();
        }
    };

    // Build the outgoing request
    let http_client = reqwest::Client::new();
    let mut request_builder = match method {
        Method::GET => http_client.get(&target_url),
        Method::POST => http_client.post(&target_url),
        Method::PUT => http_client.put(&target_url),
        Method::DELETE => http_client.delete(&target_url),
        _ => {
            return (
                StatusCode::METHOD_NOT_ALLOWED,
                Json(serde_json::json!({
                    "error": "MethodNotAllowed",
                    "message": "Method not supported for proxying"
                })),
            )
                .into_response();
        }
    };

    // Add service auth JWT
    request_builder = request_builder.header("Authorization", format!("Bearer {}", service_auth_jwt));

    // Copy headers from original request (excluding some)
    let excluded_headers: HashSet<&str> = [
        "host",
        "connection",
        "authorization",
        "atproto-proxy",
        "content-length",
    ]
    .iter()
    .cloned()
    .collect();

    for (name, value) in headers.iter() {
        let name_lower = name.as_str().to_lowercase();

        // Skip excluded headers and X-Forwarded-* headers
        if excluded_headers.contains(name_lower.as_str())
            || name_lower.starts_with("x-forwarded-")
        {
            continue;
        }

        // Skip gzip accept-encoding (let reqwest handle compression)
        if name_lower == "accept-encoding" {
            if let Ok(v) = value.to_str() {
                if v.contains("gzip") {
                    continue;
                }
            }
        }

        if let (Ok(header_name), Ok(header_value)) = (
            HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            request_builder = request_builder.header(header_name, header_value);
        }
    }

    // Add body for POST requests
    if method == Method::POST && !body.is_empty() {
        if let Some(content_type) = headers.get("content-type").and_then(|v| v.to_str().ok()) {
            request_builder = request_builder.header("Content-Type", content_type);
        }
        request_builder = request_builder.body(body.to_vec());
    }

    // Send the request
    let response = match request_builder.send().await {
        Ok(resp) => resp,
        Err(e) => {
            state
                .log
                .error(&format!("[PROXY] Error proxying to AppView: {}", e));
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": "BadGateway",
                    "message": "Error proxying request to AppView"
                })),
            )
                .into_response();
        }
    };

    let status = StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    let response_body = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            state.log.error(&format!(
                "[PROXY] Error reading response from AppView: {}",
                e
            ));
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": "BadGateway",
                    "message": "Error reading response from AppView"
                })),
            )
                .into_response();
        }
    };

    // If response is empty, return just the status code
    if response_body.is_empty() {
        return status.into_response();
    }

    // Build response with appropriate headers
    let builder = Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .header("Cache-Control", "private")
        .header("Vary", "Authorization");

    // Build and return the response
    builder
        .body(Body::from(response_body.to_vec()))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Handler for fallback app.bsky.*/chat.bsky.* routes.
///
/// This is used as the catch-all for app.bsky.* and chat.bsky.* endpoints.
pub async fn app_bsky_fallback(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    method: Method,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_string();
    let query = uri.query().map(|q| format!("?{}", q)).unwrap_or_default();

    // Check if this is an app.bsky or chat.bsky route
    if path.starts_with("/xrpc/app.bsky.") || path.starts_with("/xrpc/chat.bsky.") {
        return proxy_to_appview(State(state), method, path, query, headers, body, Some(addr)).await;
    }

    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // For non-app.bsky routes, return 501 Not Implemented
    let stat_key = StatisticKey {
        name: "xrpc/unimplemented".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    state
        .log
        .warning(&format!("[UNIMPLEMENTED] {} {}{}", method, path, query));

    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "error": "MethodNotImplemented",
            "message": format!("Endpoint not implemented: {}", path)
        })),
    )
        .into_response()
}
