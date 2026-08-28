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
use serde_json::Value as JsonValue;

use crate::pds::auth::sign_service_auth_token;
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::uri::AtprotoProxy;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info};
use super::is_valid_outbound_url;

/// Default Atproto-Proxy value for the Bluesky AppView.
const DEFAULT_ATPROTO_PROXY: &str = "did:web:api.bsky.app#bsky_appview";




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
        name: format!("{}", path),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

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
        state.log.warning(&format!(
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
    let app_view_host_name = state.db.get_config_property("AppViewHostName")
        .unwrap_or_else(|_| DEFAULT_APP_VIEW_HOST_NAME.to_string());
    let cache_expiry_minutes: u64 = 60 * 3;
    let actor_info = match state.lfs.resolve_actor_info(&atproto_proxy.did, Some(cache_expiry_minutes), &app_view_host_name).await {
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

/// Maximum number of body bytes logged for an unimplemented XRPC request.
const MAX_LOGGED_BODY_BYTES: usize = 8 * 1024;

/// Log the full URL and payload of an unimplemented /xrpc request.
///
/// This is a discovery aid: when a client (for example an atproto spaces
/// implementation) calls an endpoint this PDS does not implement yet, the
/// request is logged in enough detail to build the missing handler.
///
/// Sensitive headers (authorization, cookie, dpop) are redacted.
fn log_unimplemented_xrpc_request(
    state: &Arc<PdsState>,
    method: &Method,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    body: &Bytes,
) {
    log_xrpc_request_details(
        "[UNIMPLEMENTED_XRPC]",
        &|line| state.log.warning(line),
        method,
        path,
        query,
        headers,
        body,
    );
}

/// Log the full URL and payload of an incoming /xrpc request at info level.
///
/// This is a debugging aid controlled by the `LogXrpcEndpoints` config
/// property: when a client calls one of the configured endpoints, the request
/// is logged in enough detail to inspect exactly what was sent.
///
/// Sensitive headers (authorization, cookie, dpop) are redacted.
pub fn log_xrpc_request(
    state: &Arc<PdsState>,
    method: &Method,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    body: &Bytes,
) {
    log_xrpc_request_details(
        "[LOG_XRPC]",
        &|line| state.log.info(line),
        method,
        path,
        query,
        headers,
        body,
    );
}

/// Shared implementation for logging the details of an /xrpc request.
///
/// `prefix` is prepended to every logged line and `log_line` is the sink that
/// receives each formatted line (for example, an info- or warning-level logger).
///
/// Sensitive headers (authorization, cookie, dpop) are redacted.
fn log_xrpc_request_details(
    prefix: &str,
    log_line: &dyn Fn(&str),
    method: &Method,
    path: &str,
    query: &str,
    headers: &HeaderMap,
    body: &Bytes,
) {
    let nsid = path.strip_prefix("/xrpc/").unwrap_or(path);

    log_line(&format!("{} ----------------------------------------", prefix));
    log_line(&format!("{} method: {}", prefix, method));
    log_line(&format!("{} nsid:   {}", prefix, nsid));
    log_line(&format!("{} url:    {}{}", prefix, path, query));

    // Query parameters, one per line for readability.
    let query_params = query.trim_start_matches('?');
    if !query_params.is_empty() {
        for pair in query_params.split('&') {
            log_line(&format!("{} query:  {}", prefix, pair));
        }
    }

    // Headers (with sensitive values redacted).
    let redacted: HashSet<&str> = ["authorization", "cookie", "dpop", "set-cookie"]
        .iter()
        .cloned()
        .collect();
    for (name, value) in headers.iter() {
        let name_lower = name.as_str().to_lowercase();
        let value_str = if redacted.contains(name_lower.as_str()) {
            "<redacted>".to_string()
        } else {
            value.to_str().unwrap_or("<non-utf8>").to_string()
        };
        log_line(&format!("{} header: {}: {}", prefix, name_lower, value_str));
    }

    // Body / payload.
    if body.is_empty() {
        log_line(&format!("{} body:   <empty>", prefix));
    } else {
        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<unknown>");
        log_line(&format!(
            "{} body:   {} bytes, content-type: {}",
            prefix,
            body.len(),
            content_type
        ));

        let truncated = body.len() > MAX_LOGGED_BODY_BYTES;
        let slice = &body[..body.len().min(MAX_LOGGED_BODY_BYTES)];

        match std::str::from_utf8(slice) {
            Ok(text) => {
                // Pretty-print JSON when possible, otherwise log the raw text.
                let rendered = serde_json::from_str::<JsonValue>(text)
                    .ok()
                    .and_then(|v| serde_json::to_string_pretty(&v).ok())
                    .unwrap_or_else(|| text.to_string());

                for line in rendered.lines() {
                    log_line(&format!("{} body:   {}", prefix, line));
                }
            }
            Err(_) => {
                log_line(&format!("{} body:   <binary, not logged>", prefix));
            }
        }

        if truncated {
            log_line(&format!(
                "{} body:   <truncated, {} of {} bytes shown>",
                prefix,
                MAX_LOGGED_BODY_BYTES,
                body.len()
            ));
        }
    }

    log_line(&format!("{} ----------------------------------------", prefix));
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
        name: "501 Not Implemented".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    state
        .log
        .warning(&format!("[NOT_IMPLEMENTED] {} {}{}", method, path, query));

    // For unimplemented /xrpc endpoints, log the full request details (URL + payload)
    // so that missing XRPC endpoints can be discovered and implemented.
    if path.starts_with("/xrpc") {
        log_unimplemented_xrpc_request(&state, &method, &path, &query, &headers, &body);
    }

    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "error": "MethodNotImplemented",
            "message": format!("Endpoint not implemented: {}", path)
        })),
    )
        .into_response()
}
