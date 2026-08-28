//! the full xrpc implementation for an atproto PDS
//!
//! This module provides handlers for AT Protocol XRPC endpoints.
//! Each endpoint is implemented in its own submodule.

mod activate_account;
mod app_bsky_proxy;
mod apply_writes;
mod check_account_status;
mod create_record;
mod create_session;
mod deactivate_account;
mod delete_record;
mod describe_repo;
mod describe_server;
mod favicon;
mod get_blob;
mod get_preferences;
mod get_record;
mod get_service_auth;
mod get_session;
mod health;
mod hello;
mod list_blobs;
mod list_records;
mod put_preferences;
mod put_record;
mod refresh_session;
mod resolve_handle;
mod root;
mod subscribe_repos;
mod sync_get_record;
mod sync_get_repo;
mod sync_get_repo_status;
mod sync_list_repos;
mod upload_blob;
mod well_known_atproto_did;
mod well_known_did;

pub use activate_account::activate_account;
pub use app_bsky_proxy::{app_bsky_fallback, log_xrpc_request};
pub use apply_writes::apply_writes;
pub use check_account_status::check_account_status;
pub use create_record::create_record;
pub use create_session::create_session;
pub use deactivate_account::deactivate_account;
pub use delete_record::delete_record;
pub use describe_repo::describe_repo;
pub use describe_server::describe_server;
pub use favicon::favicon;
pub use get_blob::get_blob;
pub use get_preferences::get_preferences;
pub use get_record::get_record;
pub use get_service_auth::get_service_auth;
pub use get_session::get_session;
pub use health::health;
pub use hello::hello;
pub use list_blobs::list_blobs;
pub use list_records::list_records;
pub use put_preferences::put_preferences;
pub use put_record::put_record;
pub use refresh_session::refresh_session;
pub use resolve_handle::resolve_handle;
pub use root::root;
pub use subscribe_repos::subscribe_repos;
pub use sync_get_record::sync_get_record;
pub use sync_get_repo::sync_get_repo;
pub use sync_get_repo_status::sync_get_repo_status;
pub use sync_list_repos::sync_list_repos;
pub use upload_blob::upload_blob;
pub use well_known_atproto_did::well_known_atproto_did;
pub use well_known_did::well_known_did;


use reqwest::Url;
use std::net::SocketAddr;
use axum::http::HeaderMap;


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


/// Extract caller IP address and User-Agent from request headers.
///
/// This is the single, canonical implementation used across every layer of the
/// PDS (admin dashboard, OAuth endpoints, XRPC handlers, and the server
/// logging middleware).
///
/// IP address resolution order:
/// 1. `X-Forwarded-For` header (set by reverse proxies like Caddy). When the
///    header contains a comma-separated list, the last (right-most) entry is
///    used and trimmed of surrounding whitespace. Caddy *appends* the real
///    connecting IP to the right of any client-supplied value, so the
///    right-most entry is the trustworthy one and left-most entries may be
///    spoofed by the client. This assumes the PDS runs directly behind a
///    single trusted Caddy reverse proxy (its last hop).
/// 2. The direct connection socket address, when provided.
/// 3. The literal string `""` when neither is available.
///
/// The `User-Agent` header is returned verbatim, or `""` when absent.
///
///
/// # Security
///
/// `X-Forwarded-For` is client-controllable and can be spoofed unless a trusted
/// reverse proxy overwrites it. The returned values are only used for logging,
/// statistics, and audit records here; they must NOT be used as an
/// authentication or authorization control (e.g. rate limiting, lockouts, or
/// allowlists) without additional trust guarantees.
pub fn get_caller_info(headers: &HeaderMap, socket_addr: Option<SocketAddr>) -> (String, String) {

    // Get User-Agent
    let user_agent = headers
        .get("User-Agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "".to_string());

    // Get IP address from X-Forwarded-For, or fall back to the socket address.
    let ip_address = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            // X-Forwarded-For can contain multiple IPs. Caddy appends the real
            // connecting IP to the right, so take the last (right-most) entry;
            // left-most entries are client-supplied and can be spoofed.
            s.rsplit(',').next().unwrap_or(s).trim().to_string()
        })
        .unwrap_or_else(|| {
            socket_addr
                .map(|addr| addr.ip().to_string())
                .unwrap_or_else(|| "".to_string())
        });

    (ip_address, user_agent)
}

