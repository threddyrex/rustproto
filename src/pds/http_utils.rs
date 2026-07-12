//! http utils
//!
//! This module provides small helpers that operate on incoming HTTP requests
//! and are shared across the admin, oauth, xrpc, and server layers.

use std::net::SocketAddr;

use axum::http::HeaderMap;

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
/// 3. The literal string `"[_UNKNOWN_]"` when neither is available.
///
/// The `User-Agent` header is returned verbatim, or `"[_UNKNOWN_]"` when absent.
///
/// Requests originating from UptimeRobot (identified via the User-Agent) are
/// grouped under the `[_UPTIME_ROBOT_]` IP so monitoring traffic from its many
/// source IPs is aggregated in statistics.
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
        .unwrap_or_else(|| "[_UNKNOWN_]".to_string());

    // Get IP address from X-Forwarded-For, or fall back to the socket address.
    let mut ip_address = headers
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
                .unwrap_or_else(|| "[_UNKNOWN_]".to_string())
        });

    // Group UptimeRobot requests together (they come from many IPs).
    if user_agent.contains("www.uptimerobot.com") {
        ip_address = "[_UPTIME_ROBOT_]".to_string();
    }

    (ip_address, user_agent)
}
