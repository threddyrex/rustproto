//! Admin home page handler.
//!
//! Displays the main admin dashboard with configuration properties.

use std::sync::Arc;

use axum::{
    extract::State,
    response::{Html, IntoResponse, Redirect, Response},
};
use tower_cookies::Cookies;

use super::{get_base_styles, get_navbar_css, get_navbar_html, ADMIN_SESSION_TIMEOUT_MINUTES};
use crate::pds::db::{PdsDb, StatisticKey};
use crate::pds::server::PdsState;

/// Handle GET /admin/ - Show admin home page.
pub async fn admin_home(
    State(state): State<Arc<PdsState>>,
    cookies: Cookies,
) -> impl IntoResponse {
    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return Response::builder()
            .status(403)
            .header("Content-Type", "text/html")
            .body("Admin dashboard is disabled. Set FeatureEnabled_AdminDashboard=1 in ConfigProperty table.".to_string())
            .unwrap()
            .into_response();
    }

    // Check authentication
    if !is_authenticated(&state.db, &cookies) {
        return Redirect::to("/admin/login").into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "admin/home".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Get hostname for title
    let hostname = state
        .db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "(PdsHostname not set)".to_string());

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
<title>Admin - Home - {hostname}</title>
<style>
    {base_styles}
    {navbar_css}
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>Admin Dashboard</h1>
<p>
Welcome to the rustproto PDS Admin Dashboard.
Below is the configuration for this PDS. You can edit the config on the Config page.
</p>

<h2>Configuration</h2>
<table>
    <tr>
        <th>Key</th>
        <th>Value</th>
        <th>Description</th>
    </tr>
    {config_rows}
</table>
</div>
</body>
</html>"#,
        hostname = html_encode(&hostname),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("home"),
        config_rows = build_config_table(&state.db),
    );

    Html(html).into_response()
}

/// Check if the admin dashboard is enabled.
fn is_admin_enabled(db: &PdsDb) -> bool {
    db.get_config_property_bool("FeatureEnabled_AdminDashboard")
        .unwrap_or(false)
}

/// Check if the user is authenticated.
fn is_authenticated(db: &PdsDb, cookies: &Cookies) -> bool {
    let Some(cookie) = cookies.get("adminSessionId") else {
        return false;
    };

    let session_id = cookie.value();

    // Get session from database (we pass "unknown" as IP for now - a more complete
    // implementation would extract the real IP from headers)
    db.get_valid_admin_session(session_id, "unknown", ADMIN_SESSION_TIMEOUT_MINUTES)
        .ok()
        .flatten()
        .is_some()
        ||
    // Also check without IP restriction for development
    db.get_valid_admin_session_any_ip(session_id, ADMIN_SESSION_TIMEOUT_MINUTES)
        .ok()
        .flatten()
        .is_some()
}

/// Build the configuration table matching dnproto's Admin_Home layout.
fn build_config_table(db: &PdsDb) -> String {
    let get_value = |key: &str| -> String {
        if is_sensitive_key(key) {
            "<span class=\"dimmed\">[hidden]</span>".to_string()
        } else {
            match db.get_config_property(key) {
                Ok(v) if !v.is_empty() => html_encode(&v),
                _ => "<span class=\"dimmed\">empty</span>".to_string(),
            }
        }
    };

    let get_bool_value = |key: &str| -> String {
        match db.get_config_property_bool(key) {
            Ok(true) => "enabled".to_string(),
            Ok(false) => "<span class=\"dimmed\">disabled</span>".to_string(),
            Err(_) => "<span class=\"dimmed\">empty</span>".to_string(),
        }
    };

    let section = |name: &str| -> String {
        format!(r#"<tr class="section-header"><td colspan="3">{}</td></tr>"#, name)
    };

    let row = |key: &str, value: String, desc: &str| -> String {
        format!(
            r#"<tr>
                <td class="key-name">{}</td>
                <td>{}</td>
                <td>{}</td>
            </tr>"#,
            key, value, desc
        )
    };

    let mut rows = Vec::new();

    // Server section
    rows.push(section("Server"));
    rows.push(row("ServerListenScheme", get_value("ServerListenScheme"), "http or https?"));
    rows.push(row("ServerListenHost", get_value("ServerListenHost"), "Hostname that server listens on. Can be localhost for reverse proxy."));
    rows.push(row("ServerListenPort", get_value("ServerListenPort"), "Port that server listens on."));

    // Features section
    rows.push(section("Features"));
    rows.push(row("FeatureEnabled_AdminDashboard", get_bool_value("FeatureEnabled_AdminDashboard"), "Is the admin dashboard enabled?"));
    rows.push(row("FeatureEnabled_Oauth", get_bool_value("FeatureEnabled_Oauth"), "Is OAuth enabled? This is a global flag that turns it off/on."));
    rows.push(row("FeatureEnabled_Passkeys", get_bool_value("FeatureEnabled_Passkeys"), "Are passkeys enabled?"));
    rows.push(row("FeatureEnabled_RequestCrawl", get_bool_value("FeatureEnabled_RequestCrawl"), "If enabled, will periodically request a crawl from the crawlers."));

    // PDS section
    rows.push(section("PDS"));
    rows.push(row("PdsCrawlers", get_value("PdsCrawlers"), "Comma-separated list of relays to request crawl from. (ex: bsky.network)"));
    rows.push(row("PdsDid", get_value("PdsDid"), "DID for the PDS (ex: did:web:thisisyourpdshost.com)"));
    rows.push(row("PdsHostname", get_value("PdsHostname"), "Hostname for the PDS. What goes in your DID doc."));
    rows.push(row("PdsAvailableUserDomain", get_value("PdsAvailableUserDomain"), "A single domain that is the available user domains, prefixed with ."));

    // User section
    rows.push(section("User"));
    rows.push(row("UserHandle", get_value("UserHandle"), "Handle for the user (this is a single-user PDS)."));
    rows.push(row("UserDid", get_value("UserDid"), "DID for the user (ex: did:web:______)."));
    rows.push(row("UserEmail", get_value("UserEmail"), "User's email address."));
    rows.push(row("UserIsActive", get_bool_value("UserIsActive"), "Is the user active?"));

    // Deployment section
    rows.push(section("Deployment"));
    rows.push(row("LogRetentionDays", get_value("LogRetentionDays"), "Number of days to keep logs before deleting."));
    rows.push(row("SystemctlServiceName", get_value("SystemctlServiceName"), "systemctl service name. Gets used during deployment/restart."));
    rows.push(row("CaddyAccessLogFilePath", get_value("CaddyAccessLogFilePath"), "Access log for caddy."));

    // Security section
    rows.push(section("Security"));
    rows.push(row("AtprotoProxyAllowedDids", get_value("AtprotoProxyAllowedDids"), "Comma-separated list of DIDs allowed for Atproto-Proxy header."));
    rows.push(row("OauthAllowedRedirectUris", get_value("OauthAllowedRedirectUris"), "Comma-separated list of allowed OAuth redirect URIs."));

    rows.join("\n")
}

/// Check if a config key contains sensitive data.
fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.contains("password")
        || lower.contains("secret")
        || lower.contains("key")
        || lower.contains("jwt")
}

/// HTML encode a string to prevent XSS.
fn html_encode(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
