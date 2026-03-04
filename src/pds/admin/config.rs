//! Admin configuration page handler.
//!
//! Allows viewing and setting configuration properties in the ConfigProperty table.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::State,
    response::{Html, IntoResponse, Redirect, Response},
    Form,
};
use serde::Deserialize;
use tower_cookies::Cookies;

use super::{get_base_styles, get_navbar_css, get_navbar_html, ADMIN_SESSION_TIMEOUT_MINUTES};
use crate::pds::db::{PdsDb, StatisticKey};
use crate::pds::server::PdsState;

/// Whitelist of allowed config keys that can be set via the admin interface.
const ALLOWED_CONFIG_KEYS: &[&str] = &[
    "ServerListenScheme",
    "ServerListenHost",
    "ServerListenPort",
    "FeatureEnabled_AdminDashboard",
    "FeatureEnabled_Oauth",
    "FeatureEnabled_Passkeys",
    "PdsCrawlers",
    "PdsDid",
    "PdsHostname",
    "PdsAvailableUserDomain",
    "UserHandle",
    "UserDid",
    "UserEmail",
    "UserIsActive",
    "LogRetentionDays",
    "SystemctlServiceName",
    "CaddyAccessLogFilePath",
    "FeatureEnabled_RequestCrawl",
    "AtprotoProxyAllowedDids",
    "OauthAllowedRedirectUris",
];

/// Form data for setting a config property.
#[derive(Deserialize)]
pub struct SetConfigForm {
    key: Option<String>,
    value: Option<String>,
}

/// Handle GET /admin/config - Show configuration page.
pub async fn admin_config_get(
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
        name: "admin/config".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    render_config_page(&state.db).into_response()
}

/// Handle POST /admin/config - Set a configuration property.
pub async fn admin_config_post(
    State(state): State<Arc<PdsState>>,
    cookies: Cookies,
    Form(form): Form<SetConfigForm>,
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
        name: "admin/config".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Validate and set the config property
    let allowed_keys: HashSet<&str> = ALLOWED_CONFIG_KEYS.iter().copied().collect();
    
    if let (Some(key), Some(value)) = (form.key.as_deref(), form.value.as_deref()) {
        if allowed_keys.contains(key) {
            let _ = state.db.set_config_property(key, value);
        }
    }

    // POST-Redirect-GET pattern
    Redirect::to("/admin/config").into_response()
}

// ============================================================================
// RENDERING
// ============================================================================

/// Render the configuration page HTML.
fn render_config_page(db: &PdsDb) -> Html<String> {
    let hostname = db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "(PdsHostname not set)".to_string());

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
<title>Admin - Config - {hostname}</title>
<style>
    {base_styles}
    {navbar_css}
    .set-btn {{ background-color: #f44336; color: white; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 500; }}
    .set-btn:hover {{ background-color: #d32f2f; }}
    .enable-btn {{ background-color: #f44336; color: white; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 500; margin-bottom: 8px; display: block; }}
    .enable-btn:hover {{ background-color: #d32f2f; }}
    .disable-btn {{ background-color: #f44336; color: white; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 500; }}
    .disable-btn:hover {{ background-color: #d32f2f; }}
    .config-table {{ width: 100%; border-collapse: collapse; background-color: #2f3336; border-radius: 8px; overflow: hidden; }}
    .config-table th {{ background-color: #1d1f23; color: #8899a6; text-align: left; padding: 12px 16px; font-size: 14px; font-weight: 500; }}
    .config-table td {{ padding: 10px 16px; border-bottom: 1px solid #444; font-size: 14px; }}
    .config-table tr:last-child td {{ border-bottom: none; }}
    .config-table tr:hover {{ background-color: #3a3d41; }}
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>Config</h1>

<table class="config-table">
    <tr>
        <th>Key</th>
        <th>Value</th>
        <th>Action</th>
        <th>Description</th>
    </tr>
    <tr class="section-header"><td colspan="4">Server</td></tr>
    <tr>
        <td class="key-name">ServerListenScheme</td>
        <td>{server_listen_scheme}</td>
        <td><button class="set-btn" onclick="setConfig('ServerListenScheme', '{server_listen_scheme_js}')">Set</button></td>
        <td>http or https?</td>
    </tr>
    <tr>
        <td class="key-name">ServerListenHost</td>
        <td>{server_listen_host}</td>
        <td><button class="set-btn" onclick="setConfig('ServerListenHost', '{server_listen_host_js}')">Set</button></td>
        <td>Hostname that server listens on. Can be localhost for reverse proxy.</td>
    </tr>
    <tr>
        <td class="key-name">ServerListenPort</td>
        <td>{server_listen_port}</td>
        <td><button class="set-btn" onclick="setConfig('ServerListenPort', '{server_listen_port_js}')">Set</button></td>
        <td>Port that server listens on.</td>
    </tr>
    <tr class="section-header"><td colspan="4">Features</td></tr>
    <tr>
        <td class="key-name">FeatureEnabled_AdminDashboard</td>
        <td>{feature_admin_dashboard}</td>
        <td><button class="enable-btn" onclick="setBoolConfig('FeatureEnabled_AdminDashboard', '1')">Enable</button><button class="disable-btn" onclick="if(confirm('WARNING: Disabling the admin dashboard will lock you out immediately. Are you sure?')) setBoolConfig('FeatureEnabled_AdminDashboard', '0')">Disable</button></td>
        <td>Is the admin dashboard enabled?</td>
    </tr>
    <tr>
        <td class="key-name">FeatureEnabled_Oauth</td>
        <td>{feature_oauth}</td>
        <td><button class="enable-btn" onclick="setBoolConfig('FeatureEnabled_Oauth', '1')">Enable</button><button class="disable-btn" onclick="setBoolConfig('FeatureEnabled_Oauth', '0')">Disable</button></td>
        <td>Is OAuth enabled? This is a global flag that turns it off/on.</td>
    </tr>
    <tr>
        <td class="key-name">FeatureEnabled_Passkeys</td>
        <td>{feature_passkeys}</td>
        <td><button class="enable-btn" onclick="setBoolConfig('FeatureEnabled_Passkeys', '1')">Enable</button><button class="disable-btn" onclick="setBoolConfig('FeatureEnabled_Passkeys', '0')">Disable</button></td>
        <td>Are passkeys enabled?</td>
    </tr>
    <tr>
        <td class="key-name">FeatureEnabled_RequestCrawl</td>
        <td>{feature_request_crawl}</td>
        <td><button class="enable-btn" onclick="setBoolConfig('FeatureEnabled_RequestCrawl', '1')">Enable</button><button class="disable-btn" onclick="setBoolConfig('FeatureEnabled_RequestCrawl', '0')">Disable</button></td>
        <td>If enabled, will periodically request a crawl from the crawlers. Enable this last - things need to be configured correctly before connecting with the larger network.</td>
    </tr>
    <tr class="section-header"><td colspan="4">PDS</td></tr>
    <tr>
        <td class="key-name">PdsCrawlers</td>
        <td>{pds_crawlers}</td>
        <td><button class="set-btn" onclick="setConfig('PdsCrawlers', '{pds_crawlers_js}')">Set</button></td>
        <td>Comma-separated list of relays to request crawl from. (ex: bsky.network)</td>
    </tr>
    <tr>
        <td class="key-name">PdsDid</td>
        <td>{pds_did}</td>
        <td><button class="set-btn" onclick="setConfig('PdsDid', '{pds_did_js}')">Set</button></td>
        <td>DID for the PDS (ex: did:web:thisisyourpdshost.com)</td>
    </tr>
    <tr>
        <td class="key-name">PdsHostname</td>
        <td>{pds_hostname}</td>
        <td><button class="set-btn" onclick="setConfig('PdsHostname', '{pds_hostname_js}')">Set</button></td>
        <td>Hostname for the PDS. What goes in your DID doc.</td>
    </tr>
    <tr>
        <td class="key-name">PdsAvailableUserDomain</td>
        <td>{pds_available_user_domain}</td>
        <td><button class="set-btn" onclick="setConfig('PdsAvailableUserDomain', '{pds_available_user_domain_js}')">Set</button></td>
        <td>A single domain that is the available user domains, prefixed with .</td>
    </tr>
    <tr class="section-header"><td colspan="4">User</td></tr>
    <tr>
        <td class="key-name">UserHandle</td>
        <td>{user_handle}</td>
        <td><button class="set-btn" onclick="setConfig('UserHandle', '{user_handle_js}')">Set</button></td>
        <td>Handle for the user (this is a single-user PDS).</td>
    </tr>
    <tr>
        <td class="key-name">UserDid</td>
        <td>{user_did}</td>
        <td><button class="set-btn" onclick="setConfig('UserDid', '{user_did_js}')">Set</button></td>
        <td>DID for the user (ex: did:web:______).</td>
    </tr>
    <tr>
        <td class="key-name">UserEmail</td>
        <td>{user_email}</td>
        <td><button class="set-btn" onclick="setConfig('UserEmail', '{user_email_js}')">Set</button></td>
        <td>User's email address.</td>
    </tr>
    <tr>
        <td class="key-name">UserIsActive</td>
        <td>{user_is_active}</td>
        <td></td>
        <td>Is the user active? You can change this on the Actions page with 'Activate' and 'Deactivate' buttons.</td>
    </tr>
    <tr class="section-header"><td colspan="4">Deployment</td></tr>
    <tr>
        <td class="key-name">LogRetentionDays</td>
        <td>{log_retention_days}</td>
        <td><button class="set-btn" onclick="setConfig('LogRetentionDays', '{log_retention_days_js}')">Set</button></td>
        <td>Number of days to keep logs before deleting.</td>
    </tr>
    <tr>
        <td class="key-name">SystemctlServiceName</td>
        <td>{systemctl_service_name}</td>
        <td><button class="set-btn" onclick="setConfig('SystemctlServiceName', '{systemctl_service_name_js}')">Set</button></td>
        <td>systemctl service name. Gets used during deployment/restart.</td>
    </tr>
    <tr>
        <td class="key-name">CaddyAccessLogFilePath</td>
        <td>{caddy_access_log_file_path}</td>
        <td><button class="set-btn" onclick="setConfig('CaddyAccessLogFilePath', '{caddy_access_log_file_path_js}')">Set</button></td>
        <td>Access log for caddy.</td>
    </tr>
    <tr class="section-header"><td colspan="4">Security</td></tr>
    <tr>
        <td class="key-name">AtprotoProxyAllowedDids</td>
        <td>{atproto_proxy_allowed_dids}</td>
        <td><button class="set-btn" onclick="setConfig('AtprotoProxyAllowedDids', '{atproto_proxy_allowed_dids_js}')">Set</button></td>
        <td>Comma-separated list of DIDs allowed for Atproto-Proxy header (SSRF protection).</td>
    </tr>
    <tr>
        <td class="key-name">OauthAllowedRedirectUris</td>
        <td>{oauth_allowed_redirect_uris}</td>
        <td><button class="set-btn" onclick="setConfig('OauthAllowedRedirectUris', '{oauth_allowed_redirect_uris_js}')">Set</button></td>
        <td>Comma-separated list of allowed OAuth redirect URIs.</td>
    </tr>
</table>
<script>
function setConfig(key, currentValue) {{
    var newValue = prompt('Enter new value for ' + key + ':', currentValue);
    if (newValue !== null) {{
        var form = document.createElement('form');
        form.method = 'POST';
        form.action = '/admin/config';
        var keyInput = document.createElement('input');
        keyInput.type = 'hidden';
        keyInput.name = 'key';
        keyInput.value = key;
        var valueInput = document.createElement('input');
        valueInput.type = 'hidden';
        valueInput.name = 'value';
        valueInput.value = newValue;
        form.appendChild(keyInput);
        form.appendChild(valueInput);
        document.body.appendChild(form);
        form.submit();
    }}
}}
function setBoolConfig(key, value) {{
    var form = document.createElement('form');
    form.method = 'POST';
    form.action = '/admin/config';
    var keyInput = document.createElement('input');
    keyInput.type = 'hidden';
    keyInput.name = 'key';
    keyInput.value = key;
    var valueInput = document.createElement('input');
    valueInput.type = 'hidden';
    valueInput.name = 'value';
    valueInput.value = value;
    form.appendChild(keyInput);
    form.appendChild(valueInput);
    document.body.appendChild(form);
    form.submit();
}}
</script>
</div>
</body>
</html>
"#,
        hostname = html_encode(&hostname),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("config"),
        // Server section
        server_listen_scheme = get_config_value(db, "ServerListenScheme"),
        server_listen_scheme_js = get_config_value_for_js(db, "ServerListenScheme"),
        server_listen_host = get_config_value(db, "ServerListenHost"),
        server_listen_host_js = get_config_value_for_js(db, "ServerListenHost"),
        server_listen_port = get_config_value(db, "ServerListenPort"),
        server_listen_port_js = get_config_value_for_js(db, "ServerListenPort"),
        // Features section
        feature_admin_dashboard = get_bool_config_value(db, "FeatureEnabled_AdminDashboard"),
        feature_oauth = get_bool_config_value(db, "FeatureEnabled_Oauth"),
        feature_passkeys = get_bool_config_value(db, "FeatureEnabled_Passkeys"),
        feature_request_crawl = get_bool_config_value(db, "FeatureEnabled_RequestCrawl"),
        // PDS section
        pds_crawlers = get_config_value(db, "PdsCrawlers"),
        pds_crawlers_js = get_config_value_for_js(db, "PdsCrawlers"),
        pds_did = get_config_value(db, "PdsDid"),
        pds_did_js = get_config_value_for_js(db, "PdsDid"),
        pds_hostname = get_config_value(db, "PdsHostname"),
        pds_hostname_js = get_config_value_for_js(db, "PdsHostname"),
        pds_available_user_domain = get_config_value(db, "PdsAvailableUserDomain"),
        pds_available_user_domain_js = get_config_value_for_js(db, "PdsAvailableUserDomain"),
        // User section
        user_handle = get_config_value(db, "UserHandle"),
        user_handle_js = get_config_value_for_js(db, "UserHandle"),
        user_did = get_config_value(db, "UserDid"),
        user_did_js = get_config_value_for_js(db, "UserDid"),
        user_email = get_config_value(db, "UserEmail"),
        user_email_js = get_config_value_for_js(db, "UserEmail"),
        user_is_active = get_bool_config_value(db, "UserIsActive"),
        // Deployment section
        log_retention_days = get_config_value(db, "LogRetentionDays"),
        log_retention_days_js = get_config_value_for_js(db, "LogRetentionDays"),
        systemctl_service_name = get_config_value(db, "SystemctlServiceName"),
        systemctl_service_name_js = get_config_value_for_js(db, "SystemctlServiceName"),
        caddy_access_log_file_path = get_config_value(db, "CaddyAccessLogFilePath"),
        caddy_access_log_file_path_js = get_config_value_for_js(db, "CaddyAccessLogFilePath"),
        // Security section
        atproto_proxy_allowed_dids = get_config_value(db, "AtprotoProxyAllowedDids"),
        atproto_proxy_allowed_dids_js = get_config_value_for_js(db, "AtprotoProxyAllowedDids"),
        oauth_allowed_redirect_uris = get_config_value(db, "OauthAllowedRedirectUris"),
        oauth_allowed_redirect_uris_js = get_config_value_for_js(db, "OauthAllowedRedirectUris"),
    );

    Html(html)
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Get configuration value for HTML display.
fn get_config_value(db: &PdsDb, key: &str) -> String {
    match db.get_config_property(key) {
        Ok(value) => html_encode(&value),
        Err(_) => r#"<span class="dimmed">empty</span>"#.to_string(),
    }
}

/// Get configuration value for JavaScript (escaped for use in JS strings).
fn get_config_value_for_js(db: &PdsDb, key: &str) -> String {
    match db.get_config_property(key) {
        Ok(value) => js_escape(&value),
        Err(_) => String::new(),
    }
}

/// Get boolean configuration value for HTML display.
fn get_bool_config_value(db: &PdsDb, key: &str) -> String {
    match db.config_property_exists(key) {
        Ok(true) => match db.get_config_property_bool(key) {
            Ok(true) => "enabled".to_string(),
            Ok(false) => r#"<span class="dimmed">disabled</span>"#.to_string(),
            Err(_) => r#"<span class="dimmed">empty</span>"#.to_string(),
        },
        _ => r#"<span class="dimmed">empty</span>"#.to_string(),
    }
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

    db.get_valid_admin_session(session_id, "unknown", ADMIN_SESSION_TIMEOUT_MINUTES)
        .ok()
        .flatten()
        .is_some()
        || db
            .get_valid_admin_session_any_ip(session_id, ADMIN_SESSION_TIMEOUT_MINUTES)
            .ok()
            .flatten()
            .is_some()
}

/// HTML encode a string to prevent XSS.
fn html_encode(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// JavaScript escape a string for use in JS string literals.
fn js_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}
