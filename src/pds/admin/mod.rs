//! Admin module for PDS.
//!
//! Provides the administrative interface for managing the PDS,
//! including login, configuration viewing, and session management.

mod actions;
mod config;
mod home;
mod login;
mod passkey_auth;
mod passkeys;
mod register_passkey;
mod sessions;
mod ipstats;
mod stats;

use std::sync::Arc;

use axum::{
    Router,
    routing::get,
};
use tower_cookies::Cookies;

use super::db::PdsDb;
use super::server::PdsState;

pub use config::{admin_config_get, admin_config_post};
pub use home::admin_home;
pub use login::{admin_login_get, admin_login_post, admin_logout, get_caller_info};
pub use sessions::{
    admin_sessions, admin_delete_legacy_session, admin_delete_oauth_session,
    admin_delete_admin_session,
};
pub use stats::{
    admin_stats, admin_delete_statistic, admin_delete_all_statistics,
    admin_delete_old_statistics,
};
pub use ipstats::admin_ipstats;
pub use actions::{admin_actions_get, admin_actions_post};
pub use passkey_auth::{admin_passkey_authentication_options, admin_authenticate_passkey};
pub use passkeys::{admin_passkeys, admin_delete_passkey, admin_delete_passkey_challenge};
pub use register_passkey::{admin_register_passkey_get, admin_passkey_registration_options, admin_register_passkey_post};

// =============================================================================
// IMPORTANT: Routes are NOT registered here!
// =============================================================================
// The routes() function below is NOT USED. All admin routes are registered
// directly in src/pds/server.rs in the build_router() method.
//
// When adding a new admin page:
// 1. Create the handler in a new module file (e.g., actions.rs)
// 2. Add `mod actions;` at the top of this file
// 3. Add `pub use actions::{...};` to export the handlers
// 4. Register the routes in src/pds/server.rs (NOT here)
// 5. Update get_navbar_html() below to add navbar link if needed
// =============================================================================

/// Build admin routes.
/// 
/// NOTE: This function is currently NOT USED. Routes are registered in server.rs.
/// This exists for potential future refactoring to use nested routers.
#[allow(dead_code)]
pub fn routes() -> Router<Arc<PdsState>> {
    Router::new()
        .route("/", get(admin_home))
        .route("/login", get(admin_login_get).post(admin_login_post))
        .route("/login/", get(admin_login_get).post(admin_login_post))
        .route("/logout", axum::routing::post(admin_logout))
        .route("/sessions", get(admin_sessions))
        .route("/sessions/", get(admin_sessions))
        .route("/deletelegacysession", axum::routing::post(admin_delete_legacy_session))
        .route("/deleteoauthsession", axum::routing::post(admin_delete_oauth_session))
        .route("/deleteadminsession", axum::routing::post(admin_delete_admin_session))
        .route("/stats", get(admin_stats))
        .route("/stats/", get(admin_stats))
        .route("/ipstats", get(admin_ipstats))
        .route("/ipstats/", get(admin_ipstats))
        .route("/deletestatistic", axum::routing::post(admin_delete_statistic))
        .route("/deleteallstatistics", axum::routing::post(admin_delete_all_statistics))
        .route("/deleteoldstatistics", axum::routing::post(admin_delete_old_statistics))
        .route("/config", get(admin_config_get).post(admin_config_post))
        .route("/config/", get(admin_config_get).post(admin_config_post))
        .route("/actions", get(admin_actions_get).post(admin_actions_post))
        .route("/actions/", get(admin_actions_get).post(admin_actions_post))
}

/// CSS styles for the admin interface.
/// 
/// These match the dnproto admin dashboard styling.
pub fn get_navbar_css() -> &'static str {
    r#"
        .navbar { display: flex; align-items: center; gap: 8px; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid #2f3336; }
        .nav-btn { background-color: #4caf50; color: white; border: none; padding: 6px 12px; border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: 500; text-decoration: none; }
        .nav-btn:hover { background-color: #388e3c; }
        .nav-btn.active { background-color: #388e3c; }
        .nav-btn-destructive { background-color: #f44336; color: white; border: none; padding: 6px 12px; border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: 500; text-decoration: none; }
        .nav-btn-destructive:hover { background-color: #d32f2f; }
        .nav-btn-destructive.active { background-color: #d32f2f; }
        .nav-spacer { flex-grow: 1; }
        .logout-btn { background-color: #1d9bf0; color: white; border: none; padding: 6px 12px; border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: 500; font-family: inherit; }
        .logout-btn:hover { background-color: #1a8cd8; }
    "#
}

/// Generate the navbar HTML.
pub fn get_navbar_html(active_page: &str) -> String {
    fn active_class(page: &str, active: &str) -> &'static str {
        if page == active { " active" } else { "" }
    }
    
    format!(r#"
        <div class="navbar">
            <a href="/admin/" class="nav-btn{home}">Home</a>
            <a href="/admin/sessions" class="nav-btn{sessions}">Sessions</a>
            <a href="/admin/stats" class="nav-btn{stats}">Stats (url)</a>
            <a href="/admin/ipstats" class="nav-btn{ipstats}">Stats (ip)</a>
            <div class="nav-spacer"></div>
            <a href="/admin/config" class="nav-btn-destructive{config}">Config</a>
            <a href="/admin/actions" class="nav-btn-destructive{actions}">Actions</a>
            <a href="/admin/passkeys" class="nav-btn-destructive{passkeys}">Passkeys</a>
            <form method="post" action="/admin/logout" style="margin: 0;">
                <button type="submit" class="logout-btn">Log out</button>
            </form>
        </div>"#,
        home = active_class("home", active_page),
        sessions = active_class("sessions", active_page),
        stats = active_class("stats", active_page),
        ipstats = active_class("ipstats", active_page),
        config = active_class("config", active_page),
        actions = active_class("actions", active_page),
        passkeys = active_class("passkeys", active_page),
    )
}

/// Base styles for admin pages.
pub fn get_base_styles() -> &'static str {
    r#"
        body { background-color: #16181c; color: #e7e9ea; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 40px 20px; }
        .container { max-width: 800px; margin: 0 0 0 40px; }
        h1 { color: #8899a6; margin-bottom: 24px; }
        h2 { color: #8899a6; margin-top: 32px; margin-bottom: 16px; font-size: 18px; }
        p { margin-bottom: 16px; line-height: 1.5; }
        a { color: #1d9bf0; text-decoration: none; }
        a:hover { text-decoration: underline; }
        table { width: 100%; border-collapse: collapse; background-color: #2f3336; border-radius: 8px; overflow: hidden; margin-top: 16px; }
        th { background-color: #1d1f23; color: #8899a6; text-align: left; padding: 12px 16px; font-size: 14px; font-weight: 500; }
        td { padding: 10px 16px; border-bottom: 1px solid #444; font-size: 14px; }
        tr:last-child td { border-bottom: none; }
        tr:hover { background-color: #3a3d41; }
        .dimmed { color: #657786; }
        .key-name { font-weight: bold; color: #1d9bf0; }
        .section-header td { background-color: #1d1f23; color: #8899a6; font-weight: 500; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; }
    "#
}

/// Session timeout in minutes for admin sessions.
pub const ADMIN_SESSION_TIMEOUT_MINUTES: i32 = 60;

/// Check if the admin dashboard is enabled.
pub fn is_admin_enabled(db: &PdsDb) -> bool {
    db.get_config_property_bool("FeatureEnabled_AdminDashboard")
        .unwrap_or(false)
}

/// Check if the user is authenticated.
///
/// Validates the admin session cookie and checks that the session's IP address
/// matches the current request's IP address for security.
pub fn is_authenticated(db: &PdsDb, cookies: &Cookies, ip_address: &str) -> bool {
    let Some(cookie) = cookies.get("adminSessionId") else {
        return false;
    };

    let session_id = cookie.value();

    // Check session validity with IP address verification
    db.get_valid_admin_session(session_id, ip_address, ADMIN_SESSION_TIMEOUT_MINUTES)
        .ok()
        .flatten()
        .is_some()
}
