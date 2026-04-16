//! Admin actions page handler.
//!
//! Provides administrative actions like password generation, key pair generation,
//! and user repo management.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::{Html, IntoResponse, Redirect, Response},
    Form,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use p256::ecdsa::SigningKey;
use pbkdf2::pbkdf2_hmac;
use rand::Rng;
use serde::Deserialize;
use sha2::Sha256;
use tower_cookies::{Cookie, Cookies};

use super::{get_base_styles, get_caller_info, get_navbar_css, get_navbar_html, is_admin_enabled, is_authenticated};
use crate::log::{Logger, LogLevel};
use crate::pds::db::{PdsDb, StatisticKey};
use crate::pds::firehose_event_generator::FirehoseEventGenerator;
use crate::pds::installer::Installer;
use crate::pds::server::PdsState;

/// Password hasher constants (must match dnproto).
const SALT_SIZE: usize = 16; // 128 bits
const HASH_SIZE: usize = 32; // 256 bits
const ITERATIONS: u32 = 100_000; // OWASP recommendation

/// Form data for actions.
#[derive(Deserialize)]
pub struct ActionForm {
    action: Option<String>,
    confirm_text: Option<String>,
}

/// Handle GET /admin/actions - Show actions page.
pub async fn admin_actions_get(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
) -> impl IntoResponse {
    // Extract caller info first for IP-based session validation
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return Response::builder()
            .status(403)
            .header("Content-Type", "text/html")
            .body("Admin dashboard is disabled. Set FeatureEnabled_AdminDashboard=1 in ConfigProperty table.".to_string())
            .unwrap()
            .into_response();
    }

    // Check authentication with IP verification
    if !is_authenticated(&state.db, &cookies, &ip_address) {
        return Redirect::to("/admin/login").into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "admin/actions".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check for generated password cookies
    let generated_admin_password = get_and_remove_cookie(&cookies, "generated_admin_password");
    let generated_user_password = get_and_remove_cookie(&cookies, "generated_user_password");
    let install_repo_error = get_and_remove_cookie(&cookies, "install_repo_error");
    let install_repo_success = get_and_remove_cookie(&cookies, "install_repo_success");

    render_actions_page(
        &state.db,
        generated_admin_password.as_deref(),
        generated_user_password.as_deref(),
        install_repo_error.as_deref(),
        install_repo_success.as_deref(),
    ).into_response()
}

/// Handle POST /admin/actions - Process an action.
pub async fn admin_actions_post(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
    Form(form): Form<ActionForm>,
) -> impl IntoResponse {
    // Extract caller info first for IP-based session validation
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return Response::builder()
            .status(403)
            .header("Content-Type", "text/html")
            .body("Admin dashboard is disabled. Set FeatureEnabled_AdminDashboard=1 in ConfigProperty table.".to_string())
            .unwrap()
            .into_response();
    }

    // Check authentication with IP verification
    if !is_authenticated(&state.db, &cookies, &ip_address) {
        return Redirect::to("/admin/login").into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "admin/actions".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    let action = form.action.as_deref().unwrap_or("");

    match action {
        "generateadminpassword" => {
            // Generate a new admin password
            let new_password = create_new_admin_password();
            let hashed_password = hash_password(&new_password);
            let _ = state.db.set_config_property("AdminHashedPassword", &hashed_password);

            // Set a short-lived cookie with the cleartext password to display after redirect
            let cookie = Cookie::build(("generated_admin_password", new_password))
                .http_only(true)
                .secure(true)
                .same_site(tower_cookies::cookie::SameSite::Strict)
                .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                .path("/")
                .build();
            cookies.add(cookie);
        }
        "generateuserpassword" => {
            // Generate a new user password
            let new_password = create_new_admin_password();
            let hashed_password = hash_password(&new_password);
            let _ = state.db.set_config_property("UserHashedPassword", &hashed_password);

            // Set a short-lived cookie with the cleartext password to display after redirect
            let cookie = Cookie::build(("generated_user_password", new_password))
                .http_only(true)
                .secure(true)
                .same_site(tower_cookies::cookie::SameSite::Strict)
                .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                .path("/")
                .build();
            cookies.add(cookie);
        }
        "generatekeypair" => {
            // Generate a new P-256 key pair
            let key_pair = generate_p256_key_pair();
            let _ = state.db.set_config_property("UserPublicKeyMultibase", &key_pair.public_key_multibase);
            let _ = state.db.set_config_property("UserPrivateKeyMultibase", &key_pair.private_key_multibase);
        }
        "activateaccount" => {
            // Activate the account
            let _ = state.db.set_config_property_bool("UserIsActive", true);

            // Generate firehose events
            let generator = FirehoseEventGenerator::new(&state.db);
            if let Err(e) = generator.generate_activation_events(true) {
                let cookie = Cookie::build(("install_repo_error", format!("Account activated but firehose event failed: {}", e)))
                    .http_only(true)
                    .secure(true)
                    .same_site(tower_cookies::cookie::SameSite::Strict)
                    .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                    .path("/")
                    .build();
                cookies.add(cookie);
            }
        }
        "deactivateaccount" => {
            // Deactivate the account
            let _ = state.db.set_config_property_bool("UserIsActive", false);

            // Generate firehose events
            let generator = FirehoseEventGenerator::new(&state.db);
            if let Err(e) = generator.generate_deactivation_events() {
                let cookie = Cookie::build(("install_repo_error", format!("Account deactivated but firehose event failed: {}", e)))
                    .http_only(true)
                    .secure(true)
                    .same_site(tower_cookies::cookie::SameSite::Strict)
                    .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                    .path("/")
                    .build();
                cookies.add(cookie);
            }
        }
        "installuserrepo" => {
            // First validate the confirmation text
            let confirm_text = form.confirm_text.as_deref().unwrap_or("");
            if !confirm_text.eq_ignore_ascii_case("this will delete my repo") {
                let cookie = Cookie::build(("install_repo_error", "You must type 'this will delete my repo' to confirm this action.".to_string()))
                    .http_only(true)
                    .secure(true)
                    .same_site(tower_cookies::cookie::SameSite::Strict)
                    .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                    .path("/")
                    .build();
                cookies.add(cookie);
            } else {
                // Check if keys are configured
                let has_private_key = state.db.config_property_exists("UserPrivateKeyMultibase").unwrap_or(false);
                let has_public_key = state.db.config_property_exists("UserPublicKeyMultibase").unwrap_or(false);

                if !has_private_key || !has_public_key {
                    let cookie = Cookie::build(("install_repo_error", "User key pair not configured. Please generate a key pair first.".to_string()))
                        .http_only(true)
                        .secure(true)
                        .same_site(tower_cookies::cookie::SameSite::Strict)
                        .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                        .path("/")
                        .build();
                    cookies.add(cookie);
                } else {
                    // Get the keys from config
                    let private_key = match state.db.get_config_property("UserPrivateKeyMultibase") {
                        Ok(k) => k,
                        Err(_) => {
                            let cookie = Cookie::build(("install_repo_error", "Failed to read private key.".to_string()))
                                .http_only(true)
                                .secure(true)
                                .same_site(tower_cookies::cookie::SameSite::Strict)
                                .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                                .path("/")
                                .build();
                            cookies.add(cookie);
                            return Redirect::to("/admin/actions").into_response();
                        }
                    };
                    let public_key = match state.db.get_config_property("UserPublicKeyMultibase") {
                        Ok(k) => k,
                        Err(_) => {
                            let cookie = Cookie::build(("install_repo_error", "Failed to read public key.".to_string()))
                                .http_only(true)
                                .secure(true)
                                .same_site(tower_cookies::cookie::SameSite::Strict)
                                .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                                .path("/")
                                .build();
                            cookies.add(cookie);
                            return Redirect::to("/admin/actions").into_response();
                        }
                    };

                    // Create a simple logger for installer
                    let log = Logger::new(LogLevel::Info);

                    // Call install_repo
                    match Installer::install_repo(&state.lfs, &log, &private_key, &public_key) {
                        Ok(()) => {
                            let cookie = Cookie::build(("install_repo_success", "User repo installed successfully.".to_string()))
                                .http_only(true)
                                .secure(true)
                                .same_site(tower_cookies::cookie::SameSite::Strict)
                                .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                                .path("/")
                                .build();
                            cookies.add(cookie);
                        }
                        Err(e) => {
                            let cookie = Cookie::build(("install_repo_error", format!("Failed to install repo: {}", e)))
                                .http_only(true)
                                .secure(true)
                                .same_site(tower_cookies::cookie::SameSite::Strict)
                                .max_age(tower_cookies::cookie::time::Duration::minutes(1))
                                .path("/")
                                .build();
                            cookies.add(cookie);
                        }
                    }
                }
            }
        }
        _ => {}
    }

    // POST-Redirect-GET pattern
    Redirect::to("/admin/actions").into_response()
}

// ============================================================================
// RENDERING
// ============================================================================

/// Render the actions page HTML.
fn render_actions_page(
    db: &PdsDb,
    generated_admin_password: Option<&str>,
    generated_user_password: Option<&str>,
    install_repo_error: Option<&str>,
    install_repo_success: Option<&str>,
) -> Html<String> {
    let hostname = db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "(PdsHostname not set)".to_string());

    let admin_password_status = get_password_status(db, "AdminHashedPassword");
    let user_password_status = get_password_status(db, "UserHashedPassword");
    let user_public_key_value = get_key_value_status(db, "UserPublicKeyMultibase");

    // Get repo commit exists status (note: the function returns true if repo is EMPTY)
    let repo_commit_exists = db.repo_commit_exists().unwrap_or(true);
    let repo_commit_status = if !repo_commit_exists {
        r#"<span style="color: #4caf50;">true</span>"#
    } else {
        r#"<span style="color: #f44336;">false</span>"#
    };

    // Get UserIsActive status
    let user_is_active_status = match db.config_property_exists("UserIsActive") {
        Ok(true) => {
            match db.get_config_property_bool("UserIsActive") {
                Ok(true) => r#"<span style="color: #4caf50;">true</span>"#.to_string(),
                Ok(false) => r#"<span style="color: #f44336;">false</span>"#.to_string(),
                Err(_) => r#"<span class="dimmed">not set</span>"#.to_string(),
            }
        }
        _ => r#"<span class="dimmed">not set</span>"#.to_string(),
    };

    let admin_password_display = if let Some(password) = generated_admin_password {
        format!(r#"
        <div class="password-display">
            <div class="label">New Password Generated - Copy Now!</div>
            <div class="value">{}</div>
            <div class="password-warning">This password will not be shown again. Copy it now and store it securely.</div>
        </div>
        "#, html_encode(password))
    } else {
        String::new()
    };

    let user_password_display = if let Some(password) = generated_user_password {
        format!(r#"
        <div class="password-display">
            <div class="label">New Password Generated - Copy Now!</div>
            <div class="value">{}</div>
            <div class="password-warning">This password will not be shown again. Copy it now and store it securely.</div>
        </div>
        "#, html_encode(password))
    } else {
        String::new()
    };

    let install_repo_error_display = if let Some(error) = install_repo_error {
        format!(r#"
        <div class="error-display">
            <div class="label">Error</div>
            <div class="value">{}</div>
        </div>
        "#, html_encode(error))
    } else {
        String::new()
    };

    let install_repo_success_display = if let Some(success) = install_repo_success {
        format!(r#"
        <div class="success-display">
            <div class="label">Success</div>
            <div class="value">{}</div>
        </div>
        "#, html_encode(success))
    } else {
        String::new()
    };

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
<title>Admin - Actions - {hostname}</title>
<style>
    {base_styles}
    {navbar_css}
    .info-card {{ background-color: #2f3336; border-radius: 8px; padding: 12px 16px; margin-bottom: 8px; }}
    .label {{ color: #8899a6; font-size: 14px; }}
    .value {{ color: #1d9bf0; font-size: 14px; word-break: break-all; }}
    .action-btn {{ background-color: #4caf50; color: white; border: none; padding: 6px 12px; border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: 500; font-family: inherit; }}
    .action-btn:hover {{ background-color: #388e3c; }}
    .action-btn-destructive {{ background-color: #f44336; color: white; border: none; padding: 6px 12px; border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: 500; font-family: inherit; }}
    .action-btn-destructive:hover {{ background-color: #d32f2f; }}
    .dimmed {{ color: #657786; }}
    .password-display {{ background-color: #1a2634; border: 2px solid #1d9bf0; border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
    .password-display .label {{ color: #1d9bf0; font-weight: 500; margin-bottom: 8px; }}
    .password-display .value {{ font-family: monospace; font-size: 14px; color: #e7e9ea; word-break: break-all; user-select: all; }}
    .password-warning {{ color: #f0a81d; font-size: 13px; margin-top: 8px; }}
    .error-display {{ background-color: #2a1a1a; border: 2px solid #f44336; border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
    .error-display .label {{ color: #f44336; font-weight: 500; margin-bottom: 8px; }}
    .error-display .value {{ font-size: 14px; color: #e7e9ea; }}
    .success-display {{ background-color: #1a2a1a; border: 2px solid #4caf50; border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
    .success-display .label {{ color: #4caf50; font-weight: 500; margin-bottom: 8px; }}
    .success-display .value {{ font-size: 14px; color: #e7e9ea; }}
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>Actions</h1>

<h2>Admin Password</h2>
{admin_password_display}
<div class="info-card">
    <div class="label">AdminHashedPassword</div>
    <div class="value">{admin_password_status}</div>
</div>
<form method="post" action="/admin/actions" style="margin-top: 16px;" onsubmit="return confirm('Are you sure you want to generate a new admin password? This will invalidate the existing admin password.');">
    <input type="hidden" name="action" value="generateadminpassword" />
    <button type="submit" class="action-btn-destructive">Generate Admin Password</button>
</form>

<h2>User Password</h2>
{user_password_display}
<div class="info-card">
    <div class="label">UserHashedPassword</div>
    <div class="value">{user_password_status}</div>
</div>
<form method="post" action="/admin/actions" style="margin-top: 16px;" onsubmit="return confirm('Are you sure you want to generate a new password? This will invalidate the existing password.');">
    <input type="hidden" name="action" value="generateuserpassword" />
    <button type="submit" class="action-btn-destructive">Generate User Password</button>
</form>

<h2>User Key Pair</h2>
<div class="info-card">
    <div class="label">UserPublicKeyMultibase</div>
    <div class="value">{user_public_key_value}</div>
</div>
<form method="post" action="/admin/actions" style="margin-top: 16px;" onsubmit="return confirm('Are you sure you want to generate a new key pair? This will overwrite the existing keys.');">
    <input type="hidden" name="action" value="generatekeypair" />
    <button type="submit" class="action-btn-destructive">Generate Key Pair</button>
</form>

<h2>Install User Repo</h2>
{install_repo_error_display}
{install_repo_success_display}
<div class="info-card">
    <div class="label">Install a fresh user repository using the configured key pair</div>
    <div class="value">Repo commit exists: {repo_commit_status}</div>
</div>
<form method="post" action="/admin/actions" style="margin-top: 16px;" onsubmit="return confirm('Are you sure you want to install the user repo? This will delete any existing repo data.');">
    <input type="hidden" name="action" value="installuserrepo" />
    <div style="margin-bottom: 12px;">
        <label for="confirm_text" style="color: #f0a81d; font-size: 14px;">Type &quot;this will delete my repo&quot; to confirm:</label>
        <input type="text" id="confirm_text" name="confirm_text" autocomplete="off" style="display: block; margin-top: 8px; padding: 8px 12px; border-radius: 4px; border: 1px solid #2f3336; background-color: #16181c; color: #e7e9ea; font-size: 14px; width: 200px;" />
    </div>
    <button type="submit" class="action-btn-destructive">Install User Repo</button>
</form>

<h2>Activate Account</h2>
<div class="info-card">
    <div class="label">Activate account. This sets the config property UserIsActive, and generates firehose events for #identity and #account.</div>
    <div class="value">UserIsActive current value: {user_is_active_status}</div>
</div>
<div style="margin-top: 16px; display: flex; gap: 12px;">
    <form method="post" action="/admin/actions" style="margin: 0;" onsubmit="return confirm('Are you sure you want to activate the account?');">
        <input type="hidden" name="action" value="activateaccount" />
        <button type="submit" class="action-btn-destructive">Activate Account</button>
    </form>
    <form method="post" action="/admin/actions" style="margin: 0;" onsubmit="return confirm('Are you sure you want to deactivate the account?');">
        <input type="hidden" name="action" value="deactivateaccount" />
        <button type="submit" class="action-btn-destructive">Deactivate Account</button>
    </form>
</div>

</div>
</body>
</html>"#,
        hostname = html_encode(&hostname),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("actions"),
        admin_password_display = admin_password_display,
        admin_password_status = admin_password_status,
        user_password_display = user_password_display,
        user_password_status = user_password_status,
        user_public_key_value = user_public_key_value,
        install_repo_error_display = install_repo_error_display,
        install_repo_success_display = install_repo_success_display,
        repo_commit_status = repo_commit_status,
        user_is_active_status = user_is_active_status,
    );

    Html(html)
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Get password status for HTML display.
fn get_password_status(db: &PdsDb, key: &str) -> String {
    match db.config_property_exists(key) {
        Ok(true) => r#"<span style="color: #4caf50;">configured</span>"#.to_string(),
        _ => r#"<span class="dimmed">not configured</span>"#.to_string(),
    }
}

/// Get key value for HTML display - shows actual value or "not configured".
fn get_key_value_status(db: &PdsDb, key: &str) -> String {
    match db.get_config_property(key) {
        Ok(value) if !value.is_empty() => html_encode(&value),
        _ => r#"<span class="dimmed">not configured</span>"#.to_string(),
    }
}

/// Get and remove a cookie value.
fn get_and_remove_cookie(cookies: &Cookies, name: &str) -> Option<String> {
    if let Some(cookie) = cookies.get(name) {
        let value = cookie.value().to_string();
        cookies.remove(Cookie::build((name.to_string(), "")).path("/").build());
        Some(value)
    } else {
        None
    }
}

/// HTML encode a string to prevent XSS.
fn html_encode(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Creates a new cryptographically secure random password.
/// Returns a 64-character password with uppercase, lowercase, numbers, and special characters.
fn create_new_admin_password() -> String {
    const PASSWORD_LENGTH: usize = 64;
    const UPPERCASE_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const LOWERCASE_CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    const NUMBER_CHARS: &[u8] = b"0123456789";
    const SPECIAL_CHARS: &[u8] = b"!@#$%^&*()-_=+[]{}|;:,.<>?";

    let mut rng = rand::rng();
    let mut password = Vec::with_capacity(PASSWORD_LENGTH);

    // Ensure at least one character from each category
    password.push(UPPERCASE_CHARS[rng.random_range(0..UPPERCASE_CHARS.len())]);
    password.push(LOWERCASE_CHARS[rng.random_range(0..LOWERCASE_CHARS.len())]);
    password.push(NUMBER_CHARS[rng.random_range(0..NUMBER_CHARS.len())]);
    password.push(SPECIAL_CHARS[rng.random_range(0..SPECIAL_CHARS.len())]);

    // Combine all characters
    let all_chars: Vec<u8> = [UPPERCASE_CHARS, LOWERCASE_CHARS, NUMBER_CHARS, SPECIAL_CHARS]
        .concat();

    // Fill the rest with random characters from all categories
    for _ in 4..PASSWORD_LENGTH {
        password.push(all_chars[rng.random_range(0..all_chars.len())]);
    }

    // Shuffle the password to randomize position of guaranteed characters
    for i in (1..PASSWORD_LENGTH).rev() {
        let j = rng.random_range(0..=i);
        password.swap(i, j);
    }

    String::from_utf8(password).expect("Password should be valid UTF-8")
}

/// Hash a password using PBKDF2-SHA256 (matches dnproto's PasswordHasher).
/// Returns base64-encoded salt+hash.
fn hash_password(password: &str) -> String {
    use rand::RngCore;

    // Generate random salt
    let mut salt = [0u8; SALT_SIZE];
    rand::rng().fill_bytes(&mut salt);

    // Compute PBKDF2 hash
    let mut hash = [0u8; HASH_SIZE];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, ITERATIONS, &mut hash);

    // Combine salt + hash and encode as base64
    let mut combined = Vec::with_capacity(SALT_SIZE + HASH_SIZE);
    combined.extend_from_slice(&salt);
    combined.extend_from_slice(&hash);

    BASE64.encode(&combined)
}

// ============================================================================
// KEY PAIR GENERATION
// ============================================================================

/// P-256 multicodec prefixes for ATProto/Bluesky.
const P256_PRIVATE_KEY_PREFIX: [u8; 2] = [0x86, 0x26];
const P256_PUBLIC_KEY_PREFIX: [u8; 2] = [0x80, 0x24];

/// Result of key pair generation.
struct KeyPair {
    public_key_multibase: String,
    private_key_multibase: String,
}

/// Generate a new P-256 key pair.
/// Returns public and private keys in multibase format (base58btc with 'z' prefix).
fn generate_p256_key_pair() -> KeyPair {
    // Generate a new P-256 signing key
    let signing_key = SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
    
    // Get private key bytes (32 bytes)
    let private_key_bytes = signing_key.to_bytes();
    
    // Get compressed public key (33 bytes: 0x02/0x03 prefix + 32-byte X coordinate)
    let verifying_key = signing_key.verifying_key();
    let public_key_point = verifying_key.to_encoded_point(true); // compressed
    let public_key_bytes = public_key_point.as_bytes();
    
    // Add multicodec prefix for private key
    let mut private_key_with_prefix = Vec::with_capacity(2 + private_key_bytes.len());
    private_key_with_prefix.extend_from_slice(&P256_PRIVATE_KEY_PREFIX);
    private_key_with_prefix.extend_from_slice(&private_key_bytes);
    
    // Add multicodec prefix for public key
    let mut public_key_with_prefix = Vec::with_capacity(2 + public_key_bytes.len());
    public_key_with_prefix.extend_from_slice(&P256_PUBLIC_KEY_PREFIX);
    public_key_with_prefix.extend_from_slice(public_key_bytes);
    
    // Encode in multibase format (base58btc with 'z' prefix)
    let private_key_multibase = format!("z{}", bs58::encode(&private_key_with_prefix).into_string());
    let public_key_multibase = format!("z{}", bs58::encode(&public_key_with_prefix).into_string());
    
    KeyPair {
        public_key_multibase,
        private_key_multibase,
    }
}
