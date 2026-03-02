//! Admin actions page handler.
//!
//! Provides administrative actions like password generation, key pair generation,
//! and user repo management.

use std::sync::Arc;

use axum::{
    extract::State,
    response::{Html, IntoResponse, Redirect, Response},
    Form,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use p256::ecdsa::SigningKey;
use pbkdf2::pbkdf2_hmac;
use rand::rngs::OsRng;
use rand::Rng;
use serde::Deserialize;
use sha2::Sha256;
use tower_cookies::{Cookie, Cookies};

use super::{get_base_styles, get_navbar_css, get_navbar_html, ADMIN_SESSION_TIMEOUT_MINUTES};
use crate::pds::db::PdsDb;
use crate::pds::server::PdsState;

/// Password hasher constants (must match dnproto).
const SALT_SIZE: usize = 16; // 128 bits
const HASH_SIZE: usize = 32; // 256 bits
const ITERATIONS: u32 = 100_000; // OWASP recommendation

/// Form data for actions.
#[derive(Deserialize)]
pub struct ActionForm {
    action: Option<String>,
}

/// Handle GET /admin/actions - Show actions page.
pub async fn admin_actions_get(
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

    // Check for generated password cookies
    let generated_admin_password = get_and_remove_cookie(&cookies, "generated_admin_password");
    let generated_user_password = get_and_remove_cookie(&cookies, "generated_user_password");

    render_actions_page(&state.db, generated_admin_password.as_deref(), generated_user_password.as_deref()).into_response()
}

/// Handle POST /admin/actions - Process an action.
pub async fn admin_actions_post(
    State(state): State<Arc<PdsState>>,
    cookies: Cookies,
    Form(form): Form<ActionForm>,
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
        _ => {}
    }

    // POST-Redirect-GET pattern
    Redirect::to("/admin/actions").into_response()
}

// ============================================================================
// RENDERING
// ============================================================================

/// Render the actions page HTML.
fn render_actions_page(db: &PdsDb, generated_admin_password: Option<&str>, generated_user_password: Option<&str>) -> Html<String> {
    let hostname = db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "(PdsHostname not set)".to_string());

    let admin_password_status = get_password_status(db, "AdminHashedPassword");
    let user_password_status = get_password_status(db, "UserHashedPassword");
    let user_public_key_value = get_key_value_status(db, "UserPublicKeyMultibase");

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

    let mut rng = rand::thread_rng();
    let mut password = Vec::with_capacity(PASSWORD_LENGTH);

    // Ensure at least one character from each category
    password.push(UPPERCASE_CHARS[rng.gen_range(0..UPPERCASE_CHARS.len())]);
    password.push(LOWERCASE_CHARS[rng.gen_range(0..LOWERCASE_CHARS.len())]);
    password.push(NUMBER_CHARS[rng.gen_range(0..NUMBER_CHARS.len())]);
    password.push(SPECIAL_CHARS[rng.gen_range(0..SPECIAL_CHARS.len())]);

    // Combine all characters
    let all_chars: Vec<u8> = [UPPERCASE_CHARS, LOWERCASE_CHARS, NUMBER_CHARS, SPECIAL_CHARS]
        .concat();

    // Fill the rest with random characters from all categories
    for _ in 4..PASSWORD_LENGTH {
        password.push(all_chars[rng.gen_range(0..all_chars.len())]);
    }

    // Shuffle the password to randomize position of guaranteed characters
    for i in (1..PASSWORD_LENGTH).rev() {
        let j = rng.gen_range(0..=i);
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
    rand::thread_rng().fill_bytes(&mut salt);

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
    let signing_key = SigningKey::random(&mut OsRng);
    
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
