//! Admin login handler.
//!
//! Handles user authentication for the admin interface.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::{Html, IntoResponse, Redirect, Response},
    Form,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use pbkdf2::pbkdf2_hmac;
use serde::Deserialize;
use sha2::Sha256;
use tower_cookies::{Cookie, Cookies};
use uuid::Uuid;

use crate::pds::db::{AdminSession, PdsDb};
use crate::pds::server::PdsState;

/// Password hasher constants (must match dnproto).
const SALT_SIZE: usize = 16; // 128 bits
const HASH_SIZE: usize = 32; // 256 bits
const ITERATIONS: u32 = 100_000; // OWASP recommendation

/// Login form data.
#[derive(Deserialize)]
pub struct LoginForm {
    username: Option<String>,
    password: Option<String>,
}

/// Handle GET /admin/login - Show login page.
pub async fn admin_login_get(
    State(state): State<Arc<PdsState>>,
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

    Html(get_login_html()).into_response()
}

/// Handle POST /admin/login - Process login.
pub async fn admin_login_post(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    cookies: Cookies,
    headers: HeaderMap,
    Form(form): Form<LoginForm>,
) -> impl IntoResponse {
    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return Redirect::to("/admin/login").into_response();
    }

    let username = form.username.unwrap_or_default();
    let password = form.password.unwrap_or_default();

    // Extract caller info from headers with socket address as fallback
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Validate credentials
    let actor_correct = username == "admin";
    let stored_hash = state.db.get_config_property("AdminHashedPassword").ok();
    
    // Debug logging
    state.log.info(&format!(
        "[AUTH] [ADMIN] Attempting login: user='{}' actor_correct={} hash_exists={}",
        username,
        actor_correct,
        stored_hash.is_some()
    ));
    
    let password_matches = verify_password(stored_hash.as_deref(), &password, &state.log);
    let auth_succeeded = actor_correct && password_matches;

    if auth_succeeded {
        // Create admin session
        let session_id = Uuid::new_v4().to_string();
        let created_date = PdsDb::get_current_datetime_for_db();
        
        let session = AdminSession {
            session_id: session_id.clone(),
            ip_address,
            user_agent,
            created_date,
            auth_type: "Legacy".to_string(),
        };

        if let Err(e) = state.db.insert_admin_session(&session) {
            state.log.error(&format!("Failed to insert admin session: {}", e));
            return Redirect::to("/admin/login").into_response();
        }

        state.log.info(&format!("[AUTH] [ADMIN] authSucceeded=true"));

        // Set session cookie
        let cookie = Cookie::build(("adminSessionId", session_id))
            .path("/")
            .http_only(true)
            .secure(true)
            .same_site(tower_cookies::cookie::SameSite::Strict)
            .max_age(tower_cookies::cookie::time::Duration::hours(1))
            .build();

        cookies.add(cookie);

        Redirect::to("/admin/").into_response()
    } else {
        state.log.info(&format!("[AUTH] [ADMIN] authSucceeded=false user={}", username));
        Redirect::to("/admin/login").into_response()
    }
}

/// Handle POST /admin/logout - Log out user.
pub async fn admin_logout(
    State(state): State<Arc<PdsState>>,
    cookies: Cookies,
) -> impl IntoResponse {
    // Remove the session cookie
    if let Some(cookie) = cookies.get("adminSessionId") {
        let session_id = cookie.value().to_string();
        
        // Delete session from database
        if let Err(e) = state.db.delete_admin_session(&session_id) {
            state.log.error(&format!("Failed to delete admin session: {}", e));
        }
    }

    // Remove cookie by setting it with max_age of 0
    let cookie = Cookie::build(("adminSessionId", ""))
        .path("/")
        .max_age(tower_cookies::cookie::time::Duration::ZERO)
        .build();
    cookies.remove(cookie);

    Redirect::to("/admin/login").into_response()
}

/// Check if admin dashboard is enabled.
fn is_admin_enabled(db: &PdsDb) -> bool {
    db.get_config_property_bool("FeatureEnabled_AdminDashboard")
        .unwrap_or(false)
}

/// Extract caller IP address and User-Agent from request headers.
///
/// Tries X-Forwarded-For header first (for reverse proxy setups like Caddy),
/// then falls back to direct connection socket address. Returns "unknown" if
/// neither is available.
pub fn get_caller_info(headers: &HeaderMap, socket_addr: Option<SocketAddr>) -> (String, String) {
    // Try X-Forwarded-For header first (set by reverse proxies like Caddy)
    let ip_address = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| socket_addr.map(|addr| addr.ip().to_string()))
        .unwrap_or_else(|| "unknown".to_string());

    // Get User-Agent header
    let user_agent = headers
        .get("User-Agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    (ip_address, user_agent)
}

/// Verify a password against a stored PBKDF2 hash.
///
/// This implementation matches dnproto's PasswordHasher.VerifyPassword.
fn verify_password(stored_hash: Option<&str>, password: &str, log: &crate::log::Logger) -> bool {
    let Some(stored_hash) = stored_hash else {
        log.info("[AUTH] [ADMIN] No stored hash found");
        return false;
    };

    if password.is_empty() {
        log.info("[AUTH] [ADMIN] Empty password provided");
        return false;
    }

    // Decode the base64 hash
    let hash_bytes = match BASE64.decode(stored_hash) {
        Ok(bytes) => bytes,
        Err(e) => {
            log.info(&format!("[AUTH] [ADMIN] Failed to decode stored hash: {}", e));
            return false;
        }
    };

    // Ensure the hash is the correct length
    if hash_bytes.len() != SALT_SIZE + HASH_SIZE {
        log.info(&format!(
            "[AUTH] [ADMIN] Hash length mismatch: expected {}, got {}",
            SALT_SIZE + HASH_SIZE,
            hash_bytes.len()
        ));
        return false;
    }

    // Extract salt and stored hash
    let salt = &hash_bytes[..SALT_SIZE];
    let stored_hash_bytes = &hash_bytes[SALT_SIZE..];

    // Compute the hash with the same parameters
    let mut computed_hash = [0u8; HASH_SIZE];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, ITERATIONS, &mut computed_hash);
    
    // Debug: log first few bytes of each hash
    log.info(&format!(
        "[AUTH] [ADMIN] Salt (first 4): {:02x}{:02x}{:02x}{:02x}, Stored hash (first 4): {:02x}{:02x}{:02x}{:02x}, Computed hash (first 4): {:02x}{:02x}{:02x}{:02x}",
        salt[0], salt[1], salt[2], salt[3],
        stored_hash_bytes[0], stored_hash_bytes[1], stored_hash_bytes[2], stored_hash_bytes[3],
        computed_hash[0], computed_hash[1], computed_hash[2], computed_hash[3]
    ));
    
    let matches = constant_time_eq(stored_hash_bytes, &computed_hash);
    log.info(&format!("[AUTH] [ADMIN] Password verification result: {}", matches));

    matches
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

/// Generate the login page HTML.
fn get_login_html() -> String {
    r#"<!DOCTYPE html>
<html>
<head>
<title>Login Required</title>
<style>
    body { background-color: #16181c; color: #e7e9ea; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; padding: 40px 20px; }
    .container { max-width: 500px; margin: 0 0 0 40px; }
    h1 { color: #8899a6; margin-bottom: 24px; }
    p { margin-bottom: 16px; line-height: 1.5; }
    label { display: block; margin-bottom: 6px; color: #8899a6; }
    input[type="text"], input[type="password"] { width: 100%; padding: 12px; margin-bottom: 16px; background-color: #2f3336; border: 1px solid #3d4144; border-radius: 6px; color: #e7e9ea; font-size: 16px; box-sizing: border-box; }
    input:focus { outline: none; border-color: #1d9bf0; }
    button { background-color: #4caf50; color: white; border: none; padding: 12px 24px; border-radius: 6px; font-size: 16px; font-weight: bold; cursor: pointer; }
    button:hover { background-color: #388e3c; }
</style>
</head>
<body>
<div class="container">
<h1>Login Required</h1>
<p>You must be logged in to access account information.</p>

<form method="post" action="/admin/login">
    <label for="username">Username</label>
    <input type="text" id="username" name="username" />
    <label for="password">Password</label>
    <input type="password" id="password" name="password" />
    <button type="submit">Log in with Password</button>
</form>
</div>
</body>
</html>"#.to_string()
}
