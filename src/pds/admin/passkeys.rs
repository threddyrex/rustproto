//! Admin passkeys page handler.
//!
//! Displays and manages WebAuthn passkeys.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::{Html, IntoResponse, Redirect, Response},
    Form,
};
use serde::Deserialize;
use tower_cookies::Cookies;

use super::{get_base_styles, get_caller_info, get_navbar_css, get_navbar_html, is_admin_enabled, is_authenticated};
use crate::pds::db::{Passkey, PasskeyChallenge, StatisticKey};
use crate::pds::server::PdsState;

/// Handle GET /admin/passkeys - Show passkeys page.
pub async fn admin_passkeys(
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
        name: "admin/passkeys".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Get hostname for title
    let hostname = state
        .db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "(PdsHostname not set)".to_string());

    // Get all passkeys sorted by newest first
    let mut passkeys = state.db.get_all_passkeys().unwrap_or_default();
    passkeys.sort_by(|a, b| b.created_date.cmp(&a.created_date));

    // Get all passkey challenges sorted by newest first
    let mut challenges = state.db.get_all_passkey_challenges().unwrap_or_default();
    challenges.sort_by(|a, b| b.created_date.cmp(&a.created_date));

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
<title>Admin - Passkeys - {hostname}</title>
<style>
    {base_styles}
    {navbar_css}
    .delete-btn {{ background-color: #f44336; color: white; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 500; }}
    .delete-btn:hover {{ background-color: #d32f2f; }}
    .add-btn {{ background-color: #f44336; color: white; border: none; padding: 6px 12px; border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: 500; text-decoration: none; }}
    .add-btn:hover {{ background-color: #d32f2f; text-decoration: none; }}
    .section-header {{ display: flex; align-items: center; justify-content: space-between; margin-top: 32px; margin-bottom: 16px; }}
    .section-header h2 {{ margin: 0; }}
    .count {{ color: #8899a6; font-size: 14px; margin-left: 8px; }}
    .passkeys-table {{ width: 100%; border-collapse: collapse; background-color: #2f3336; border-radius: 8px; overflow: hidden; margin-bottom: 24px; }}
    .passkeys-table th {{ background-color: #1d1f23; color: #8899a6; text-align: left; padding: 12px 16px; font-size: 14px; font-weight: 500; }}
    .passkeys-table td {{ padding: 10px 16px; border-bottom: 1px solid #444; font-size: 14px; }}
    .passkey-name {{ font-weight: bold; color: #1d9bf0; }}
    .passkeys-table tr:last-child td {{ border-bottom: none; }}
    .passkeys-table tr:hover {{ background-color: #3a3d41; }}
    .challenge-text {{ font-family: monospace; font-size: 12px; }}
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>Passkeys</h1>

<div class="section-header">
    <h2>Passkeys <span class="count">({passkey_count})</span></h2>
    <a href="/oauth/register-passkey" class="add-btn">Add Passkey</a>
</div>
<table class="passkeys-table">
    <thead>
        <tr>
            <th>Name</th>
            <th>Created</th>
            <th>Action</th>
        </tr>
    </thead>
    <tbody>
        {passkey_rows}
    </tbody>
</table>

<div class="section-header">
    <h2>Passkey Challenges <span class="count">({challenge_count})</span></h2>
</div>
<table class="passkeys-table">
    <thead>
        <tr>
            <th>Challenge</th>
            <th>Created</th>
            <th>Action</th>
        </tr>
    </thead>
    <tbody>
        {challenge_rows}
    </tbody>
</table>
</div>
</body>
</html>"#,
        hostname = html_encode(&hostname),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("passkeys"),
        passkey_count = passkeys.len(),
        passkey_rows = build_passkeys_html(&passkeys),
        challenge_count = challenges.len(),
        challenge_rows = build_challenges_html(&challenges),
    );

    Html(html).into_response()
}

// ============================================================================
// DELETE HANDLER
// ============================================================================

/// Form data for deleting a passkey.
#[derive(Deserialize)]
pub struct DeletePasskeyForm {
    name: Option<String>,
}

/// Handle POST /admin/deletepasskey - Delete a passkey.
pub async fn admin_delete_passkey(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
    Form(form): Form<DeletePasskeyForm>,
) -> impl IntoResponse {
    // Extract caller info first for IP-based session validation
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return Redirect::to("/admin/login").into_response();
    }

    // Check authentication with IP verification
    if !is_authenticated(&state.db, &cookies, &ip_address) {
        return Redirect::to("/admin/login").into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "admin/deletepasskey".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Delete the passkey
    if let Some(name) = form.name {
        if !name.is_empty() {
            if let Err(e) = state.db.delete_passkey_by_name(&name) {
                state.log.error(&format!("Failed to delete passkey: {}", e));
            }
        }
    }

    Redirect::to("/admin/passkeys").into_response()
}

/// Form data for deleting a passkey challenge.
#[derive(Deserialize)]
pub struct DeletePasskeyChallengeForm {
    challenge: Option<String>,
}

/// Handle POST /admin/deletepasskeychallenge - Delete a passkey challenge.
pub async fn admin_delete_passkey_challenge(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
    Form(form): Form<DeletePasskeyChallengeForm>,
) -> impl IntoResponse {
    // Extract caller info first for IP-based session validation
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return Redirect::to("/admin/login").into_response();
    }

    // Check authentication with IP verification
    if !is_authenticated(&state.db, &cookies, &ip_address) {
        return Redirect::to("/admin/login").into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "admin/deletepasskeychallenge".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Delete the challenge
    if let Some(challenge) = form.challenge {
        if !challenge.is_empty() {
            if let Err(e) = state.db.delete_passkey_challenge(&challenge) {
                state.log.error(&format!("Failed to delete passkey challenge: {}", e));
            }
        }
    }

    Redirect::to("/admin/passkeys").into_response()
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Build HTML rows for passkeys.
fn build_passkeys_html(passkeys: &[Passkey]) -> String {
    if passkeys.is_empty() {
        return r#"<tr><td colspan="3" style="text-align: center; color: #8899a6;">No passkeys registered</td></tr>"#.to_string();
    }

    passkeys
        .iter()
        .map(|p| {
            format!(
                r#"<tr>
                    <td class="passkey-name">{name}</td>
                    <td>{created}</td>
                    <td>
                        <form method="post" action="/admin/deletepasskey" style="display:inline;">
                            <input type="hidden" name="name" value="{name_value}" />
                            <button type="submit" class="delete-btn">Delete</button>
                        </form>
                    </td>
                </tr>"#,
                name = html_encode(&p.name),
                created = html_encode(&p.created_date),
                name_value = html_encode(&p.name),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Build HTML rows for passkey challenges.
fn build_challenges_html(challenges: &[PasskeyChallenge]) -> String {
    if challenges.is_empty() {
        return r#"<tr><td colspan="3" style="text-align: center; color: #8899a6;">No passkey challenges</td></tr>"#.to_string();
    }

    challenges
        .iter()
        .map(|c| {
            format!(
                r#"<tr>
                    <td class="challenge-text">{challenge}</td>
                    <td>{created}</td>
                    <td>
                        <form method="post" action="/admin/deletepasskeychallenge" style="display:inline;">
                            <input type="hidden" name="challenge" value="{challenge_value}" />
                            <button type="submit" class="delete-btn">Delete</button>
                        </form>
                    </td>
                </tr>"#,
                challenge = html_encode(&c.challenge),
                created = html_encode(&c.created_date),
                challenge_value = html_encode(&c.challenge),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// HTML encode a string to prevent XSS.
fn html_encode(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
