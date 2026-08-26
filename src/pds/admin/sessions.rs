//! Admin sessions page handler.
//!
//! Displays and manages all session types (Legacy, OAuth, Admin).

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::{Html, IntoResponse, Redirect, Response},
    Form,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tower_cookies::Cookies;

use super::{get_base_styles, get_caller_info, get_navbar_css, get_navbar_html, is_admin_enabled, is_authenticated};
use crate::pds::db::{AdminSession, LegacySession, OauthSession, PasskeyChallenge, StatisticKey};
use crate::pds::server::PdsState;

/// Handle GET /admin/sessions - Show sessions page.
pub async fn admin_sessions(
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
        name: "admin/sessions".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Get hostname for title
    let hostname = state
        .db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "(PdsHostname not set)".to_string());

    // Get all sessions sorted by newest first
    let mut legacy_sessions = state.db.get_all_legacy_sessions().unwrap_or_default();
    legacy_sessions.sort_by(|a, b| b.last_used_date.cmp(&a.last_used_date));

    let mut oauth_sessions = state.db.get_all_oauth_sessions().unwrap_or_default();
    oauth_sessions.sort_by(|a, b| b.last_used_date.cmp(&a.last_used_date));

    let mut admin_sessions = state.db.get_all_admin_sessions().unwrap_or_default();
    admin_sessions.sort_by(|a, b| b.last_used_date.cmp(&a.last_used_date));
    // Get all passkey challenges sorted by newest first
    let mut challenges = state.db.get_all_passkey_challenges().unwrap_or_default();
    challenges.sort_by(|a, b| b.created_date.cmp(&a.created_date));
    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Admin - Sessions - {hostname}</title>
<style>
    {base_styles}
    {navbar_css}
    .delete-btn {{ background-color: #4caf50; color: white; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 500; }}
    .delete-btn:hover {{ background-color: #388e3c; }}
    .session-count {{ color: #8899a6; font-size: 14px; margin-left: 8px; }}
    .sessions-table {{ width: 100%; border-collapse: collapse; background-color: #2f3336; border-radius: 8px; overflow: hidden; margin-bottom: 24px; }}
    .sessions-table th {{ background-color: #1d1f23; color: #8899a6; text-align: left; padding: 12px 16px; font-size: 14px; font-weight: 500; }}
    .sessions-table th.sortable {{ cursor: pointer; user-select: none; }}
    .sessions-table th.sortable:hover {{ background-color: #2a2d31; color: #e7e9ea; }}
    .sessions-table th.sortable::after {{ content: ' \2195'; opacity: 0.3; }}
    .sessions-table th.sortable.asc::after {{ content: ' \2191'; opacity: 1; }}
    .sessions-table th.sortable.desc::after {{ content: ' \2193'; opacity: 1; }}
    .sessions-table td {{ padding: 10px 16px; border-bottom: 1px solid #444; font-size: 14px; }}
    .ip-address {{ font-weight: bold; color: #1d9bf0; }}
    .scope-cell {{ max-width: 340px; }}
    .scope-cell summary {{ cursor: pointer; color: #1d9bf0; font-size: 13px; user-select: none; white-space: nowrap; }}
    .scope-cell summary:hover {{ color: #e7e9ea; }}
    .scope-list {{ margin-top: 6px; max-height: 260px; overflow-y: auto; }}
    .scope-item {{ font-family: monospace; font-size: 12px; color: #8899a6; overflow-wrap: anywhere; padding: 2px 0; border-bottom: 1px solid #3a3d41; }}
    .scope-item:last-child {{ border-bottom: none; }}
    .scope-empty {{ color: #8899a6; font-style: italic; font-size: 13px; }}
    .sessions-table tr:last-child td {{ border-bottom: none; }}
    .sessions-table tr:hover {{ background-color: #3a3d41; }}
    .challenge-text {{ font-family: monospace; font-size: 12px; }}
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>Sessions</h1>

<h2>Admin Sessions <span class="session-count">({admin_count})</span></h2>
<table class="sessions-table" id="adminSessionsTable">
    <thead>
        <tr>
            <th class="sortable" data-col="0" data-type="string">IP Address</th>
            <th class="sortable desc" data-col="1" data-type="number" style="text-align: right;">Used</th>
            <th class="sortable" data-col="2" data-type="number" style="text-align: right;">Created</th>
            <th class="sortable" data-col="3" data-type="string">User Agent</th>
            <th class="sortable" data-col="4" data-type="string">AuthType</th>
            <th>Action</th>
        </tr>
    </thead>
    <tbody>
        {admin_rows}
    </tbody>
</table>

<h2>Legacy Sessions <span class="session-count">({legacy_count})</span></h2>
<table class="sessions-table" id="legacySessionsTable">
    <thead>
        <tr>
            <th class="sortable" data-col="0" data-type="string">IP Address</th>
            <th class="sortable desc" data-col="1" data-type="number" style="text-align: right;">Used</th>
            <th class="sortable" data-col="2" data-type="number" style="text-align: right;">Created</th>
            <th class="sortable" data-col="3" data-type="string">User Agent</th>
            <th>Action</th>
        </tr>
    </thead>
    <tbody>
        {legacy_rows}
    </tbody>
</table>

<h2>OAuth Sessions <span class="session-count">({oauth_count})</span></h2>
<table class="sessions-table" id="oauthSessionsTable">
    <thead>
        <tr>
            <th class="sortable" data-col="0" data-type="string">IP Address</th>
            <th class="sortable desc" data-col="1" data-type="number" style="text-align: right;">Used</th>
            <th class="sortable" data-col="2" data-type="number" style="text-align: right;">Created</th>
            <th class="sortable" data-col="3" data-type="string">Client ID</th>
            <th class="sortable" data-col="4" data-type="string">Auth Type</th>
            <th>Scope</th>
            <th>Requested Scope</th>
            <th>Action</th>
        </tr>
    </thead>
    <tbody>
        {oauth_rows}
    </tbody>
</table>

<h2>Passkey Challenges <span class="session-count">({challenge_count})</span></h2>
<table class="sessions-table" id="passkeyChallengesTable">
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
<script>
// Table sorting for multiple tables
(function() {{
    const tables = document.querySelectorAll('.sessions-table');
    
    tables.forEach(table => {{
        const headers = table.querySelectorAll('th.sortable');
        
        headers.forEach(header => {{
            header.addEventListener('click', function() {{
                const colIndex = parseInt(this.dataset.col);
                const type = this.dataset.type;
                const isDesc = this.classList.contains('desc');
                
                // Remove sort classes from all headers in this table
                headers.forEach(h => h.classList.remove('asc', 'desc'));
                
                // Toggle sort direction (default to desc on first click)
                const newDir = isDesc ? 'asc' : 'desc';
                this.classList.add(newDir);
                
                sortTable(table, colIndex, type, newDir === 'asc');
            }});
        }});
    }});
    
    function sortTable(table, colIndex, type, ascending) {{
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        
        rows.sort((a, b) => {{
            const aCell = a.cells[colIndex];
            const bCell = b.cells[colIndex];
            
            if (!aCell || !bCell) return 0;
            
            let aVal = aCell.textContent.trim();
            let bVal = bCell.textContent.trim();
            
            if (type === 'number') {{
                aVal = parseFloat(aVal) || 0;
                bVal = parseFloat(bVal) || 0;
                return ascending ? aVal - bVal : bVal - aVal;
            }} else {{
                return ascending 
                    ? aVal.localeCompare(bVal)
                    : bVal.localeCompare(aVal);
            }}
        }});
        
        rows.forEach(row => tbody.appendChild(row));
    }}
}})();
</script>
</body>
</html>"#,
        hostname = html_encode(&hostname),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("sessions"),
        legacy_count = legacy_sessions.len(),
        legacy_rows = build_legacy_sessions_html(&legacy_sessions),
        oauth_count = oauth_sessions.len(),
        oauth_rows = build_oauth_sessions_html(&oauth_sessions),
        admin_count = admin_sessions.len(),
        admin_rows = build_admin_sessions_html(&admin_sessions),
        challenge_count = challenges.len(),
        challenge_rows = build_challenges_html(&challenges),
    );

    Html(html).into_response()
}

// ============================================================================
// DELETE HANDLERS
// ============================================================================

/// Form data for deleting a legacy session.
#[derive(Deserialize)]
pub struct DeleteLegacySessionForm {
    #[serde(rename = "refreshJwt")]
    refresh_jwt: Option<String>,
}

/// Handle POST /admin/deletelegacysession - Delete a legacy session.
pub async fn admin_delete_legacy_session(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
    Form(form): Form<DeleteLegacySessionForm>,
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
        name: "admin/deletelegacysession".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Delete the session
    if let Some(refresh_jwt) = form.refresh_jwt {
        if !refresh_jwt.is_empty() {
            if let Err(e) = state.db.delete_legacy_session_for_refresh_jwt(&refresh_jwt) {
                state.log.error(&format!("Failed to delete legacy session: {}", e));
            }
        }
    }

    Redirect::to("/admin/sessions").into_response()
}

/// Form data for deleting an OAuth session.
#[derive(Deserialize)]
pub struct DeleteOauthSessionForm {
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
}

/// Handle POST /admin/deleteoauthsession - Delete an OAuth session.
pub async fn admin_delete_oauth_session(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
    Form(form): Form<DeleteOauthSessionForm>,
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
        name: "admin/deleteoauthsession".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Delete the session
    if let Some(session_id) = form.session_id {
        if !session_id.is_empty() {
            if let Err(e) = state.db.delete_oauth_session_by_session_id(&session_id) {
                state.log.error(&format!("Failed to delete OAuth session: {}", e));
            }
        }
    }

    Redirect::to("/admin/sessions").into_response()
}

/// Form data for deleting an admin session.
#[derive(Deserialize)]
pub struct DeleteAdminSessionForm {
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
}

/// Handle POST /admin/deleteadminsession - Delete an admin session.
pub async fn admin_delete_admin_session(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
    Form(form): Form<DeleteAdminSessionForm>,
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
        name: "admin/deleteadminsession".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Delete the session
    if let Some(session_id) = form.session_id {
        if !session_id.is_empty() {
            if let Err(e) = state.db.delete_admin_session(&session_id) {
                state.log.error(&format!("Failed to delete admin session: {}", e));
            }
        }
    }

    Redirect::to("/admin/sessions").into_response()
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Calculate a human-friendly "time ago" from a created date string.
///
/// Returns a tuple of `(display, sort_minutes)` where `display` is a compact
/// string (e.g. `45m`, `3.2h`, `2.1d`) and `sort_minutes` is the elapsed time
/// in minutes for correct numeric sorting regardless of the unit shown.
fn calculate_time_ago(created_date: &str) -> (String, f64) {
    if let Ok(created) = DateTime::parse_from_rfc3339(created_date) {
        let elapsed = Utc::now().signed_duration_since(created.with_timezone(&Utc));
        let minutes = (elapsed.num_seconds() as f64 / 60.0).max(0.0);
        let display = if minutes < 60.0 {
            format!("{:.0}m", minutes)
        } else if minutes < 60.0 * 24.0 {
            format!("{:.1}h", minutes / 60.0)
        } else {
            format!("{:.1}d", minutes / (60.0 * 24.0))
        };
        (display, minutes)
    } else {
        ("N/A".to_string(), 0.0)
    }
}

/// Build HTML rows for legacy sessions.
fn build_legacy_sessions_html(sessions: &[LegacySession]) -> String {
    if sessions.is_empty() {
        return r#"<tr><td colspan="5" style="text-align: center; color: #8899a6;">No legacy sessions</td></tr>"#.to_string();
    }

    sessions
        .iter()
        .map(|s| {
            let (created_display, sort_minutes) = calculate_time_ago(&s.created_date);
            let (used_display, used_sort_minutes) = calculate_time_ago(&s.last_used_date);
            format!(
                r#"<tr>
                    <td class="ip-address">{ip}</td>
                    <td style="text-align: right;" data-sort="{used_sort_minutes}">{used}</td>
                    <td style="text-align: right;" data-sort="{sort_minutes}">{created}</td>
                    <td>{user_agent}</td>
                    <td>
                        <form method="post" action="/admin/deletelegacysession" style="display:inline;">
                            <input type="hidden" name="refreshJwt" value="{refresh_jwt}" />
                            <button type="submit" class="delete-btn">Delete</button>
                        </form>
                    </td>
                </tr>"#,
                ip = html_encode(&s.ip_address),
                used = html_encode(&used_display),
                used_sort_minutes = used_sort_minutes,
                user_agent = html_encode(&s.user_agent),
                created = html_encode(&created_display),
                sort_minutes = sort_minutes,
                refresh_jwt = html_encode(&s.refresh_jwt),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Render a scope string as a collapsible cell.
///
/// The full scope string can be very long (permission-set expansions produce
/// dozens of tokens), so by default the cell shows just a count and expands on
/// click into a readable list with one space-separated scope per line.
fn format_scope_cell(scope: &str) -> String {
    let tokens: Vec<&str> = scope.split_whitespace().collect();
    if tokens.is_empty() {
        return r#"<span class="scope-empty">(none)</span>"#.to_string();
    }

    let items = tokens
        .iter()
        .map(|token| {
            // Add soft-wrap opportunities after separators so long single tokens
            // (e.g. `space:...?a=1&b=2`) wrap at sensible points rather than
            // being broken mid-word.
            let encoded = html_encode(token)
                .replace("&amp;", "&amp;<wbr>")
                .replace('?', "?<wbr>");
            format!(r#"<div class="scope-item">{}</div>"#, encoded)
        })
        .collect::<Vec<_>>()
        .join("");

    let label = if tokens.len() == 1 {
        "1 scope".to_string()
    } else {
        format!("{} scopes", tokens.len())
    };

    format!(
        r#"<details class="scope-cell"><summary>{label}</summary><div class="scope-list">{items}</div></details>"#,
        label = label,
        items = items,
    )
}

/// Build HTML rows for OAuth sessions.
fn build_oauth_sessions_html(sessions: &[OauthSession]) -> String {
    if sessions.is_empty() {
        return r#"<tr><td colspan="8" style="text-align: center; color: #8899a6;">No OAuth sessions</td></tr>"#.to_string();
    }

    sessions
        .iter()
        .map(|s| {
            let (created_display, sort_minutes) = calculate_time_ago(&s.created_date);
            let (used_display, used_sort_minutes) = calculate_time_ago(&s.last_used_date);
            format!(
                r#"<tr>
                    <td class="ip-address">{ip}</td>
                    <td style="text-align: right;" data-sort="{used_sort_minutes}">{used}</td>
                    <td style="text-align: right;" data-sort="{sort_minutes}">{created}</td>
                    <td>{client_id}</td>
                    <td>{auth_type}</td>
                    <td>{scope}</td>
                    <td>{requested_scope}</td>
                    <td>
                        <form method="post" action="/admin/deleteoauthsession" style="display:inline;">
                            <input type="hidden" name="sessionId" value="{session_id}" />
                            <button type="submit" class="delete-btn">Delete</button>
                        </form>
                    </td>
                </tr>"#,
                ip = html_encode(&s.ip_address),
                used = html_encode(&used_display),
                used_sort_minutes = used_sort_minutes,
                created = html_encode(&created_display),
                sort_minutes = sort_minutes,
                client_id = html_encode(&s.client_id),
                auth_type = html_encode(&s.auth_type),
                scope = format_scope_cell(&s.scope),
                requested_scope = format_scope_cell(&s.requested_scope),
                session_id = html_encode(&s.session_id),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Build HTML rows for admin sessions.
fn build_admin_sessions_html(sessions: &[AdminSession]) -> String {
    if sessions.is_empty() {
        return r#"<tr><td colspan="6" style="text-align: center; color: #8899a6;">No admin sessions</td></tr>"#.to_string();
    }

    sessions
        .iter()
        .map(|s| {
            let (created_display, sort_minutes) = calculate_time_ago(&s.created_date);
            let (used_display, used_sort_minutes) = calculate_time_ago(&s.last_used_date);
            format!(
                r#"<tr>
                    <td class="ip-address">{ip}</td>
                    <td style="text-align: right;" data-sort="{used_sort_minutes}">{used}</td>
                    <td style="text-align: right;" data-sort="{sort_minutes}">{created}</td>
                    <td>{user_agent}</td>
                    <td>{auth_type}</td>
                    <td>
                        <form method="post" action="/admin/deleteadminsession" style="display:inline;">
                            <input type="hidden" name="sessionId" value="{session_id}" />
                            <button type="submit" class="delete-btn">Delete</button>
                        </form>
                    </td>
                </tr>"#,
                ip = html_encode(&s.ip_address),
                used = html_encode(&used_display),
                used_sort_minutes = used_sort_minutes,
                user_agent = html_encode(&s.user_agent),
                created = html_encode(&created_display),
                sort_minutes = sort_minutes,
                auth_type = html_encode(&s.auth_type),
                session_id = html_encode(&s.session_id),
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
