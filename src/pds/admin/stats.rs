//! Admin statistics page handler.
//!
//! Displays and manages statistics collected by the PDS.

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
use crate::pds::db::{Statistic, StatisticKey};
use crate::pds::server::PdsState;

/// Handle GET /admin/stats - Show statistics page.
pub async fn admin_stats(
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
        name: "admin/stats".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Get hostname for title
    let hostname = state
        .db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "(PdsHostname not set)".to_string());

    // Get all statistics sorted by last updated (newest first)
    let mut statistics = state.db.get_all_statistics().unwrap_or_default();
    statistics.sort_by(|a, b| b.last_updated_date.cmp(&a.last_updated_date));

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
<title>Admin - Statistics - {hostname}</title>
<style>
    {base_styles}
    {navbar_css}
    .delete-btn {{ background-color: #4caf50; color: white; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 12px; font-weight: 500; }}
    .delete-btn:hover {{ background-color: #388e3c; }}
    .delete-all-btn {{ background-color: #4caf50; color: white; border: none; padding: 6px 12px; border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: 500; font-family: inherit; }}
    .delete-all-btn:hover {{ background-color: #388e3c; }}
    .section-header {{ display: flex; justify-content: space-between; align-items: center; }}
    .session-count {{ color: #8899a6; font-size: 14px; margin-left: 8px; }}
    .stats-table {{ width: 100%; border-collapse: collapse; background-color: #2f3336; border-radius: 8px; overflow: hidden; }}
    .stats-table th {{ background-color: #1d1f23; color: #8899a6; text-align: left; padding: 12px 16px; font-size: 14px; font-weight: 500; }}
    .stats-table th.sortable {{ cursor: pointer; user-select: none; }}
    .stats-table th.sortable:hover {{ background-color: #2a2d31; color: #e7e9ea; }}
    .stats-table th.sortable::after {{ content: ' \2195'; opacity: 0.3; }}
    .stats-table th.sortable.asc::after {{ content: ' \2191'; opacity: 1; }}
    .stats-table th.sortable.desc::after {{ content: ' \2193'; opacity: 1; }}
    .stats-table td {{ padding: 10px 16px; border-bottom: 1px solid #444; font-size: 14px; }}
    .ip-address {{ font-weight: bold; color: #1d9bf0; }}
    .stats-table tr:last-child td {{ border-bottom: none; }}
    .stats-table tr:hover {{ background-color: #3a3d41; }}
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>Statistics</h1>

<div class="section-header">
    <h2>Statistics <span class="session-count">({stats_count})</span></h2>
    <div style="display: flex; gap: 8px;">
        <form method="post" action="/admin/deleteallstatistics" style="display:inline;" onsubmit="return confirm('Are you sure you want to delete all statistics?');">
            <button type="submit" class="delete-all-btn">Delete All</button>
        </form>
        <form method="post" action="/admin/deleteoldstatistics" style="display:inline;" onsubmit="return confirm('Are you sure you want to delete statistics older than 24 hours?');">
            <button type="submit" class="delete-all-btn">Delete Old (&gt;24hr)</button>
        </form>
    </div>
</div>
<div style="margin-bottom: 16px; display: flex; gap: 12px;">
    <input type="text" id="showFilterInput" placeholder="Show..." style="flex: 1; padding: 10px 14px; font-size: 14px; background-color: #2f3336; color: #e7e9ea; border: 1px solid #444; border-radius: 6px; outline: none;" onfocus="this.style.borderColor='#4caf50'" onblur="this.style.borderColor='#444'" />
    <input type="text" id="hideFilterInput" placeholder="Hide..." style="flex: 1; padding: 10px 14px; font-size: 14px; background-color: #2f3336; color: #e7e9ea; border: 1px solid #444; border-radius: 6px; outline: none;" onfocus="this.style.borderColor='#f44336'" onblur="this.style.borderColor='#444'" />
</div>
<table class="stats-table filterable-table" id="statsTable">
    <thead>
        <tr>
            <th class="sortable" data-col="0" data-type="string">IP Address</th>
            <th class="sortable" data-col="1" data-type="string">User Agent</th>
            <th class="sortable" data-col="2" data-type="string">Name</th>
            <th class="sortable" data-col="3" data-type="number" style="text-align: right;">Value</th>
            <th class="sortable desc" data-col="4" data-type="string">Last Updated</th>
            <th class="sortable" data-col="5" data-type="number" style="text-align: right;">Minutes Ago</th>
            <th>Action</th>
        </tr>
    </thead>
    <tbody>
        {stats_rows}
    </tbody>
</table>
</div>
<script>
// Table sorting for multiple tables
(function() {{
    const tables = document.querySelectorAll('.stats-table');
    
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

// Table filtering for all filterable tables
(function() {{
    const showFilterInput = document.getElementById('showFilterInput');
    const hideFilterInput = document.getElementById('hideFilterInput');
    const tables = document.querySelectorAll('.filterable-table');
    if (!showFilterInput || !hideFilterInput || tables.length === 0) return;
    
    function applyFilters() {{
        const showText = showFilterInput.value.toLowerCase();
        const hideText = hideFilterInput.value.toLowerCase();
        
        tables.forEach(table => {{
            const tbody = table.querySelector('tbody');
            const rows = tbody.querySelectorAll('tr');
            
            rows.forEach(row => {{
                const cells = row.querySelectorAll('td');
                let rowText = '';
                cells.forEach(cell => {{
                    rowText += cell.textContent.toLowerCase() + ' ';
                }});
                
                // Hide filter takes precedence
                if (hideText && rowText.includes(hideText)) {{
                    row.style.display = 'none';
                    return;
                }}
                
                // Show filter: if empty, show all; otherwise must match
                if (showText && !rowText.includes(showText)) {{
                    row.style.display = 'none';
                    return;
                }}
                
                row.style.display = '';
            }});
        }});
    }}
    
    showFilterInput.addEventListener('input', applyFilters);
    hideFilterInput.addEventListener('input', applyFilters);
}})();
</script>
</body>
</html>"#,
        hostname = html_encode(&hostname),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("stats"),
        stats_count = statistics.len(),
        stats_rows = build_statistics_html(&statistics),
    );

    Html(html).into_response()
}

// ============================================================================
// DELETE HANDLERS
// ============================================================================

/// Form data for deleting a single statistic.
#[derive(Deserialize)]
pub struct DeleteStatisticForm {
    name: Option<String>,
    #[serde(rename = "ipAddress")]
    ip_address: Option<String>,
    #[serde(rename = "userAgent")]
    user_agent: Option<String>,
}

/// Handle POST /admin/deletestatistic - Delete a single statistic.
pub async fn admin_delete_statistic(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
    Form(form): Form<DeleteStatisticForm>,
) -> impl IntoResponse {
    // Extract caller info first for IP-based session validation
    let (caller_ip, caller_ua) = get_caller_info(&headers, Some(addr));

    // Check if admin dashboard is enabled
    if !is_admin_enabled(&state.db) {
        return Redirect::to("/admin/login").into_response();
    }

    // Check authentication with IP verification
    if !is_authenticated(&state.db, &cookies, &caller_ip) {
        return Redirect::to("/admin/login").into_response();
    }

    // Increment statistics
    let stat_key = StatisticKey {
        name: "admin/deletestatistic".to_string(),
        ip_address: caller_ip,
        user_agent: caller_ua,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Delete the statistic
    if let (Some(name), Some(ip_address), Some(user_agent)) =
        (form.name, form.ip_address, form.user_agent)
    {
        let key = StatisticKey {
            name,
            ip_address,
            user_agent,
        };
        if let Err(e) = state.db.delete_statistic_by_key(&key) {
            state
                .log
                .error(&format!("Failed to delete statistic: {}", e));
        }
    }

    Redirect::to("/admin/stats").into_response()
}

/// Handle POST /admin/deleteallstatistics - Delete all statistics.
pub async fn admin_delete_all_statistics(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
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

    // Increment statistics (note: this stat will also be deleted)
    let stat_key = StatisticKey {
        name: "admin/deleteallstatistics".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Delete all statistics
    if let Err(e) = state.db.delete_all_statistics() {
        state
            .log
            .error(&format!("Failed to delete all statistics: {}", e));
    }

    Redirect::to("/admin/stats").into_response()
}

/// Handle POST /admin/deleteoldstatistics - Delete old statistics (>24 hours).
pub async fn admin_delete_old_statistics(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
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
        name: "admin/deleteoldstatistics".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Delete statistics older than 24 hours
    if let Err(e) = state.db.delete_old_statistics(24) {
        state
            .log
            .error(&format!("Failed to delete old statistics: {}", e));
    }

    Redirect::to("/admin/stats").into_response()
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/// Calculate the minutes ago from a last updated date string.
fn calculate_minutes_ago(last_updated_date: &str) -> String {
    // Parse the date - format is "yyyy-MM-ddTHH:mm:ss.fffZ"
    if let Ok(last_updated) = DateTime::parse_from_rfc3339(last_updated_date) {
        let elapsed = Utc::now().signed_duration_since(last_updated.with_timezone(&Utc));
        let minutes = elapsed.num_seconds() as f64 / 60.0;
        format!("{:.1}m", minutes.max(0.0))
    } else {
        "N/A".to_string()
    }
}

/// Build HTML rows for statistics.
fn build_statistics_html(statistics: &[Statistic]) -> String {
    if statistics.is_empty() {
        return r#"<tr><td colspan="7" style="text-align: center; color: #8899a6;">No statistics</td></tr>"#.to_string();
    }

    statistics
        .iter()
        .map(|s| {
            format!(
                r#"<tr>
                    <td class="ip-address">{ip}</td>
                    <td>{user_agent}</td>
                    <td>{name}</td>
                    <td style="text-align: right;">{value}</td>
                    <td>{last_updated}</td>
                    <td style="text-align: right;">{minutes_ago}</td>
                    <td>
                        <form method="post" action="/admin/deletestatistic" style="display:inline;">
                            <input type="hidden" name="name" value="{name_encoded}" />
                            <input type="hidden" name="ipAddress" value="{ip_encoded}" />
                            <input type="hidden" name="userAgent" value="{user_agent_encoded}" />
                            <button type="submit" class="delete-btn">Delete</button>
                        </form>
                    </td>
                </tr>"#,
                ip = html_encode(&s.ip_address),
                user_agent = html_encode(&s.user_agent),
                name = html_encode(&s.name),
                value = s.value,
                last_updated = html_encode(&s.last_updated_date),
                minutes_ago = calculate_minutes_ago(&s.last_updated_date),
                name_encoded = html_encode(&s.name),
                ip_encoded = html_encode(&s.ip_address),
                user_agent_encoded = html_encode(&s.user_agent),
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
