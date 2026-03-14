//! Admin IP statistics page handler.
//!
//! Displays statistics aggregated by IP address.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, Query, State},
    http::HeaderMap,
    response::{Html, IntoResponse, Redirect, Response},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tower_cookies::Cookies;

use super::{get_base_styles, get_caller_info, get_navbar_css, get_navbar_html, is_admin_enabled, is_authenticated};
use crate::pds::db::{Statistic, StatisticKey};
use crate::pds::server::PdsState;

/// Query parameters for the IP stats page.
#[derive(Deserialize, Default)]
pub struct IpStatsQuery {
    ip: Option<String>,
}

/// Handle GET /admin/ipstats - Show statistics aggregated by IP address.
pub async fn admin_ipstats(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    cookies: Cookies,
    Query(query): Query<IpStatsQuery>,
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
        name: "admin/ipstats".to_string(),
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

    let html = if let Some(filter_ip) = &query.ip {
        // Detail page: show all stats for a specific IP address
        let filtered: Vec<&Statistic> = statistics.iter().filter(|s| s.ip_address == *filter_ip).collect();
        build_ipstats_detail_page(&hostname, filter_ip, &filtered)
    } else {
        // Summary page: show aggregated table grouped by IP address
        build_ipstats_summary_page(&hostname, &statistics)
    };

    Html(html).into_response()
}

/// Build the IP stats summary page HTML.
fn build_ipstats_summary_page(hostname: &str, statistics: &[Statistic]) -> String {
    // Aggregate statistics by (IP address, User Agent): (total_value, most_recent_last_updated)
    let mut summary: std::collections::BTreeMap<(String, String), (i64, String)> = std::collections::BTreeMap::new();
    for s in statistics {
        let key = (s.ip_address.clone(), s.user_agent.clone());
        let entry = summary.entry(key).or_insert((0, String::new()));
        entry.0 += s.value;
        if entry.1.is_empty() || s.last_updated_date > entry.1 {
            entry.1 = s.last_updated_date.clone();
        }
    }

    // Convert to vec and sort by last_updated desc
    let mut rows: Vec<(String, String, i64, String)> = summary
        .into_iter()
        .map(|((ip, ua), (value, last_updated))| (ip, ua, value, last_updated))
        .collect();
    rows.sort_by(|a, b| b.3.cmp(&a.3));

    let ip_count = rows.len();
    let total_stats = statistics.len();
    let stats_rows = build_summary_rows_html(&rows);

    format!(
        r#"<!DOCTYPE html>
<html>
<head>
<title>Admin - IP Statistics - {hostname}</title>
<style>
    {base_styles}
    {navbar_css}
    .section-header {{ display: flex; justify-content: space-between; align-items: center; }}
    .session-count {{ color: #8899a6; font-size: 14px; margin-left: 8px; }}
    .delete-all-btn {{ background-color: #4caf50; color: white; border: none; padding: 6px 12px; border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: 500; font-family: inherit; }}
    .delete-all-btn:hover {{ background-color: #388e3c; }}
    .stats-table {{ width: 100%; border-collapse: collapse; background-color: #2f3336; border-radius: 8px; overflow: hidden; }}
    .stats-table th {{ background-color: #1d1f23; color: #8899a6; text-align: left; padding: 12px 16px; font-size: 14px; font-weight: 500; }}
    .stats-table th.sortable {{ cursor: pointer; user-select: none; }}
    .stats-table th.sortable:hover {{ background-color: #2a2d31; color: #e7e9ea; }}
    .stats-table th.sortable::after {{ content: ' \2195'; opacity: 0.3; }}
    .stats-table th.sortable.asc::after {{ content: ' \2191'; opacity: 1; }}
    .stats-table th.sortable.desc::after {{ content: ' \2193'; opacity: 1; }}
    .stats-table td {{ padding: 10px 16px; border-bottom: 1px solid #444; font-size: 14px; }}
    .stats-table tr:last-child td {{ border-bottom: none; }}
    .stats-table tr:hover {{ background-color: #3a3d41; }}
    .ip-link {{ font-weight: bold; color: #1d9bf0; text-decoration: none; }}
    .ip-link:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>IP Statistics</h1>

<div class="section-header">
    <h2>By IP Address <span class="session-count">({ip_count} addresses, {total_stats} total stats)</span></h2>
    <div style="display: flex; gap: 8px;">
        <form method="post" action="/admin/deleteallstatistics" style="display:inline;" onsubmit="return confirm('Are you sure you want to delete all statistics?');">
            <input type="hidden" name="redirectTo" value="/admin/ipstats" />
            <button type="submit" class="delete-all-btn">Delete All</button>
        </form>
        <form method="post" action="/admin/deleteoldstatistics" style="display:inline;" onsubmit="return confirm('Are you sure you want to delete statistics older than 24 hours?');">
            <input type="hidden" name="redirectTo" value="/admin/ipstats" />
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
            <th class="sortable" data-col="2" data-type="number" style="text-align: right;">Value</th>
            <th class="sortable desc" data-col="3" data-type="string">Last Updated</th>
            <th class="sortable" data-col="4" data-type="number" style="text-align: right;">Minutes Ago</th>
        </tr>
    </thead>
    <tbody>
        {stats_rows}
    </tbody>
</table>
</div>
{sort_and_filter_script}
</body>
</html>"#,
        hostname = html_encode(hostname),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("ipstats"),
        ip_count = ip_count,
        total_stats = total_stats,
        stats_rows = stats_rows,
        sort_and_filter_script = get_sort_and_filter_script(),
    )
}

/// Build the IP stats detail page HTML for a specific IP address.
fn build_ipstats_detail_page(hostname: &str, filter_ip: &str, statistics: &[&Statistic]) -> String {
    let stats_count = statistics.len();
    let stats_rows = build_detail_rows_html(statistics);

    format!(
        r#"<!DOCTYPE html>
<html>
<head>
<title>Admin - IP Statistics - {filter_ip} - {hostname}</title>
<style>
    {base_styles}
    {navbar_css}
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
    .stats-table tr:last-child td {{ border-bottom: none; }}
    .stats-table tr:hover {{ background-color: #3a3d41; }}
    .name-link {{ color: #1d9bf0; text-decoration: none; }}
    .name-link:hover {{ text-decoration: underline; }}
    .back-link {{ color: #1d9bf0; text-decoration: none; font-size: 14px; }}
    .back-link:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>IP Statistics</h1>

<a href="/admin/ipstats" class="back-link">&larr; Back to Summary</a>

<div class="section-header">
    <h2>{filter_ip} <span class="session-count">({stats_count})</span></h2>
</div>
<table class="stats-table" id="statsTable">
    <thead>
        <tr>
            <th class="sortable" data-col="0" data-type="string">Name</th>
            <th class="sortable" data-col="1" data-type="string">User Agent</th>
            <th class="sortable" data-col="2" data-type="number" style="text-align: right;">Value</th>
            <th class="sortable desc" data-col="3" data-type="string">Last Updated</th>
            <th class="sortable" data-col="4" data-type="number" style="text-align: right;">Minutes Ago</th>
        </tr>
    </thead>
    <tbody>
        {stats_rows}
    </tbody>
</table>
</div>
{sort_and_filter_script}
</body>
</html>"#,
        hostname = html_encode(hostname),
        filter_ip = html_encode(filter_ip),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("ipstats"),
        stats_count = stats_count,
        stats_rows = stats_rows,
        sort_and_filter_script = get_sort_and_filter_script(),
    )
}

/// Build HTML rows for the summary page (grouped by IP address + user agent).
fn build_summary_rows_html(rows: &[(String, String, i64, String)]) -> String {
    if rows.is_empty() {
        return r#"<tr><td colspan="5" style="text-align: center; color: #8899a6;">No statistics</td></tr>"#.to_string();
    }

    rows.iter()
        .map(|(ip, ua, value, last_updated)| {
            format!(
                r#"<tr>
                    <td><a href="/admin/ipstats?ip={ip_url}" class="ip-link">{ip}</a></td>
                    <td>{ua}</td>
                    <td style="text-align: right;">{value}</td>
                    <td>{last_updated}</td>
                    <td style="text-align: right;">{minutes_ago}</td>
                </tr>"#,
                ip_url = url_encode(ip),
                ip = html_encode(ip),
                ua = html_encode(ua),
                value = value,
                last_updated = html_encode(last_updated),
                minutes_ago = calculate_minutes_ago(last_updated),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Build HTML rows for the detail page (individual stats for an IP address).
fn build_detail_rows_html(statistics: &[&Statistic]) -> String {
    if statistics.is_empty() {
        return r#"<tr><td colspan="5" style="text-align: center; color: #8899a6;">No statistics</td></tr>"#.to_string();
    }

    statistics
        .iter()
        .map(|s| {
            format!(
                r#"<tr>
                    <td>{name}</td>
                    <td>{user_agent}</td>
                    <td style="text-align: right;">{value}</td>
                    <td>{last_updated}</td>
                    <td style="text-align: right;">{minutes_ago}</td>
                </tr>"#,
                name = html_encode(&s.name),
                user_agent = html_encode(&s.user_agent),
                value = s.value,
                last_updated = html_encode(&s.last_updated_date),
                minutes_ago = calculate_minutes_ago(&s.last_updated_date),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Calculate the minutes ago from a last updated date string.
fn calculate_minutes_ago(last_updated_date: &str) -> String {
    if let Ok(last_updated) = DateTime::parse_from_rfc3339(last_updated_date) {
        let elapsed = Utc::now().signed_duration_since(last_updated.with_timezone(&Utc));
        let minutes = elapsed.num_seconds() as f64 / 60.0;
        format!("{:.1}m", minutes.max(0.0))
    } else {
        "N/A".to_string()
    }
}

/// Get the JavaScript for table sorting and filtering.
fn get_sort_and_filter_script() -> &'static str {
    r#"<script>
// Table sorting
(function() {
    const tables = document.querySelectorAll('.stats-table');
    
    tables.forEach(table => {
        const headers = table.querySelectorAll('th.sortable');
        
        headers.forEach(header => {
            header.addEventListener('click', function() {
                const colIndex = parseInt(this.dataset.col);
                const type = this.dataset.type;
                const isDesc = this.classList.contains('desc');
                
                headers.forEach(h => h.classList.remove('asc', 'desc'));
                
                const newDir = isDesc ? 'asc' : 'desc';
                this.classList.add(newDir);
                
                sortTable(table, colIndex, type, newDir === 'asc');
            });
        });
    });
    
    function sortTable(table, colIndex, type, ascending) {
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        
        rows.sort((a, b) => {
            const aCell = a.cells[colIndex];
            const bCell = b.cells[colIndex];
            
            if (!aCell || !bCell) return 0;
            
            let aVal = aCell.textContent.trim();
            let bVal = bCell.textContent.trim();
            
            if (type === 'number') {
                aVal = parseFloat(aVal) || 0;
                bVal = parseFloat(bVal) || 0;
                return ascending ? aVal - bVal : bVal - aVal;
            } else {
                return ascending 
                    ? aVal.localeCompare(bVal)
                    : bVal.localeCompare(aVal);
            }
        });
        
        rows.forEach(row => tbody.appendChild(row));
    }
})();

// Table filtering
(function() {
    const showFilterInput = document.getElementById('showFilterInput');
    const hideFilterInput = document.getElementById('hideFilterInput');
    const tables = document.querySelectorAll('.filterable-table');
    if (!showFilterInput || !hideFilterInput || tables.length === 0) return;
    
    function applyFilters() {
        const showText = showFilterInput.value.toLowerCase();
        const hideText = hideFilterInput.value.toLowerCase();
        
        tables.forEach(table => {
            const tbody = table.querySelector('tbody');
            const rows = tbody.querySelectorAll('tr');
            
            rows.forEach(row => {
                const cells = row.querySelectorAll('td');
                let rowText = '';
                cells.forEach(cell => {
                    rowText += cell.textContent.toLowerCase() + ' ';
                });
                
                if (hideText && rowText.includes(hideText)) {
                    row.style.display = 'none';
                    return;
                }
                
                if (showText && !rowText.includes(showText)) {
                    row.style.display = 'none';
                    return;
                }
                
                row.style.display = '';
            });
        });
    }
    
    showFilterInput.addEventListener('input', applyFilters);
    hideFilterInput.addEventListener('input', applyFilters);
})();
</script>"#
}

/// URL-encode a string for use in query parameters.
fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}

/// HTML encode a string to prevent XSS.
fn html_encode(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
