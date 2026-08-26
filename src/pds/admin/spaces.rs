//! Admin spaces page handler.
//!
//! Displays all permissioned spaces stored in the `Space` table.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::{Html, IntoResponse, Redirect, Response},
};
use tower_cookies::Cookies;

use super::{get_base_styles, get_caller_info, get_navbar_css, get_navbar_html, is_admin_enabled, is_authenticated};
use crate::pds::db::{DbSpace, StatisticKey};
use crate::pds::server::PdsState;

/// Handle GET /admin/spaces - Show spaces page.
pub async fn admin_spaces(
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
        name: "admin/spaces".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Get hostname for title
    let hostname = state
        .db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "(PdsHostname not set)".to_string());

    // Get all spaces (already sorted newest first by the query).
    let spaces = state.db.get_all_spaces().unwrap_or_default();

    let html = build_spaces_page(&hostname, &spaces);

    Html(html).into_response()
}

/// Build the spaces page HTML showing every space in one table.
fn build_spaces_page(hostname: &str, spaces: &[DbSpace]) -> String {
    let total_rows = spaces.len();
    let spaces_rows = build_spaces_rows_html(spaces);

    format!(
        r#"<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Admin - Spaces - {hostname}</title>
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
</style>
</head>
<body>
<div class="container">
{navbar}
<h1>Spaces</h1>

<div class="section-header">
    <h2>Spaces <span class="session-count">({total_rows} rows)</span></h2>
</div>
<div style="margin-bottom: 16px; display: flex; gap: 12px; align-items: center;">
    <input type="text" id="showFilterInput" placeholder="Show..." style="flex: 1; padding: 10px 14px; font-size: 14px; background-color: #2f3336; color: #e7e9ea; border: 1px solid #444; border-radius: 6px; outline: none;" onfocus="this.style.borderColor='#4caf50'" onblur="this.style.borderColor='#444'" />
    <input type="text" id="hideFilterInput" placeholder="Hide..." style="flex: 1; padding: 10px 14px; font-size: 14px; background-color: #2f3336; color: #e7e9ea; border: 1px solid #444; border-radius: 6px; outline: none;" onfocus="this.style.borderColor='#f44336'" onblur="this.style.borderColor='#444'" />
</div>
<table class="stats-table filterable-table" id="spacesTable">
    <thead>
        <tr>
            <th class="sortable" data-col="0" data-type="string">URI</th>
            <th class="sortable" data-col="1" data-type="string">Owner DID</th>
            <th class="sortable" data-col="2" data-type="string">Space Type</th>
            <th class="sortable" data-col="3" data-type="string">Skey</th>
            <th class="sortable desc" data-col="4" data-type="string">Created</th>
        </tr>
    </thead>
    <tbody>
        {spaces_rows}
    </tbody>
</table>
</div>
{sort_and_filter_script}
</body>
</html>"#,
        hostname = html_encode(hostname),
        base_styles = get_base_styles(),
        navbar_css = get_navbar_css(),
        navbar = get_navbar_html("spaces"),
        total_rows = total_rows,
        spaces_rows = spaces_rows,
        sort_and_filter_script = get_sort_and_filter_script(),
    )
}

/// Build HTML rows for the spaces table.
fn build_spaces_rows_html(spaces: &[DbSpace]) -> String {
    if spaces.is_empty() {
        return r#"<tr><td colspan="5" style="text-align: center; color: #8899a6;">No spaces</td></tr>"#.to_string();
    }

    spaces
        .iter()
        .map(|s| {
            format!(
                r#"<tr>
                    <td>{uri}</td>
                    <td>{owner_did}</td>
                    <td>{space_type}</td>
                    <td>{skey}</td>
                    <td>{created}</td>
                </tr>"#,
                uri = html_encode(&s.uri),
                owner_did = html_encode(&s.owner_did),
                space_type = html_encode(&s.space_type),
                skey = html_encode(&s.skey),
                created = html_encode(&s.created_date),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Get the JavaScript for table sorting and filtering.
fn get_sort_and_filter_script() -> &'static str {
    r#"<script>
// Table sorting for multiple tables
(function() {
    const tables = document.querySelectorAll('.stats-table');

    tables.forEach(table => {
        const headers = table.querySelectorAll('th.sortable');

        headers.forEach(header => {
            header.addEventListener('click', function() {
                const colIndex = parseInt(this.dataset.col);
                const type = this.dataset.type;
                const isDesc = this.classList.contains('desc');

                // Remove sort classes from all headers in this table
                headers.forEach(h => h.classList.remove('asc', 'desc'));

                // Toggle sort direction (default to desc on first click)
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
                aVal = aCell.dataset.sort !== undefined ? parseFloat(aCell.dataset.sort) : (parseFloat(aVal) || 0);
                bVal = bCell.dataset.sort !== undefined ? parseFloat(bCell.dataset.sort) : (parseFloat(bVal) || 0);
                if (isNaN(aVal)) aVal = 0;
                if (isNaN(bVal)) bVal = 0;
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

// Table filtering for all filterable tables
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

                // Hide filter takes precedence
                if (hideText && rowText.includes(hideText)) {
                    row.style.display = 'none';
                    return;
                }

                // Show filter: if empty, show all; otherwise must match
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

/// HTML encode a string to prevent XSS.
fn html_encode(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
