//! Root endpoint.
//!
//! Serves an index.html file from the static directory if it exists,
//! otherwise returns a default page.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Redirect, Response},
};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use super::{get_caller_info};

/// GET / - Serve index.html from the static directory, or a default page.
///
/// If the `HomePageRedirect` config property is set to a non-empty value, the
/// request is redirected there. Otherwise, looks for `{datadir}/static/index.html`
/// and returns it with the `text/html` content type. If the file does not exist,
/// returns a small default HTML page linking to the rustproto project.
pub async fn root(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    let stat_key = StatisticKey {
        name: "/".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // If a home page redirect is configured, redirect there before anything else.
    if let Ok(redirect_url) = state.db.get_config_property("HomePageRedirect") {
        if !redirect_url.trim().is_empty() {
            return Redirect::to(redirect_url.trim()).into_response();
        }
    }

    let index_path = state.lfs.get_path_static_dir().join("index.html");

    match tokio::fs::read(&index_path).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            bytes,
        )
            .into_response(),
        Err(_) => {
            let html = concat!(
                "<!DOCTYPE html><html><head><meta charset=\"utf-8\">",
                "<title>rustproto</title></head><body>",
                "<p><a href=\"https://github.com/threddyrex/rustproto\">rustproto</a> PDS implementation</p>",
                "</body></html>",
            );
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                html,
            )
                .into_response()
        }
    }
}
