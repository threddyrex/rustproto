//! Favicon endpoint.
//!
//! Serves the favicon.ico file from the static directory if it exists.

use std::sync::Arc;

use axum::{
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::pds::server::PdsState;

/// GET /favicon.ico - Serve the favicon if it exists.
///
/// Looks for `{datadir}/static/favicon.ico` and returns it with the
/// `image/x-icon` content type. Returns 404 if the file does not exist.
pub async fn favicon(
    State(state): State<Arc<PdsState>>,
) -> Response {
    let favicon_path = state.lfs.get_path_static_dir().join("favicon.ico");

    match tokio::fs::read(&favicon_path).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "image/x-icon")],
            bytes,
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
