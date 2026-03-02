//! Hello endpoint - simple test endpoint.
//!
//! Returns a simple text response to verify the server is running.

use axum::response::IntoResponse;

/// GET /hello - Simple test endpoint.
///
/// Returns "world" as plain text.
pub async fn hello() -> impl IntoResponse {
    "world"
}
