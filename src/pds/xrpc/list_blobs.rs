//! com.atproto.sync.listBlobs endpoint.
//!
//! Lists blob CIDs for a repository with pagination.

use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

/// Query parameters for listBlobs.
#[derive(Deserialize)]
pub struct ListBlobsQuery {
    /// Repository DID (optional, defaults to local user).
    #[allow(dead_code)]
    did: Option<String>,
    /// Maximum number of blobs to return (default: 100).
    limit: Option<i32>,
    /// Cursor for pagination.
    cursor: Option<String>,
}

/// Successful response for listBlobs.
#[derive(Serialize)]
pub struct ListBlobsResponse {
    /// List of blob CIDs.
    cids: Vec<String>,
    /// Cursor for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

/// Error response for listBlobs.
#[derive(Serialize)]
pub struct ListBlobsError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.sync.listBlobs - List blob CIDs.
///
/// Returns a paginated list of blob CIDs stored in the repository.
///
/// # Query Parameters
///
/// * `did` - Optional repository DID (defaults to local user)
/// * `limit` - Maximum blobs to return (default: 100)
/// * `cursor` - Pagination cursor
///
/// # Returns
///
/// * `200 OK` with list of blob CIDs
pub async fn list_blobs(
    State(state): State<Arc<PdsState>>,
    Query(query): Query<ListBlobsQuery>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.listBlobs".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Parse limit with default of 100
    let limit = query.limit.unwrap_or(100).min(1000).max(1);

    // Get blob list with pagination
    let blobs = match state.db.list_blobs_with_cursor(query.cursor.as_deref(), limit) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ListBlobsError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to list blobs: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Determine next cursor (last CID if we have results)
    let next_cursor = if !blobs.is_empty() {
        Some(blobs[blobs.len() - 1].clone())
    } else {
        None
    };

    // Return response
    let response = ListBlobsResponse {
        cids: blobs,
        cursor: next_cursor,
    };

    (StatusCode::OK, Json(response)).into_response()
}
