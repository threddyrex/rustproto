//! com.atproto.sync.getBlob endpoint.
//!
//! Downloads a blob by CID.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::pds::blob_db::BlobDb;
use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

/// Query parameters for getBlob.
#[derive(Deserialize)]
pub struct GetBlobQuery {
    /// Repository DID (optional, defaults to local user).
    #[allow(dead_code)]
    did: Option<String>,
    /// CID of the blob to download.
    cid: Option<String>,
}

/// Error response for getBlob.
#[derive(Serialize)]
pub struct GetBlobError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.sync.getBlob - Download a blob.
///
/// Returns the raw bytes of a blob by its CID.
///
/// # Query Parameters
///
/// * `did` - Optional repository DID (defaults to local user)
/// * `cid` - CID of the blob to download (required)
///
/// # Returns
///
/// * `200 OK` with blob bytes and appropriate Content-Type
/// * `400 Bad Request` if CID is missing
/// * `404 Not Found` if blob does not exist
pub async fn get_blob(
    State(state): State<Arc<PdsState>>,
    Query(query): Query<GetBlobQuery>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.getBlob".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Validate CID parameter
    let cid = match &query.cid {
        Some(c) if !c.is_empty() => c,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetBlobError {
                    error: "InvalidRequest".to_string(),
                    message: "Missing cid".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Check if blob exists in database
    let blob_exists = match state.db.blob_exists(cid) {
        Ok(exists) => exists,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetBlobError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to check blob existence: {}", e),
                }),
            )
                .into_response();
        }
    };

    if !blob_exists {
        return (
            StatusCode::NOT_FOUND,
            Json(GetBlobError {
                error: "NotFound".to_string(),
                message: "Blob not found".to_string(),
            }),
        )
            .into_response();
    }

    // Check if blob bytes exist on disk
    let blob_db = BlobDb::new(&state.lfs, state.log);
    if !blob_db.has_blob_bytes(cid) {
        return (
            StatusCode::NOT_FOUND,
            Json(GetBlobError {
                error: "NotFound".to_string(),
                message: "Blob not found".to_string(),
            }),
        )
            .into_response();
    }

    // Get blob metadata
    let blob = match state.db.get_blob_by_cid(cid) {
        Ok(Some(b)) => b,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(GetBlobError {
                    error: "NotFound".to_string(),
                    message: "Blob not found".to_string(),
                }),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetBlobError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to get blob metadata: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Get blob bytes
    let blob_bytes = match blob_db.get_blob_bytes(cid) {
        Ok(bytes) => bytes,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetBlobError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to read blob bytes: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Build response with content type and length headers
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, blob.content_type)
        .header(header::CONTENT_LENGTH, blob.content_length.to_string())
        .body(Body::from(blob_bytes))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build response",
            )
                .into_response()
        })
}
