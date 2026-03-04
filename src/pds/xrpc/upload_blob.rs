//! com.atproto.repo.uploadBlob endpoint.
//!
//! Uploads a blob (binary data) to the PDS and returns a blob reference.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::pds::blob_db::BlobDb;
use crate::pds::db::{Blob, StatisticKey};
use crate::pds::server::PdsState;
use crate::pds::xrpc::auth_helpers::{auth_failure_response, check_user_auth_with_lxm, get_caller_info, AuthType};
use crate::repo::CidV1;

/// Blob reference response.
#[derive(Serialize)]
pub struct BlobRef {
    #[serde(rename = "$type")]
    type_field: String,
    #[serde(rename = "ref")]
    ref_field: BlobLink,
    #[serde(rename = "mimeType")]
    mime_type: String,
    size: i32,
}

/// Blob link containing the CID.
#[derive(Serialize)]
pub struct BlobLink {
    #[serde(rename = "$link")]
    link: String,
}

/// Successful response for uploadBlob.
#[derive(Serialize)]
pub struct UploadBlobResponse {
    blob: BlobRef,
}

/// Error response for uploadBlob.
#[derive(Serialize)]
pub struct UploadBlobError {
    error: String,
    message: String,
}

/// POST /xrpc/com.atproto.repo.uploadBlob - Upload a blob.
///
/// Uploads binary data to the PDS and returns a blob reference that can
/// be used in records.
///
/// # Request Headers
///
/// * `Content-Type` - MIME type of the blob
/// * `Content-Length` - Size of the blob in bytes
/// * `Authorization` - Bearer token for authentication
///
/// # Request Body
///
/// Raw binary data of the blob.
///
/// # Returns
///
/// * `200 OK` with blob reference
/// * `400 Bad Request` if the request is invalid
/// * `401 Unauthorized` if not authenticated
pub async fn upload_blob(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.repo.uploadBlob".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check authentication (supports Legacy, OAuth, and Service auth)
    let allowed_auth_types = [AuthType::Legacy, AuthType::Oauth, AuthType::Service];
    let auth_result = check_user_auth_with_lxm(
        &state,
        &headers,
        Some(&allowed_auth_types),
        "POST",
        "/xrpc/com.atproto.repo.uploadBlob",
        Some("com.atproto.repo.uploadBlob"),
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Get content type and fix if necessary
    let content_type = headers
        .get("Content-Type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "*/*".to_string());

    let blob_bytes = body.to_vec();
    let content_length = blob_bytes.len() as i32;

    // Fix content type if it's empty or generic
    let content_type = if content_type.is_empty() || content_type == "*/*" {
        fix_content_type(&blob_bytes)
    } else {
        content_type
    };

    // Validate content length
    if content_length == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(UploadBlobError {
                error: "InvalidRequest".to_string(),
                message: "File content is empty.".to_string(),
            }),
        )
            .into_response();
    }

    // Generate CID for the blob
    let cid = match CidV1::compute_cid_for_blob_bytes(&blob_bytes) {
        Ok(cid) => cid.base32,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(UploadBlobError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to compute CID: {}", e),
                }),
            )
                .into_response();
        }
    };

    // Create or update blob in database
    let blob = Blob {
        cid: cid.clone(),
        content_type: content_type.clone(),
        content_length,
    };

    let blob_db = BlobDb::new(&state.lfs, state.log);

    // Check if blob exists and update or insert accordingly
    match state.db.blob_exists(&cid) {
        Ok(true) => {
            // Update existing blob
            if let Err(e) = state.db.update_blob(&blob) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(UploadBlobError {
                        error: "InternalError".to_string(),
                        message: format!("Failed to update blob metadata: {}", e),
                    }),
                )
                    .into_response();
            }
            if let Err(e) = blob_db.update_blob_bytes(&cid, &blob_bytes) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(UploadBlobError {
                        error: "InternalError".to_string(),
                        message: format!("Failed to update blob bytes: {}", e),
                    }),
                )
                    .into_response();
            }
        }
        Ok(false) => {
            // Insert new blob
            if let Err(e) = state.db.insert_blob(&blob) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(UploadBlobError {
                        error: "InternalError".to_string(),
                        message: format!("Failed to insert blob metadata: {}", e),
                    }),
                )
                    .into_response();
            }
            if let Err(e) = blob_db.insert_blob_bytes(&cid, &blob_bytes) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(UploadBlobError {
                        error: "InternalError".to_string(),
                        message: format!("Failed to insert blob bytes: {}", e),
                    }),
                )
                    .into_response();
            }
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(UploadBlobError {
                    error: "InternalError".to_string(),
                    message: format!("Failed to check blob existence: {}", e),
                }),
            )
                .into_response();
        }
    }

    // Log the upload
    let user_did = state
        .db
        .get_config_property("UserDid")
        .unwrap_or_else(|_| "unknown".to_string());
    state.log.info(&format!(
        "Uploaded blob cid={} contentType={} contentLength={} userDid={}",
        cid, content_type, content_length, user_did
    ));

    // Return the blob reference
    let response = UploadBlobResponse {
        blob: BlobRef {
            type_field: "blob".to_string(),
            ref_field: BlobLink { link: cid },
            mime_type: content_type,
            size: content_length,
        },
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// Detect content type from file magic bytes.
fn fix_content_type(blob_bytes: &[u8]) -> String {
    let len = blob_bytes.len();

    // MP4
    if len > 12
        && blob_bytes[4] == b'f'
        && blob_bytes[5] == b't'
        && blob_bytes[6] == b'y'
        && blob_bytes[7] == b'p'
        && blob_bytes[8] == b'i'
        && blob_bytes[9] == b's'
        && blob_bytes[10] == b'o'
        && blob_bytes[11] == b'm'
    {
        return "video/mp4".to_string();
    }

    // MOV
    if len > 12
        && blob_bytes[4] == b'f'
        && blob_bytes[5] == b't'
        && blob_bytes[6] == b'y'
        && blob_bytes[7] == b'p'
        && blob_bytes[8] == b'q'
        && blob_bytes[9] == b't'
        && blob_bytes[10] == b' '
        && blob_bytes[11] == b' '
    {
        return "video/quicktime".to_string();
    }

    // AVI
    if len > 12
        && blob_bytes[0] == 0x52 // R
        && blob_bytes[1] == 0x49 // I
        && blob_bytes[2] == 0x46 // F
        && blob_bytes[3] == 0x46 // F
        && blob_bytes[8] == 0x41 // A
        && blob_bytes[9] == 0x56 // V
        && blob_bytes[10] == 0x49 // I
    {
        return "video/avi".to_string();
    }

    // JPEG
    if len > 3 && blob_bytes[0] == 0xFF && blob_bytes[1] == 0xD8 && blob_bytes[2] == 0xFF {
        return "image/jpeg".to_string();
    }

    // PNG
    if len > 8
        && blob_bytes[0] == 0x89
        && blob_bytes[1] == 0x50
        && blob_bytes[2] == 0x4E
        && blob_bytes[3] == 0x47
        && blob_bytes[4] == 0x0D
        && blob_bytes[5] == 0x0A
        && blob_bytes[6] == 0x1A
        && blob_bytes[7] == 0x0A
    {
        return "image/png".to_string();
    }

    // GIF
    if len > 6
        && blob_bytes[0] == 0x47 // G
        && blob_bytes[1] == 0x49 // I
        && blob_bytes[2] == 0x46 // F
        && blob_bytes[3] == 0x38 // 8
        && (blob_bytes[4] == 0x39 || blob_bytes[4] == 0x37) // 9 or 7
        && blob_bytes[5] == 0x61 // a
    {
        return "image/gif".to_string();
    }

    // WebP
    if len > 12
        && blob_bytes[0] == 0x52 // R
        && blob_bytes[1] == 0x49 // I
        && blob_bytes[2] == 0x46 // F
        && blob_bytes[3] == 0x46 // F
        && blob_bytes[8] == 0x57 // W
        && blob_bytes[9] == 0x45 // E
        && blob_bytes[10] == 0x42 // B
        && blob_bytes[11] == 0x50 // P
    {
        return "image/webp".to_string();
    }

    // WebM
    if len > 4
        && blob_bytes[0] == 0x1A
        && blob_bytes[1] == 0x45
        && blob_bytes[2] == 0xDF
        && blob_bytes[3] == 0xA3
    {
        return "video/webm".to_string();
    }

    // Default
    "application/octet-stream".to_string()
}
