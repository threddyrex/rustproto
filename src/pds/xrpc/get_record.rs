//! com.atproto.repo.getRecord endpoint.
//!
//! Retrieves a single record from a repository.

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
use crate::pds::xrpc::is_valid_outbound_host;
use crate::repo::DagCborObject;
use crate::ws::{ActorQueryOptions, BlueskyClient};

/// Query parameters for getRecord.
#[derive(Deserialize)]
pub struct GetRecordQuery {
    /// Repository DID or handle.
    repo: Option<String>,
    /// Collection NSID.
    collection: Option<String>,
    /// Record key.
    rkey: Option<String>,
    /// Optional CID to verify record version.
    cid: Option<String>,
}

/// Successful response for getRecord.
#[derive(Serialize)]
pub struct GetRecordResponse {
    /// AT URI of the record.
    uri: String,
    /// CID of the record.
    cid: String,
    /// The record data.
    value: serde_json::Value,
}

/// Error response for getRecord.
#[derive(Serialize)]
pub struct GetRecordError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.repo.getRecord - Get a single record.
///
/// Retrieves a single record from a repository by collection and rkey.
///
/// # Query Parameters
///
/// * `repo` - Repository identifier (DID or handle)
/// * `collection` - Collection NSID
/// * `rkey` - Record key
/// * `cid` - Optional CID to verify record version
///
/// # Returns
///
/// * `200 OK` with the record
/// * `400 Bad Request` if parameters are invalid
/// * `404 Not Found` if the record doesn't exist
pub async fn get_record(
    State(state): State<Arc<PdsState>>,
    Query(query): Query<GetRecordQuery>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.repo.getRecord".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Validate required parameters
    let collection = match &query.collection {
        Some(c) if !c.is_empty() => c,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetRecordError {
                    error: "InvalidRequest".to_string(),
                    message: "Error: Params must have 'collection' and 'rkey'.".to_string(),
                }),
            )
                .into_response();
        }
    };

    let rkey = match &query.rkey {
        Some(r) if !r.is_empty() => r,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(GetRecordError {
                    error: "InvalidRequest".to_string(),
                    message: "Error: Params must have 'collection' and 'rkey'.".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Get local user info
    let user_did = state.db.get_config_property("UserDid").unwrap_or_default();
    let user_handle = state.db.get_config_property("UserHandle").unwrap_or_default();

    // Determine the repo to query
    let repo = query.repo.as_deref().unwrap_or(&user_did);

    // Check if this is a local repo request
    let is_local = repo == user_did || repo == user_handle;

    if !is_local {
        // Proxy request to the target PDS
        state.log.info(&format!("Proxying getRecord request for repo: {}", repo));

        // Resolve the repo
        let client = BlueskyClient::new();
        let options = ActorQueryOptions {
            resolve_handle_via_bluesky: true,
            ..Default::default()
        };

        let actor_info = match client.resolve_actor_info(repo, Some(options)).await {
            Ok(info) => info,
            Err(e) => {
                state.log.error(&format!("Unable to resolve actor info for repo: {}: {}", repo, e));
                return (
                    StatusCode::NOT_FOUND,
                    Json(GetRecordError {
                        error: "NotFound".to_string(),
                        message: "Unable to resolve repository".to_string(),
                    }),
                )
                    .into_response();
            }
        };

        let pds = match actor_info.pds {
            Some(pds) if !pds.is_empty() => pds,
            _ => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(GetRecordError {
                        error: "NotFound".to_string(),
                        message: "Unable to resolve repository PDS".to_string(),
                    }),
                )
                    .into_response();
            }
        };

        // Validate PDS hostname (SSRF protection)
        if !is_valid_outbound_host(&pds) {
            state.log.error(&format!("[SECURITY] Blocked invalid or internal PDS hostname: {}", pds));
            return (
                StatusCode::BAD_REQUEST,
                Json(GetRecordError {
                    error: "InvalidRequest".to_string(),
                    message: "Invalid PDS hostname".to_string(),
                }),
            )
                .into_response();
        }

        let actor_did = actor_info.did.unwrap_or_default();

        // Make proxy request using reqwest
        let target_url = format!(
            "https://{}/xrpc/com.atproto.repo.getRecord?repo={}&collection={}&rkey={}",
            pds, actor_did, collection, rkey
        );

        state.log.info(&format!("Proxying to: {}", target_url));

        let http_client = reqwest::Client::new();
        match http_client.get(&target_url).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<serde_json::Value>().await {
                        Ok(json) => {
                            return (StatusCode::OK, Json(json)).into_response();
                        }
                        Err(e) => {
                            state.log.error(&format!("Error parsing proxy response: {}", e));
                        }
                    }
                }
            }
            Err(e) => {
                state.log.error(&format!("Error proxying getRecord request: {}", e));
            }
        }

        return (
            StatusCode::NOT_FOUND,
            Json(GetRecordError {
                error: "NotFound".to_string(),
                message: "Record not found".to_string(),
            }),
        )
            .into_response();
    }

    // Local record retrieval
    let record_exists = match state.db.record_exists(collection, rkey) {
        Ok(exists) => exists,
        Err(e) => {
            state.log.error(&format!("Database error checking record: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetRecordError {
                    error: "InternalError".to_string(),
                    message: "Database error".to_string(),
                }),
            )
                .into_response();
        }
    };

    if !record_exists {
        return (
            StatusCode::NOT_FOUND,
            Json(GetRecordError {
                error: "NotFound".to_string(),
                message: "Error: Record not found.".to_string(),
            }),
        )
            .into_response();
    }

    let repo_record = match state.db.get_repo_record(collection, rkey) {
        Ok(record) => record,
        Err(e) => {
            state.log.error(&format!("Failed to get record: {}", e));
            return (
                StatusCode::NOT_FOUND,
                Json(GetRecordError {
                    error: "NotFound".to_string(),
                    message: "Error: Record not found.".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Verify CID if provided
    if let Some(expected_cid) = &query.cid {
        if &repo_record.cid != expected_cid {
            return (
                StatusCode::NOT_FOUND,
                Json(GetRecordError {
                    error: "NotFound".to_string(),
                    message: "Record CID mismatch".to_string(),
                }),
            )
                .into_response();
        }
    }

    // Parse the DAG-CBOR data to JSON
    let value = match DagCborObject::from_bytes(&repo_record.dag_cbor_bytes) {
        Ok(dag_cbor) => dag_cbor.to_json_value(),
        Err(e) => {
            state.log.error(&format!("Failed to parse record data: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GetRecordError {
                    error: "InternalError".to_string(),
                    message: "Failed to parse record data".to_string(),
                }),
            )
                .into_response();
        }
    };

    let uri = format!("at://{}/{}/{}", user_did, collection, rkey);

    (
        StatusCode::OK,
        Json(GetRecordResponse {
            uri,
            cid: repo_record.cid,
            value,
        }),
    )
        .into_response()
}
