//! com.atproto.repo.listRecords endpoint.
//!
//! Lists records in a repository collection with pagination.

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
use crate::repo::DagCborObject;

/// Query parameters for listRecords.
#[derive(Deserialize)]
pub struct ListRecordsQuery {
    /// Repository DID or handle.
    #[allow(dead_code)]
    repo: Option<String>,
    /// Collection NSID (required).
    collection: Option<String>,
    /// Maximum number of records to return (default: 50, max: 100).
    limit: Option<i32>,
    /// Cursor for pagination.
    cursor: Option<String>,
    /// Reverse the order of results.
    reverse: Option<bool>,
}

/// A single record in the list response.
#[derive(Serialize)]
pub struct ListRecordItem {
    /// AT URI of the record.
    uri: String,
    /// CID of the record.
    cid: String,
    /// The record data.
    value: serde_json::Value,
}

/// Successful response for listRecords.
#[derive(Serialize)]
pub struct ListRecordsResponse {
    /// List of records.
    records: Vec<ListRecordItem>,
    /// Cursor for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

/// Error response for listRecords.
#[derive(Serialize)]
pub struct ListRecordsError {
    error: String,
    message: String,
}

/// GET /xrpc/com.atproto.repo.listRecords - List records in a collection.
///
/// Returns a paginated list of records in a collection.
///
/// # Query Parameters
///
/// * `repo` - Optional repository identifier (defaults to local user)
/// * `collection` - Collection NSID (required)
/// * `limit` - Maximum records to return (default: 50, max: 100)
/// * `cursor` - Pagination cursor
/// * `reverse` - Reverse the order
///
/// # Returns
///
/// * `200 OK` with list of records
/// * `400 Bad Request` if collection is missing
pub async fn list_records(
    State(state): State<Arc<PdsState>>,
    Query(query): Query<ListRecordsQuery>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.repo.listRecords".to_string(),
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
                Json(ListRecordsError {
                    error: "InvalidRequest".to_string(),
                    message: "Error: Param 'collection' is required.".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Parse limit (default 50, max 100)
    let limit = query.limit.unwrap_or(50).min(100).max(1);

    // Parse reverse flag
    let reverse = query.reverse.unwrap_or(false);

    // Get local user info
    let user_did = state.db.get_config_property("UserDid").unwrap_or_default();

    // Get records from database
    let records = match state.db.list_repo_records_by_collection(
        collection,
        limit,
        query.cursor.as_deref(),
        reverse,
    ) {
        Ok(records) => records,
        Err(e) => {
            state.log.error(&format!("Failed to list records: {}", e));
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ListRecordsError {
                    error: "InternalError".to_string(),
                    message: "Failed to list records".to_string(),
                }),
            )
                .into_response();
        }
    };

    // Convert to response format
    let mut response_records = Vec::new();

    for (rkey, record) in &records {
        // Parse the DAG-CBOR data to JSON
        let value = match DagCborObject::from_bytes(&record.dag_cbor_bytes) {
            Ok(dag_cbor) => dag_cbor.to_json_value(),
            Err(_) => continue, // Skip malformed records
        };

        let uri = format!("at://{}/{}/{}", user_did, collection, rkey);

        response_records.push(ListRecordItem {
            uri,
            cid: record.cid.clone(),
            value,
        });
    }

    // Determine cursor for next page
    let cursor = if records.len() >= limit as usize {
        records.last().map(|(rkey, _)| rkey.clone())
    } else {
        None
    };

    (
        StatusCode::OK,
        Json(ListRecordsResponse {
            records: response_records,
            cursor,
        }),
    )
        .into_response()
}
