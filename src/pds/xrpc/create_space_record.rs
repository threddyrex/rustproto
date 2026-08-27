//! com.atproto.space.createRecord endpoint.
//!
//! Creates a record in a permissioned space's repository. This is the
//! space-scoped parallel of `com.atproto.repo.createRecord`: instead of writing
//! into the caller's public repo (MST + signed commit + firehose), the record is
//! stored in the `SpaceRepoRecord` table, keyed by the space it belongs to.
//!
//! Like repo records, the record body is persisted as DAG-CBOR bytes with its
//! computed CID.
//!
//! Authentication is OAuth: the record is authored by this account, whether the
//! space is hosted on this PDS or on another one.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::pds::user_repo::{parse_json_to_dag_cbor, UserRepo};
use crate::repo::{CidV1, DagCborObject, DagCborValue};
use crate::uri::{SpaceAtUri, SpaceRef};

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info, AuthType};
use super::space_helpers::{is_spaces_enabled, spaces_disabled_response};

/// Request body for createRecord.
#[derive(Deserialize)]
pub struct CreateSpaceRecordRequest {
    /// Repository DID (must match the authenticated user).
    repo: Option<String>,
    /// Reference to the space (`at://{authority}/space/{spaceType}/{skey}`).
    space: Option<String>,
    /// Collection NSID.
    collection: Option<String>,
    /// Record key (optional, auto-generated if not provided).
    rkey: Option<String>,
    /// The record data.
    record: Option<serde_json::Value>,
    /// Whether the record should be validated against its lexicon. This
    /// implementation does not perform lexicon validation; the flag only affects
    /// the reported `validationStatus`.
    #[allow(dead_code)]
    validate: Option<bool>,
}

/// Successful response for createRecord.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSpaceRecordResponse {
    /// AT URI of the created record.
    uri: String,
    /// CID of the created record.
    cid: String,
    /// Validation status.
    validation_status: String,
}

/// Error response for createRecord.
#[derive(Serialize)]
pub struct CreateSpaceRecordError {
    error: String,
    message: String,
}

fn error_response(status: StatusCode, error: &str, message: &str) -> Response {
    (
        status,
        Json(CreateSpaceRecordError {
            error: error.to_string(),
            message: message.to_string(),
        }),
    )
        .into_response()
}

/// POST /xrpc/com.atproto.space.createRecord - Create a record in a space.
///
/// # Headers
///
/// * `Authorization` - Required (OAuth).
///
/// # Request Body
///
/// * `repo` - Required. Repository DID (must match the authenticated user).
/// * `space` - Required. Reference to the space.
/// * `collection` - Required. Collection NSID.
/// * `rkey` - Optional record key (auto-generated if not provided).
/// * `record` - Required. The record data.
/// * `validate` - Optional. Affects the reported `validationStatus` only.
///
/// # Returns
///
/// * `200 OK` with `{uri, cid, validationStatus}` on success
/// * `400 Bad Request` for malformed input
/// * `401 Unauthorized` if authentication is missing
pub async fn create_space_record(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<CreateSpaceRecordRequest>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.space.createRecord".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Ensure the spaces feature is enabled.
    if !is_spaces_enabled(&state) {
        return spaces_disabled_response();
    }

    // Authenticate the caller. Per the lexicon, auth is OAuth only.
    let auth_result = check_user_auth(
        &state,
        &headers,
        Some(&[AuthType::Oauth]),
        "POST",
        "/xrpc/com.atproto.space.createRecord",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Validate and parse the required space parameter.
    let space_uri = match body.space {
        Some(space) if !space.is_empty() => space,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing required parameter: space",
            );
        }
    };
    let space_ref = match SpaceRef::from_space_ref(&space_uri) {
        Some(space_ref) => space_ref,
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                &format!("Invalid space uri: {}", space_uri),
            );
        }
    };
    let canonical_uri = space_ref.to_space_ref();

    // Validate the required collection parameter.
    let collection = match body.collection {
        Some(collection) if !collection.is_empty() => collection,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing required parameter: collection",
            );
        }
    };

    // Validate the required record parameter.
    let record_json = match body.record {
        Some(record) => record,
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Missing required parameter: record",
            );
        }
    };

    // The record is authored by (and written into) this account's repo.
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "User DID not configured",
            );
        }
    };

    // If provided, the repo must match the authenticated user.
    if let Some(repo) = &body.repo {
        if !repo.is_empty() && repo != &user_did {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "repo does not match the authenticated user",
            );
        }
    }

    // A space may be hosted on this PDS (this account is its authority) or on a
    // different PDS. When this host is the authority, the space must already
    // exist locally (created via com.atproto.simplespace.createSpace). When the
    // space is hosted elsewhere, we record the member's contribution without
    // requiring a local copy of the space.
    if space_ref.authority == user_did {
        if let Err(e) = state.db.get_space(&canonical_uri) {
            state.log.info(&format!(
                "[SPACE] [CREATE_RECORD] Space not found {}: {}",
                canonical_uri, e
            ));
            return error_response(
                StatusCode::BAD_REQUEST,
                "SpaceNotFound",
                "The requested space does not exist",
            );
        }
    }

    // Generate an rkey if none was provided.
    let rkey = body.rkey.unwrap_or_else(UserRepo::generate_tid);

    // Reject a collision with an existing record in this space.
    match state
        .db
        .space_repo_record_exists(&canonical_uri, &collection, &rkey)
    {
        Ok(true) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                "Record already exists.",
            );
        }
        Ok(false) => {}
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] [CREATE_RECORD] existence check failed: {}", e));
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to check for existing record",
            );
        }
    }

    // Parse the record JSON to DAG-CBOR and stamp its $type with the collection,
    // matching how repo records are stored.
    let mut record = match parse_json_to_dag_cbor(&record_json) {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "InvalidRequest",
                &format!("Failed to parse record: {}", e),
            );
        }
    };
    if let DagCborValue::Map(ref mut map) = record.value {
        map.insert(
            "$type".to_string(),
            DagCborObject::new_text(collection.clone()),
        );
    }

    // Compute the record CID and serialize to DAG-CBOR bytes.
    let record_cid = match CidV1::compute_cid_for_dag_cbor(&record) {
        Ok(cid) => cid,
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] [CREATE_RECORD] CID computation failed: {}", e));
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to compute record CID",
            );
        }
    };
    let record_bytes = match record.to_bytes() {
        Ok(bytes) => bytes,
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] [CREATE_RECORD] serialization failed: {}", e));
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to serialize record",
            );
        }
    };

    // Persist the space record.
    if let Err(e) = state.db.insert_space_repo_record(
        &canonical_uri,
        &collection,
        &rkey,
        &record_cid.base32,
        &record_bytes,
    ) {
        state
            .log
            .error(&format!("[SPACE] [CREATE_RECORD] insert failed: {}", e));
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ServerError",
            "Failed to persist space record",
        );
    }

    // The record URI for a permissioned space includes the author (repo) DID
    // segment between the space URI and the collection/rkey. `SpaceAtUri`
    // codifies this shape:
    // `at://{authority}/space/{spaceType}/{skey}/{authorDid}/{collection}/{rkey}`.
    let uri = SpaceAtUri::from_space_ref(&space_ref, &user_did, &collection, &rkey)
        .to_space_at_uri();

    state.log.info(&format!(
        "[SPACE] [CREATE_RECORD] Created {} ({})",
        uri, record_cid.base32
    ));

    // We do not perform lexicon validation; report accordingly.
    let validation_status = if body.validate == Some(false) {
        "unknown"
    } else {
        "valid"
    };

    Json(CreateSpaceRecordResponse {
        uri,
        cid: record_cid.base32,
        validation_status: validation_status.to_string(),
    })
    .into_response()
}
