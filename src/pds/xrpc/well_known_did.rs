//! DID document endpoint for did:web resolution.
//!
//! GET /.well-known/did.json
//!
//! Returns the DID document for the user hosted on this PDS,
//! enabling did:web resolution. This endpoint does not require
//! authentication and sets CORS headers to allow any origin.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::pds::xrpc::auth_helpers::get_caller_info;

/// Verification method in the DID document.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VerificationMethod {
    id: String,
    r#type: String,
    controller: String,
    public_key_multibase: String,
}

/// Service entry in the DID document.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Service {
    id: String,
    r#type: String,
    service_endpoint: String,
}

/// DID document response.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DidDocument {
    #[serde(rename = "@context")]
    context: Vec<String>,
    id: String,
    also_known_as: Vec<String>,
    verification_method: Vec<VerificationMethod>,
    service: Vec<Service>,
}

/// GET /.well-known/did.json - DID document for did:web resolution.
///
/// Returns the DID document containing:
/// - The user's DID (did:web)
/// - Handle as alsoKnownAs
/// - Public key verification method
/// - PDS service endpoint
///
/// No authentication required. CORS header is set to allow any origin.
pub async fn well_known_did(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: ".well-known/did.json".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Load config properties
    let user_did = match state.db.get_config_property("UserDid") {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "ServerError", "message": "UserDid not configured"})),
            ).into_response();
        }
    };

    let user_handle = match state.db.get_config_property("UserHandle") {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "ServerError", "message": "UserHandle not configured"})),
            ).into_response();
        }
    };

    let public_key_multibase = match state.db.get_config_property("UserPublicKeyMultibase") {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "ServerError", "message": "UserPublicKeyMultibase not configured"})),
            ).into_response();
        }
    };

    let pds_hostname = match state.db.get_config_property("PdsHostname") {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "ServerError", "message": "PdsHostname not configured"})),
            ).into_response();
        }
    };

    let did_doc = DidDocument {
        context: vec![
            "https://www.w3.org/ns/did/v1".to_string(),
            "https://w3id.org/security/multikey/v1".to_string(),
            "https://w3id.org/security/suites/secp256k1-2019/v1".to_string(),
        ],
        id: user_did.clone(),
        also_known_as: vec![format!("at://{}", user_handle)],
        verification_method: vec![VerificationMethod {
            id: format!("{}#atproto", user_did),
            r#type: "Multikey".to_string(),
            controller: user_did.clone(),
            public_key_multibase,
        }],
        service: vec![Service {
            id: "#atproto_pds".to_string(),
            r#type: "AtprotoPersonalDataServer".to_string(),
            service_endpoint: format!("https://{}", pds_hostname),
        }],
    };

    (
        [(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
        Json(did_doc),
    ).into_response()
}
