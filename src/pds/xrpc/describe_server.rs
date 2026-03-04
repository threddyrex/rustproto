//! com.atproto.server.describeServer endpoint.
//!
//! Returns metadata about the PDS server, including supported user domains,
//! DID, and configuration flags.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::IntoResponse,
};
use serde::Serialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::pds::xrpc::auth_helpers::get_caller_info;

/// Server description response structure.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DescribeServerResponse {
    /// Whether an invite code is required to create an account.
    invite_code_required: bool,
    /// Whether phone verification is required for account creation.
    phone_verification_required: bool,
    /// List of available user domains for handle creation.
    available_user_domains: Vec<String>,
    /// Optional links to privacy policy and terms of service.
    #[serde(skip_serializing_if = "Option::is_none")]
    links: Option<LinksResponse>,
    /// Optional contact information.
    #[serde(skip_serializing_if = "Option::is_none")]
    contact: Option<ContactResponse>,
    /// The DID of this PDS server.
    did: String,
}

/// Links section for describe server response.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LinksResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    privacy_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    terms_of_service: Option<String>,
}

/// Contact section for describe server response.
#[derive(Serialize)]
struct ContactResponse {
    email: String,
}

/// GET /xrpc/com.atproto.server.describeServer - Server metadata endpoint.
///
/// Returns information about this PDS server including:
/// - Available user domains for handle creation
/// - The server's DID
/// - Whether invite codes or phone verification are required
pub async fn describe_server(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.server.describeServer".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Get configuration values
    let available_user_domain = state
        .db
        .get_config_property("PdsAvailableUserDomain")
        .unwrap_or_else(|_| String::new());

    let did = state
        .db
        .get_config_property("PdsDid")
        .unwrap_or_else(|_| String::new());

    let response = DescribeServerResponse {
        invite_code_required: true,
        phone_verification_required: true,
        available_user_domains: vec![available_user_domain],
        links: None,
        contact: None,
        did,
    };

    Json(response)
}
