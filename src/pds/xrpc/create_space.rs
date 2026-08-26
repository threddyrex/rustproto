//! com.atproto.simplespace.createSpace endpoint.
//!
//! Creates a new permissioned space managed by the `simplespace` implementation.
//! A space is *not* an implicit default: it comes into existence here, anchored
//! on the authenticated account's DID (its owner/authority), and its user-access
//! `policy` and app-access `appAccess` configuration are persisted so that
//! `com.atproto.simplespace.getSpace` can describe it later.
//!
//! This PDS hosts a single account, so the space owner is always this account's
//! DID.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::pds::db::{DbSpace, PdsDb, StatisticKey};
use crate::pds::server::PdsState;

use super::auth_helpers::{auth_failure_response, check_user_auth, get_caller_info, AuthType};

/// The fixed marker segment identifying a permissioned-space URI.
const SPACE_MARKER: &str = "space";

/// Known `policy` union variants (user-access policy).
const POLICY_PUBLIC: &str = "com.atproto.simplespace.defs#publicPolicy";
const POLICY_MEMBER_LIST: &str = "com.atproto.simplespace.defs#memberListPolicy";
const POLICY_MANAGING_APP: &str = "com.atproto.simplespace.defs#managingAppPolicy";

/// Known `appAccess` union variants (app-access policy).
const APP_ACCESS_OPEN: &str = "com.atproto.simplespace.defs#open";
const APP_ACCESS_ALLOW_LIST: &str = "com.atproto.simplespace.defs#allowList";

/// Request body for createSpace.
#[derive(Deserialize)]
pub struct CreateSpaceRequest {
    /// The NSID of the space type (e.g. `my.bulletin.board`).
    #[serde(rename = "type")]
    space_type: String,
    /// Optional space key. Auto-generated (TID) when absent.
    skey: Option<String>,
    /// User-access policy union.
    policy: Value,
    /// App-access policy union.
    #[serde(rename = "appAccess")]
    app_access: Value,
}

/// Successful response for createSpace.
#[derive(Serialize)]
pub struct CreateSpaceResponse {
    /// URI of the created space.
    uri: String,
}

/// Error response for createSpace.
#[derive(Serialize)]
pub struct CreateSpaceError {
    error: String,
    message: String,
}

fn error_response(status: StatusCode, error: &str, message: &str) -> Response {
    (
        status,
        Json(CreateSpaceError {
            error: error.to_string(),
            message: message.to_string(),
        }),
    )
        .into_response()
}

/// Validate that a string is a plausible NSID (dotted, non-empty segments).
fn is_valid_nsid(nsid: &str) -> bool {
    if nsid.is_empty() {
        return false;
    }
    let segments: Vec<&str> = nsid.split('.').collect();
    if segments.len() < 2 {
        return false;
    }
    segments.iter().all(|seg| {
        !seg.is_empty()
            && seg
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-')
            && seg.chars().next().map(|c| c.is_ascii_alphabetic()).unwrap_or(false)
    })
}

/// Validate a record-key-shaped space key.
fn is_valid_skey(skey: &str) -> bool {
    if skey.is_empty() || skey.len() > 512 {
        return false;
    }
    if skey == "." || skey == ".." {
        return false;
    }
    skey.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':' | '~'))
}

/// Validate the `policy` union. Returns `Err(message)` for an unsupported or
/// malformed variant.
fn validate_policy(policy: &Value) -> Result<(), String> {
    let ty = policy
        .get("$type")
        .and_then(Value::as_str)
        .ok_or_else(|| "policy is missing $type".to_string())?;
    match ty {
        POLICY_PUBLIC | POLICY_MEMBER_LIST => Ok(()),
        POLICY_MANAGING_APP => {
            let managing_app = policy.get("managingApp").and_then(Value::as_str);
            match managing_app {
                Some(app) if !app.is_empty() => Ok(()),
                _ => Err("managingAppPolicy requires a non-empty managingApp".to_string()),
            }
        }
        other => Err(format!("Unsupported policy variant: {}", other)),
    }
}

/// Validate the `appAccess` union. Returns `Err(message)` for an unsupported or
/// malformed variant.
fn validate_app_access(app_access: &Value) -> Result<(), String> {
    let ty = app_access
        .get("$type")
        .and_then(Value::as_str)
        .ok_or_else(|| "appAccess is missing $type".to_string())?;
    match ty {
        APP_ACCESS_OPEN => Ok(()),
        APP_ACCESS_ALLOW_LIST => match app_access.get("allowed").and_then(Value::as_array) {
            Some(allowed) if allowed.iter().all(Value::is_string) => Ok(()),
            _ => Err("allowList requires an 'allowed' array of client IDs".to_string()),
        },
        other => Err(format!("Unsupported appAccess variant: {}", other)),
    }
}

/// POST /xrpc/com.atproto.simplespace.createSpace - Create a permissioned space.
///
/// # Headers
///
/// * `Authorization` - Required (Legacy or OAuth). The caller becomes the owner.
///
/// # Request Body
///
/// * `type` - Required. The NSID of the space type.
/// * `skey` - Optional. The space key; auto-generated (TID) when absent.
/// * `policy` - Required. User-access policy union.
/// * `appAccess` - Required. App-access policy union.
///
/// # Returns
///
/// * `200 OK` with the created space URI on success
/// * `400 Bad Request` for malformed input or an unsupported policy/appAccess
/// * `401 Unauthorized` if not authenticated
pub async fn create_space(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<CreateSpaceRequest>,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.simplespace.createSpace".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    // Check authentication (OAuth only).
    let auth_result = check_user_auth(
        &state,
        &headers,
        Some(&[AuthType::Oauth]),
        "POST",
        "/xrpc/com.atproto.simplespace.createSpace",
    );
    if !auth_result.is_authenticated {
        return auth_failure_response(&auth_result);
    }

    // Validate the space type.
    if !is_valid_nsid(&body.space_type) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            &format!("Invalid space type (must be an NSID): {}", body.space_type),
        );
    }

    // Resolve the space key: use the provided one or generate a TID.
    let skey = match body.skey {
        Some(skey) => {
            if !is_valid_skey(&skey) {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "InvalidRequest",
                    &format!("Invalid skey: {}", skey),
                );
            }
            skey
        }
        None => generate_tid(),
    };

    // Validate the policy and appAccess unions.
    if let Err(message) = validate_policy(&body.policy) {
        return error_response(StatusCode::BAD_REQUEST, "UnsupportedPolicy", &message);
    }
    if let Err(message) = validate_app_access(&body.app_access) {
        return error_response(StatusCode::BAD_REQUEST, "UnsupportedAppAccess", &message);
    }

    // The space is anchored on this account's DID.
    let owner_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "User DID not configured",
            );
        }
    };

    let uri = format!(
        "at://{}/{}/{}/{}",
        owner_did, SPACE_MARKER, body.space_type, skey
    );

    // A space with this owner, type, and skey may not already exist.
    match state.db.space_exists(&uri) {
        Ok(true) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "SpaceAlreadyExists",
                "A space with this owner, type, and skey already exists",
            );
        }
        Ok(false) => {}
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] [CREATE] Failed to check space: {}", e));
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to check for existing space",
            );
        }
    }

    let space = DbSpace {
        uri: uri.clone(),
        owner_did,
        space_type: body.space_type,
        skey,
        policy_json: body.policy.to_string(),
        app_access_json: body.app_access.to_string(),
        created_date: PdsDb::get_current_datetime_for_db(),
    };

    if let Err(e) = state.db.insert_space(&space) {
        state
            .log
            .error(&format!("[SPACE] [CREATE] Failed to insert space: {}", e));
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ServerError",
            "Failed to persist space",
        );
    }

    state
        .log
        .info(&format!("[SPACE] [CREATE] Created space {}", uri));

    Json(CreateSpaceResponse { uri }).into_response()
}

/// Generate a TID (timestamp identifier) for use as an auto-generated space key.
///
/// TIDs are 64-bit values encoded as 13-character base32-sortable strings.
fn generate_tid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let ts = micros & 0x1F_FFFF_FFFF_FFFF;
    let clock_id: u64 = rand::random::<u16>() as u64 & 0x3FF;
    let value = (ts << 10) | clock_id;

    const ALPHABET: &[u8] = b"234567abcdefghijklmnopqrstuvwxyz";
    let mut result = String::with_capacity(13);
    let mut remaining = value;
    for _ in 0..13 {
        let idx = (remaining & 0x1F) as usize;
        result.insert(0, ALPHABET[idx] as char);
        remaining >>= 5;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_nsid() {
        assert!(is_valid_nsid("my.bulletin.board"));
        assert!(is_valid_nsid("app.bsky.group"));
        assert!(!is_valid_nsid("board"));
        assert!(!is_valid_nsid(""));
        assert!(!is_valid_nsid("my..board"));
        assert!(!is_valid_nsid("my.1bad.board"));
    }

    #[test]
    fn validates_skey() {
        assert!(is_valid_skey("self"));
        assert!(is_valid_skey("3kabc"));
        assert!(!is_valid_skey(""));
        assert!(!is_valid_skey("."));
        assert!(!is_valid_skey(".."));
        assert!(!is_valid_skey("has/slash"));
    }

    #[test]
    fn accepts_known_policies() {
        assert!(validate_policy(&json!({ "$type": POLICY_PUBLIC })).is_ok());
        assert!(validate_policy(&json!({ "$type": POLICY_MEMBER_LIST })).is_ok());
        assert!(validate_policy(
            &json!({ "$type": POLICY_MANAGING_APP, "managingApp": "did:web:app.example.com#svc" })
        )
        .is_ok());
    }

    #[test]
    fn rejects_bad_policies() {
        assert!(validate_policy(&json!({ "$type": POLICY_MANAGING_APP })).is_err());
        assert!(validate_policy(&json!({ "$type": "com.atproto.simplespace.defs#nope" })).is_err());
        assert!(validate_policy(&json!({})).is_err());
    }

    #[test]
    fn validates_app_access() {
        assert!(validate_app_access(&json!({ "$type": APP_ACCESS_OPEN })).is_ok());
        assert!(validate_app_access(
            &json!({ "$type": APP_ACCESS_ALLOW_LIST, "allowed": ["https://app.example.com"] })
        )
        .is_ok());
        assert!(validate_app_access(&json!({ "$type": APP_ACCESS_ALLOW_LIST })).is_err());
        assert!(validate_app_access(&json!({ "$type": "x#y" })).is_err());
    }

    #[test]
    fn generates_13_char_tid() {
        let tid = generate_tid();
        assert_eq!(tid.len(), 13);
        assert!(tid
            .chars()
            .all(|c| "234567abcdefghijklmnopqrstuvwxyz".contains(c)));
    }
}
