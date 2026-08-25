//! `com.atproto.simplespace.createSpace` endpoint.

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

use crate::pds::{
    db::{PdsDb, SimpleSpace, StatisticKey},
    oauth::get_allowed_permission_sets,
    server::PdsState,
    user_repo::UserRepo,
};

use super::auth_helpers::{
    check_oauth_auth_with_scope, get_caller_info, scope_includes_permission_set,
};

const CREATE_SPACE_PATH: &str = "/xrpc/com.atproto.simplespace.createSpace";

#[derive(Deserialize)]
pub struct CreateSpaceRequest {
    #[serde(rename = "type")]
    space_type: String,
    skey: Option<String>,
    policy: Value,
    #[serde(rename = "appAccess")]
    app_access: Value,
}

#[derive(Serialize)]
pub struct CreateSpaceResponse {
    uri: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

fn error(status: StatusCode, name: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: name.to_string(),
            message: message.into(),
        }),
    )
        .into_response()
}

fn is_nsid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 317
        && value.split('.').count() >= 3
        && value.split('.').all(|segment| {
            !segment.is_empty()
                && segment.len() <= 63
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn is_record_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !value.contains('/')
        && !value.contains('?')
        && !value.contains('#')
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

fn union_type(value: &Value) -> Option<&str> {
    value.get("$type")?.as_str()
}

fn is_supported_policy(value: &Value) -> bool {
    match union_type(value) {
        Some("com.atproto.simplespace.defs#publicPolicy") => true,
        Some("com.atproto.simplespace.defs#managingAppPolicy") => value
            .get("managingApp")
            .and_then(Value::as_str)
            .is_some_and(|app| !app.is_empty()),
        _ => false,
    }
}

fn is_supported_app_access(value: &Value) -> bool {
    matches!(union_type(value), Some("com.atproto.simplespace.defs#open"))
}

fn scope_grants_manage_create(scope: &str, space_type: &str) -> bool {
    scope.split_whitespace().any(|token| {
        let Some(rest) = token.strip_prefix("space:") else {
            return false;
        };
        let mut parts = rest.splitn(2, '?');
        let granted_type = parts.next().unwrap_or("");
        let query = parts.next().unwrap_or("");
        (granted_type == space_type || granted_type == "*")
            && query.split('&').any(|item| item == "manage=create")
    })
}

/// POST /xrpc/com.atproto.simplespace.createSpace.
pub async fn create_space(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<CreateSpaceRequest>,
) -> Response {
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.simplespace.createSpace".to_string(),
        ip_address,
        user_agent,
    };
    let _ = state.db.increment_statistic_for_endpoint(&stat_key);

    let auth = check_oauth_auth_with_scope(&state, &headers, "POST", CREATE_SPACE_PATH);
    if !auth.is_authenticated {
        return error(
            if auth.is_expired {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::UNAUTHORIZED
            },
            if auth.is_expired {
                "ExpiredToken"
            } else {
                "AuthRequired"
            },
            auth.error
                .unwrap_or_else(|| "OAuth authentication is required".to_string()),
        );
    }

    if !is_nsid(&body.space_type) {
        return error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "type must be a valid NSID",
        );
    }
    let skey = body.skey.unwrap_or_else(UserRepo::generate_tid);
    if !is_record_key(&skey) {
        return error(
            StatusCode::BAD_REQUEST,
            "InvalidRequest",
            "skey must be a valid record key",
        );
    }
    if !is_supported_policy(&body.policy) {
        return error(
            StatusCode::BAD_REQUEST,
            "UnsupportedPolicy",
            "The requested policy is not implemented",
        );
    }
    if !is_supported_app_access(&body.app_access) {
        return error(
            StatusCode::BAD_REQUEST,
            "UnsupportedAppAccess",
            "The requested appAccess policy is not implemented",
        );
    }

    let scope = auth.scope.unwrap_or_default();
    let allowed_permission_sets = get_allowed_permission_sets(&state.db);
    if !scope_grants_manage_create(&scope, &body.space_type)
        && !scope_includes_permission_set(&scope, &allowed_permission_sets)
    {
        return error(
            StatusCode::FORBIDDEN,
            "InvalidToken",
            "Session is missing a covering space manage=create grant",
        );
    }

    let authority = match auth.user_did {
        Some(did) => did,
        None => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Token missing subject",
            );
        }
    };
    let configured_did = match state.db.get_config_property("UserDid") {
        Ok(did) => did,
        Err(_) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "User DID not configured",
            );
        }
    };
    if authority != configured_did {
        return error(
            StatusCode::FORBIDDEN,
            "InvalidToken",
            "Space authority is not hosted here",
        );
    }

    let space = SimpleSpace {
        authority: authority.clone(),
        space_type: body.space_type.clone(),
        skey: skey.clone(),
        policy_json: body.policy.to_string(),
        app_access_json: body.app_access.to_string(),
        created_date: PdsDb::get_current_datetime_for_db(),
    };
    match state.db.insert_simple_space(&space) {
        Ok(()) => Json(CreateSpaceResponse {
            uri: format!("at://{authority}/space/{}/{skey}", body.space_type),
        })
        .into_response(),
        Err(crate::pds::db::PdsDbError::SqliteError(rusqlite::Error::SqliteFailure(_, _))) => {
            error(
                StatusCode::CONFLICT,
                "SpaceAlreadyExists",
                "A space with this type and key already exists",
            )
        }
        Err(e) => {
            state
                .log
                .error(&format!("[SPACE] Failed to create space: {e}"));
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServerError",
                "Failed to create space",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_space_type_and_key() {
        assert!(is_nsid("my.bulletin.board"));
        assert!(is_record_key("self"));
    }

    #[test]
    fn rejects_invalid_space_type_and_key() {
        assert!(!is_nsid("bulletin"));
        assert!(!is_nsid("my..board"));
        assert!(!is_record_key(""));
        assert!(!is_record_key("contains/slash"));
    }

    #[test]
    fn supports_only_enforceable_configuration_variants() {
        assert!(is_supported_policy(&serde_json::json!({
            "$type": "com.atproto.simplespace.defs#publicPolicy"
        })));
        assert!(is_supported_policy(&serde_json::json!({
            "$type": "com.atproto.simplespace.defs#managingAppPolicy",
            "managingApp": "did:web:bulletin.example#bulletin"
        })));
        assert!(!is_supported_policy(&serde_json::json!({
            "$type": "com.atproto.simplespace.defs#memberListPolicy"
        })));
        assert!(is_supported_app_access(&serde_json::json!({
            "$type": "com.atproto.simplespace.defs#open"
        })));
        assert!(!is_supported_app_access(&serde_json::json!({
            "$type": "com.atproto.simplespace.defs#allowList",
            "allowed": ["https://app.example/client-metadata.json"]
        })));
    }

    #[test]
    fn recognizes_manage_create_scope() {
        assert!(scope_grants_manage_create(
            "atproto space:my.bulletin.board?manage=create",
            "my.bulletin.board"
        ));
        assert!(scope_grants_manage_create(
            "atproto space:*?manage=create",
            "my.bulletin.board"
        ));
        assert!(!scope_grants_manage_create(
            "atproto space:my.bulletin.board?action=create",
            "my.bulletin.board"
        ));
    }
}
