//! OAuth permission scope resolution.
//!
//! Expands `include:<nsid>` permission-set references into concrete granular
//! permission scope strings. An `include:` token points at a permission-set
//! Lexicon (published under `com.atproto.lexicon.schema`); resolving it inlines
//! the permissions declared by that set. Every other scope token (`atproto`,
//! `account:*`, `repo:*`, `rpc:*`, `blob:*/*`, `transition:*`, ...) is already
//! concrete and passes through unchanged.
//!
//! Resolution happens once, at OAuth session creation time. The expanded scope
//! string is stored on the `OauthSession` and becomes the single source of truth
//! for the session (including on refresh), so the granted permissions are frozen
//! at consent time and never silently widen if an upstream permission set later
//! changes.
//!
//! See the atproto permission spec: <https://atproto.com/specs/permission>.

use serde_json::Value;

use crate::log::Logger;
use crate::ws::BlueskyClient;

/// Resolves an OAuth scope string, expanding any `include:<nsid>` permission-set
/// references into concrete granular permission scopes.
///
/// Tokens are space-separated. Each `include:` token is resolved via Lexicon
/// resolution and replaced by the permissions declared in the referenced set;
/// all other tokens pass through unchanged. Duplicate scopes (which are common
/// once several sets are expanded) are collapsed while preserving first-seen
/// order.
///
/// Resolution is best-effort: if a permission set can not be resolved, the
/// original `include:` token is preserved verbatim and a warning is logged,
/// rather than failing the entire token request.
pub async fn resolve_scopes(raw_scope: &str, client: &BlueskyClient, log: &Logger) -> String {
    let mut resolved: Vec<String> = Vec::new();

    for token in raw_scope.split_whitespace() {
        if let Some((nsid, inherited_aud)) = parse_include_token(token) {
            match client.resolve_lexicon_schema(nsid).await {
                Ok(lexicon) => match permissions_from_lexicon(&lexicon) {
                    Some(permissions) => {
                        let expanded = expand_permissions(permissions, inherited_aud.as_deref());
                        log.info(&format!(
                            "[AUTH] [OAUTH] scope: Resolved permission set '{}' into {} scope(s)",
                            nsid,
                            expanded.len()
                        ));
                        for scope in expanded {
                            push_unique(&mut resolved, scope);
                        }
                    }
                    None => {
                        log.warning(&format!(
                            "[AUTH] [OAUTH] scope: Permission set '{}' has no permissions; keeping include token verbatim",
                            nsid
                        ));
                        push_unique(&mut resolved, token.to_string());
                    }
                },
                Err(e) => {
                    log.warning(&format!(
                        "[AUTH] [OAUTH] scope: Failed to resolve permission set '{}': {:?}; keeping include token verbatim",
                        nsid, e
                    ));
                    push_unique(&mut resolved, token.to_string());
                }
            }
        } else {
            push_unique(&mut resolved, token.to_string());
        }
    }

    resolved.join(" ")
}

/// Parses an `include:<nsid>[?aud=<aud>]` token.
///
/// Returns `None` if the token is not an `include:` reference. Otherwise returns
/// the referenced NSID and the (still percent-encoded) `aud` value from the
/// invocation, if present. The `aud` is inherited by `rpc` permissions inside
/// the set that set `inheritAud`.
fn parse_include_token(token: &str) -> Option<(&str, Option<String>)> {
    let rest = token.strip_prefix("include:")?;

    let (nsid, query) = match rest.split_once('?') {
        Some((nsid, query)) => (nsid, Some(query)),
        None => (rest, None),
    };

    if nsid.is_empty() {
        return None;
    }

    let aud = query.and_then(|q| {
        q.split('&')
            .find_map(|pair| pair.strip_prefix("aud="))
            .filter(|v| !v.is_empty())
            .map(|v| v.to_string())
    });

    Some((nsid, aud))
}

/// Navigates a resolved Lexicon record to its permission-set `permissions` array.
///
/// The record may arrive wrapped in a `com.atproto.repo.getRecord` envelope
/// (`{ uri, cid, value: { ...lexicon... } }`) or as the bare Lexicon document, so
/// both shapes are handled.
fn permissions_from_lexicon(lexicon: &Value) -> Option<&Vec<Value>> {
    let defs = lexicon
        .get("value")
        .and_then(|v| v.get("defs"))
        .or_else(|| lexicon.get("defs"))?;

    defs.get("main")
        .and_then(|m| m.get("permissions"))
        .and_then(|p| p.as_array())
}

/// Expands a permission-set `permissions` array into granular scope strings.
fn expand_permissions(permissions: &[Value], inherited_aud: Option<&str>) -> Vec<String> {
    let mut scopes = Vec::new();
    for permission in permissions {
        permission_to_scopes(permission, inherited_aud, &mut scopes);
    }
    scopes
}

/// Converts a single permission object into zero or more granular scope strings.
///
/// Only the resource types that are valid inside a permission set (`rpc`, `repo`)
/// are expanded. Unknown resources, and permissions with invalid `aud`
/// configuration, are ignored per the spec.
fn permission_to_scopes(permission: &Value, inherited_aud: Option<&str>, out: &mut Vec<String>) {
    let resource = permission.get("resource").and_then(|r| r.as_str());

    match resource {
        Some("rpc") => {
            let aud = match resolve_rpc_aud(permission, inherited_aud) {
                Some(aud) => aud,
                // Invalid aud configuration: ignore this permission entirely.
                None => return,
            };
            for lxm in string_array(permission.get("lxm")) {
                out.push(format!("rpc:{}?aud={}", lxm, aud));
            }
        }
        Some("repo") => {
            let action_suffix = action_suffix(permission.get("action"));
            for collection in string_array(permission.get("collection")) {
                out.push(format!("repo:{}{}", collection, action_suffix));
            }
        }
        // `blob`, `account`, and `identity` can not appear in permission sets, and
        // unknown resources must be ignored so the permission model can evolve.
        _ => {}
    }
}

/// Determines the effective `aud` for an `rpc` permission inside a set.
///
/// Returns `None` (meaning: ignore the permission) for invalid configurations:
/// `inheritAud` combined with an explicit `aud`, `inheritAud` without an
/// inherited value, or a plain permission missing its required `aud`.
fn resolve_rpc_aud(permission: &Value, inherited_aud: Option<&str>) -> Option<String> {
    let inherit = permission
        .get("inheritAud")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let own_aud = permission.get("aud").and_then(|v| v.as_str());

    if inherit {
        if own_aud.is_some() {
            return None;
        }
        inherited_aud.map(|a| a.to_string())
    } else {
        own_aud.map(|a| a.to_string())
    }
}

/// Builds the `?action=...` suffix for a `repo` permission, preserving order.
fn action_suffix(action: Option<&Value>) -> String {
    let actions = string_array(action);
    if actions.is_empty() {
        return String::new();
    }
    let params: Vec<String> = actions.iter().map(|a| format!("action={}", a)).collect();
    format!("?{}", params.join("&"))
}

/// Reads a JSON value that may be a string array (or a single string) into a
/// `Vec<&str>`, skipping non-string entries.
fn string_array(value: Option<&Value>) -> Vec<&str> {
    match value {
        Some(Value::Array(items)) => items.iter().filter_map(|v| v.as_str()).collect(),
        Some(Value::String(s)) => vec![s.as_str()],
        _ => Vec::new(),
    }
}

/// Pushes a scope onto the accumulator unless it is already present.
fn push_unique(scopes: &mut Vec<String>, scope: String) {
    if !scopes.iter().any(|s| s == &scope) {
        scopes.push(scope);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_include_token_without_aud() {
        let (nsid, aud) = parse_include_token("include:my.bulletin.permissions").unwrap();
        assert_eq!(nsid, "my.bulletin.permissions");
        assert_eq!(aud, None);
    }

    #[test]
    fn parse_include_token_with_aud() {
        let (nsid, aud) =
            parse_include_token("include:app.bsky.authFullApp?aud=did:web:api.bsky.app%23bsky_appview")
                .unwrap();
        assert_eq!(nsid, "app.bsky.authFullApp");
        assert_eq!(aud.as_deref(), Some("did:web:api.bsky.app%23bsky_appview"));
    }

    #[test]
    fn parse_include_token_rejects_non_include() {
        assert!(parse_include_token("rpc:app.bsky.feed.searchPosts?aud=x").is_none());
        assert!(parse_include_token("atproto").is_none());
        assert!(parse_include_token("include:").is_none());
    }

    #[test]
    fn expand_rpc_inherits_aud() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "rpc",
            "inheritAud": true,
            "lxm": ["app.bsky.feed.getPosts", "app.bsky.feed.searchPosts"]
        })];
        let scopes = expand_permissions(&permissions, Some("did:web:api.bsky.app%23bsky_appview"));
        assert_eq!(
            scopes,
            vec![
                "rpc:app.bsky.feed.getPosts?aud=did:web:api.bsky.app%23bsky_appview",
                "rpc:app.bsky.feed.searchPosts?aud=did:web:api.bsky.app%23bsky_appview",
            ]
        );
    }

    #[test]
    fn expand_rpc_inherit_without_inherited_aud_is_ignored() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "rpc",
            "inheritAud": true,
            "lxm": ["app.bsky.feed.getPosts"]
        })];
        // No aud on the include invocation -> inheritAud rpc perms are invalid.
        assert!(expand_permissions(&permissions, None).is_empty());
    }

    #[test]
    fn expand_rpc_inherit_with_explicit_aud_is_ignored() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "rpc",
            "inheritAud": true,
            "aud": "did:web:other#svc",
            "lxm": ["app.bsky.feed.getPosts"]
        })];
        assert!(expand_permissions(&permissions, Some("did:web:api.bsky.app%23bsky_appview")).is_empty());
    }

    #[test]
    fn expand_rpc_with_own_aud() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "rpc",
            "aud": "did:web:api.example.com%23svc",
            "lxm": ["com.example.doThing"]
        })];
        let scopes = expand_permissions(&permissions, None);
        assert_eq!(scopes, vec!["rpc:com.example.doThing?aud=did:web:api.example.com%23svc"]);
    }

    #[test]
    fn expand_repo_with_actions() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "repo",
            "action": ["create", "update", "delete"],
            "collection": ["app.bsky.feed.post", "app.bsky.feed.like"]
        })];
        let scopes = expand_permissions(&permissions, None);
        assert_eq!(
            scopes,
            vec![
                "repo:app.bsky.feed.post?action=create&action=update&action=delete",
                "repo:app.bsky.feed.like?action=create&action=update&action=delete",
            ]
        );
    }

    #[test]
    fn expand_repo_without_actions() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "repo",
            "collection": ["app.bsky.feed.post"]
        })];
        let scopes = expand_permissions(&permissions, None);
        assert_eq!(scopes, vec!["repo:app.bsky.feed.post"]);
    }

    #[test]
    fn expand_ignores_unknown_resource() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "something-new",
            "collection": ["app.bsky.feed.post"]
        })];
        assert!(expand_permissions(&permissions, None).is_empty());
    }

    #[test]
    fn permissions_from_wrapped_record() {
        let record = json!({
            "uri": "at://did:plc:x/com.atproto.lexicon.schema/app.bsky.authFullApp",
            "cid": "bafy...",
            "value": {
                "lexicon": 1,
                "id": "app.bsky.authFullApp",
                "defs": {
                    "main": {
                        "type": "permission-set",
                        "permissions": [
                            { "type": "permission", "resource": "repo", "collection": ["app.bsky.feed.post"] }
                        ]
                    }
                }
            }
        });
        let permissions = permissions_from_lexicon(&record).unwrap();
        assert_eq!(permissions.len(), 1);
    }

    #[test]
    fn permissions_from_bare_lexicon() {
        let lexicon = json!({
            "lexicon": 1,
            "id": "app.bsky.authFullApp",
            "defs": {
                "main": {
                    "type": "permission-set",
                    "permissions": [
                        { "type": "permission", "resource": "repo", "collection": ["app.bsky.feed.post"] }
                    ]
                }
            }
        });
        assert!(permissions_from_lexicon(&lexicon).is_some());
    }
}
