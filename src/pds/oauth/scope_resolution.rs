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
    permissions
        .iter()
        .filter_map(|permission| permission_to_scope(permission, inherited_aud))
        .collect()
}

/// Converts a single permission object into its granular scope string.
///
/// Each permission entry becomes exactly one scope token (a permission set does
/// not get to decide how many scopes it becomes). Only the resource types that
/// are valid inside a permission set are expanded (`repo`, `rpc`, and `space`).
/// Unknown resources, and permissions with invalid configuration, are ignored
/// per the spec so the permission model can evolve.
fn permission_to_scope(permission: &Value, inherited_aud: Option<&str>) -> Option<String> {
    match permission.get("resource").and_then(|r| r.as_str()) {
        Some("repo") => {
            let collection = string_array(permission.get("collection"));
            if collection.is_empty() {
                return None;
            }
            Some(format_scope(
                "repo",
                "collection",
                &[
                    ("collection", collection),
                    ("action", string_array(permission.get("action"))),
                ],
            ))
        }
        Some("rpc") => {
            let lxm = string_array(permission.get("lxm"));
            if lxm.is_empty() {
                return None;
            }
            // Invalid aud configuration means the whole permission is ignored.
            let aud = resolve_rpc_aud(permission, inherited_aud)?;
            Some(format_scope(
                "rpc",
                "lxm",
                &[("lxm", lxm), ("aud", vec![aud])],
            ))
        }
        Some("space") => {
            // A space grant with no space type names no spaces, so it grants none.
            let space_type = permission.get("spaceType").and_then(|v| v.as_str())?;
            Some(format_scope(
                "space",
                "spaceType",
                &[
                    ("spaceType", vec![space_type]),
                    ("authority", string_array(permission.get("authority"))),
                    ("skey", string_array(permission.get("skey"))),
                    ("collection", string_array(permission.get("collection"))),
                    ("action", string_array(permission.get("action"))),
                    ("manage", string_array(permission.get("manage"))),
                ],
            ))
        }
        // `blob`, `account`, and `identity` can not appear in permission sets, and
        // unknown resources must be ignored so the permission model can evolve.
        _ => None,
    }
}

/// Formats a single permission as an atproto scope string.
///
/// The `positional` field, when it carries exactly one value, is rendered as the
/// positional segment (`prefix:value`); otherwise every field is rendered as
/// repeated `key=value` query parameters. Empty fields are omitted. Field order
/// is preserved as given by the caller.
fn format_scope(prefix: &str, positional_key: &str, fields: &[(&str, Vec<&str>)]) -> String {
    let mut positional: Option<&str> = None;
    let mut params: Vec<String> = Vec::new();

    for (key, values) in fields {
        if values.is_empty() {
            continue;
        }
        if *key == positional_key && values.len() == 1 {
            positional = Some(values[0]);
        } else {
            for value in values {
                params.push(format!("{}={}", key, value));
            }
        }
    }

    let mut scope = prefix.to_string();
    if let Some(value) = positional {
        scope.push(':');
        scope.push_str(value);
    }
    if !params.is_empty() {
        scope.push('?');
        scope.push_str(&params.join("&"));
    }
    scope
}

/// Determines the effective `aud` for an `rpc` permission inside a set.
///
/// Returns `None` (meaning: ignore the permission) for invalid configurations:
/// `inheritAud` combined with an explicit `aud`, `inheritAud` without an
/// inherited value, or a plain permission missing its required `aud`.
fn resolve_rpc_aud<'a>(permission: &'a Value, inherited_aud: Option<&'a str>) -> Option<&'a str> {
    let inherit = permission
        .get("inheritAud")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let own_aud = permission.get("aud").and_then(|v| v.as_str());

    if inherit {
        if own_aud.is_some() {
            return None;
        }
        inherited_aud
    } else {
        own_aud
    }
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
        // Multiple lxm values -> a single token with repeated params.
        assert_eq!(
            scopes,
            vec![
                "rpc?lxm=app.bsky.feed.getPosts&lxm=app.bsky.feed.searchPosts&aud=did:web:api.bsky.app%23bsky_appview",
            ]
        );
    }

    #[test]
    fn expand_rpc_single_lxm_uses_positional() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "rpc",
            "inheritAud": true,
            "lxm": ["app.bsky.feed.searchPosts"]
        })];
        let scopes = expand_permissions(&permissions, Some("did:web:api.bsky.app%23bsky_appview"));
        assert_eq!(
            scopes,
            vec!["rpc:app.bsky.feed.searchPosts?aud=did:web:api.bsky.app%23bsky_appview"]
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
        // Multiple collections -> single token, collections as repeated params.
        assert_eq!(
            scopes,
            vec![
                "repo?collection=app.bsky.feed.post&collection=app.bsky.feed.like&action=create&action=update&action=delete",
            ]
        );
    }

    #[test]
    fn expand_repo_single_collection_uses_positional() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "repo",
            "collection": ["app.bsky.feed.post"]
        })];
        let scopes = expand_permissions(&permissions, None);
        assert_eq!(scopes, vec!["repo:app.bsky.feed.post"]);
    }

    #[test]
    fn expand_space_grant() {
        // The real `my.bulletin.permissions` permission entry.
        let permissions = vec![json!({
            "type": "permission",
            "resource": "space",
            "spaceType": "my.bulletin.board",
            "authority": "*",
            "skey": "self",
            "collection": ["my.bulletin.post", "my.bulletin.removal", "my.bulletin.position"],
            "action": ["read", "create", "update", "delete"],
            "manage": ["create", "update", "delete"]
        })];
        let scopes = expand_permissions(&permissions, None);
        assert_eq!(
            scopes,
            vec![
                "space:my.bulletin.board?authority=*&skey=self&collection=my.bulletin.post&collection=my.bulletin.removal&collection=my.bulletin.position&action=read&action=create&action=update&action=delete&manage=create&manage=update&manage=delete",
            ]
        );
    }

    #[test]
    fn expand_bare_space_grant_needs_no_query() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "space",
            "spaceType": "app.bulleted.space"
        })];
        let scopes = expand_permissions(&permissions, None);
        assert_eq!(scopes, vec!["space:app.bulleted.space"]);
    }

    #[test]
    fn expand_space_without_space_type_is_ignored() {
        let permissions = vec![json!({
            "type": "permission",
            "resource": "space",
            "authority": "self"
        })];
        assert!(expand_permissions(&permissions, None).is_empty());
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
