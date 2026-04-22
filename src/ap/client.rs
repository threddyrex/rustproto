//! ActivityPub client for resolving actors.
//!
//! Resolution is a two-step process:
//! 1. WebFinger lookup at `https://{host}/.well-known/webfinger?resource=acct:{user}@{host}`
//!    to find a link with `rel = "self"` (typically `application/activity+json`).
//! 2. Fetch the linked actor object with `Accept: application/activity+json`.

use reqwest::Client;
use serde_json::Value;
use thiserror::Error;

use crate::ap::models::{ActivityPubActor, WebFingerResponse, WebFingerLink};
use crate::log::logger;

#[derive(Error, Debug)]
pub enum ApClientError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("JSON parsing failed: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Invalid actor identifier: {0}")]
    InvalidActor(String),

    #[error("Resolution failed: {0}")]
    ResolutionFailed(String),
}

/// MIME type used for ActivityPub object fetches.
pub const ACTIVITY_JSON_MIME: &str = "application/activity+json";

/// The result of resolving an ActivityPub actor.
///
/// Bundles the typed actor object with the WebFinger metadata that produced it
/// (when WebFinger was actually used). The raw JSON of the actor object is
/// also kept for callers that want full fidelity.
#[derive(Debug, Clone)]
pub struct ApActor {
    /// `acct:user@host` value used as the WebFinger `resource` query, if any.
    pub webfinger_subject: Option<String>,
    /// Full URL of the WebFinger request, if WebFinger was used.
    pub webfinger_url: Option<String>,
    /// `href` of the WebFinger `self` link (== the actor object URL).
    pub actor_url: Option<String>,
    /// Parsed WebFinger response, if WebFinger was used.
    pub webfinger: Option<WebFingerResponse>,
    /// Typed ActivityPub actor object.
    pub actor: ActivityPubActor,
    /// Raw JSON of the actor object for full-fidelity inspection.
    pub raw: Value,
}

/// HTTP client for ActivityPub interactions.
pub struct ApClient {
    client: Client,
}

impl Default for ApClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ApClient {
    /// Creates a new ActivityPub client.
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .user_agent("rustproto")
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Resolves an ActivityPub actor.
    ///
    /// The `actor` argument may be in any of these forms:
    /// - `user@host`
    /// - `@user@host`
    /// - `acct:user@host`
    /// - a full actor URL (e.g. `https://host/users/user`); WebFinger is skipped
    pub async fn resolve_actor(&self, actor: &str) -> Result<ApActor, ApClientError> {
        let actor = actor.trim();
        if actor.is_empty() {
            return Err(ApClientError::InvalidActor("Actor is empty".to_string()));
        }

        // If a URL was passed in, skip WebFinger and fetch the actor object directly.
        if actor.starts_with("http://") || actor.starts_with("https://") {
            logger().info(&format!(
                "[AP] No WebFinger needed; treating actor as direct URL: {}",
                actor
            ));
            let (raw, parsed) = self.fetch_actor_object(actor).await?;
            return Ok(ApActor {
                webfinger_subject: None,
                webfinger_url: None,
                actor_url: Some(actor.to_string()),
                webfinger: None,
                actor: parsed,
                raw,
            });
        }

        let (user, host) = parse_acct(actor)?;
        let subject = format!("acct:{}@{}", user, host);

        let (webfinger_url, webfinger, self_href) =
            self.webfinger_lookup(&host, &subject).await?;
        let (raw, parsed) = self.fetch_actor_object(&self_href).await?;

        Ok(ApActor {
            webfinger_subject: Some(subject),
            webfinger_url: Some(webfinger_url),
            actor_url: Some(self_href),
            webfinger: Some(webfinger),
            actor: parsed,
            raw,
        })
    }

    /// Performs a WebFinger lookup. Returns `(webfinger_url, parsed_response, self_href)`.
    async fn webfinger_lookup(
        &self,
        host: &str,
        subject: &str,
    ) -> Result<(String, WebFingerResponse, String), ApClientError> {
        let url = format!(
            "https://{}/.well-known/webfinger?resource={}",
            host,
            urlencode(subject)
        );

        logger().info(&format!("[AP] WebFinger GET {}", url));

        let resp = self
            .client
            .get(&url)
            .header("Accept", "application/jrd+json, application/json")
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            logger().warning(&format!("[AP] WebFinger GET {} -> HTTP {}", url, status));
            return Err(ApClientError::ResolutionFailed(format!(
                "WebFinger request to {} failed with status {}",
                url, status
            )));
        }

        let body_text = resp.text().await?;
        logger().info(&format!(
            "[AP] WebFinger GET {} -> HTTP {} ({} bytes)",
            url,
            status,
            body_text.len()
        ));

        let parsed: WebFingerResponse = serde_json::from_str(&body_text)?;
        logger().trace(&format!(
            "[AP] WebFinger response body:\n{}",
            serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| body_text.clone())
        ));

        let self_href = pick_self_href(&parsed.links).ok_or_else(|| {
            ApClientError::ResolutionFailed(
                "WebFinger response did not contain a usable 'self' link".to_string(),
            )
        })?;

        logger().info(&format!("[AP] WebFinger picked self href: {}", self_href));

        Ok((url, parsed, self_href))
    }

    /// Fetches and parses an actor object from the given URL.
    async fn fetch_actor_object(
        &self,
        url: &str,
    ) -> Result<(Value, ActivityPubActor), ApClientError> {
        logger().info(&format!("[AP] Actor GET {}", url));

        let resp = self
            .client
            .get(url)
            .header("Accept", ACTIVITY_JSON_MIME)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            logger().warning(&format!("[AP] Actor GET {} -> HTTP {}", url, status));
            return Err(ApClientError::ResolutionFailed(format!(
                "Actor fetch from {} failed with status {}",
                url, status
            )));
        }

        let body_text = resp.text().await?;
        logger().info(&format!(
            "[AP] Actor GET {} -> HTTP {} ({} bytes)",
            url,
            status,
            body_text.len()
        ));

        let raw: Value = serde_json::from_str(&body_text)?;
        let parsed: ActivityPubActor = serde_json::from_value(raw.clone())?;
        logger().trace(&format!(
            "[AP] Actor response body:\n{}",
            serde_json::to_string_pretty(&raw).unwrap_or_else(|_| body_text.clone())
        ));
        Ok((raw, parsed))
    }
}

/// Picks the best `self` link from a WebFinger response, preferring
/// `application/activity+json` (or any activitystreams type), falling back to
/// the first `self` link with an `href`.
fn pick_self_href(links: &[WebFingerLink]) -> Option<String> {
    let mut fallback: Option<String> = None;
    for link in links {
        if link.rel.as_deref() != Some("self") {
            continue;
        }
        let href = match &link.href {
            Some(h) => h.clone(),
            None => continue,
        };
        let typ = link.link_type.as_deref().unwrap_or("");
        if typ == ACTIVITY_JSON_MIME || typ.contains("activitystreams") {
            return Some(href);
        }
        if fallback.is_none() {
            fallback = Some(href);
        }
    }
    fallback
}

/// Parses an `acct:`-style identifier into `(user, host)`.
fn parse_acct(input: &str) -> Result<(String, String), ApClientError> {
    let mut s = input;
    if let Some(rest) = s.strip_prefix("acct:") {
        s = rest;
    }
    if let Some(rest) = s.strip_prefix('@') {
        s = rest;
    }

    let parts: Vec<&str> = s.split('@').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return Err(ApClientError::InvalidActor(format!(
            "Expected 'user@host', got '{}'",
            input
        )));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Minimal URL-encoder for the WebFinger `resource` query parameter.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let safe = b.is_ascii_alphanumeric()
            || b == b'-'
            || b == b'_'
            || b == b'.'
            || b == b'~';
        if safe {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_acct_plain() {
        let (u, h) = parse_acct("alice@example.com").unwrap();
        assert_eq!(u, "alice");
        assert_eq!(h, "example.com");
    }

    #[test]
    fn parse_acct_at_prefix() {
        let (u, h) = parse_acct("@alice@example.com").unwrap();
        assert_eq!(u, "alice");
        assert_eq!(h, "example.com");
    }

    #[test]
    fn parse_acct_acct_prefix() {
        let (u, h) = parse_acct("acct:alice@example.com").unwrap();
        assert_eq!(u, "alice");
        assert_eq!(h, "example.com");
    }

    #[test]
    fn parse_acct_invalid() {
        assert!(parse_acct("alice").is_err());
        assert!(parse_acct("").is_err());
    }

    #[test]
    fn urlencode_acct() {
        assert_eq!(urlencode("acct:alice@example.com"), "acct%3Aalice%40example.com");
    }

    #[test]
    fn pick_self_prefers_activity_json() {
        let links = vec![
            WebFingerLink {
                rel: Some("self".into()),
                link_type: Some("text/html".into()),
                href: Some("https://x/html".into()),
                ..Default::default()
            },
            WebFingerLink {
                rel: Some("self".into()),
                link_type: Some("application/activity+json".into()),
                href: Some("https://x/ap".into()),
                ..Default::default()
            },
        ];
        assert_eq!(pick_self_href(&links).as_deref(), Some("https://x/ap"));
    }
}
