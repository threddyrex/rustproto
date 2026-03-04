//! OAuth helper utilities.
//!
//! This module provides shared utilities for OAuth endpoints.

use std::sync::Arc;

use axum::http::HeaderMap;

use crate::pds::db::PdsDb;
use crate::pds::server::PdsState;

/// Extract caller info (IP address and user agent) from headers.
///
/// IP address is extracted from X-Forwarded-For header (first IP if multiple).
/// Falls back to "unknown" if not present.
pub fn get_caller_info(headers: &HeaderMap) -> (String, String) {
    let ip_address = headers
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let user_agent = headers
        .get("User-Agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    (ip_address, user_agent)
}

/// Check if OAuth is enabled in the PDS configuration.
///
/// OAuth is enabled if the `FeatureEnabled_Oauth` config property is set to true.
pub fn is_oauth_enabled(db: &PdsDb) -> bool {
    db.get_config_property_bool("FeatureEnabled_Oauth")
        .unwrap_or(false)
}

/// Check if passkeys are enabled in the PDS configuration.
///
/// Passkeys are enabled if the `FeatureEnabled_Passkeys` config property is set to true.
pub fn is_passkeys_enabled(db: &PdsDb) -> bool {
    db.get_config_property_bool("FeatureEnabled_Passkeys")
        .unwrap_or(false)
}

/// Get the PDS issuer URL (e.g., "https://pds.example.com").
pub fn get_issuer(state: &Arc<PdsState>) -> String {
    let hostname = state
        .db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "localhost".to_string());
    format!("https://{}", hostname)
}

/// Get the PDS hostname.
pub fn get_hostname(state: &Arc<PdsState>) -> String {
    state
        .db
        .get_config_property("PdsHostname")
        .unwrap_or_else(|_| "localhost".to_string())
}

/// Get the allowed OAuth redirect URIs.
pub fn get_allowed_redirect_uris(db: &PdsDb) -> std::collections::HashSet<String> {
    db.get_config_property("OauthAllowedRedirectUris")
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Parse URL-encoded form data into key-value pairs.
pub fn parse_form_data(body: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for pair in body.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            let key = urlencoding::decode(key).unwrap_or_default().to_string();
            let value = urlencoding::decode(value).unwrap_or_default().to_string();
            map.insert(key, value);
        }
    }
    map
}

/// Get a value from URL-encoded form data.
pub fn get_form_value(body: &str, key: &str) -> Option<String> {
    parse_form_data(body).remove(key)
}

/// HTML-encode a string to prevent XSS.
pub fn html_encode(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_form_data() {
        let body = "client_id=test&redirect_uri=https%3A%2F%2Fexample.com";
        let data = parse_form_data(body);
        assert_eq!(data.get("client_id"), Some(&"test".to_string()));
        assert_eq!(
            data.get("redirect_uri"),
            Some(&"https://example.com".to_string())
        );
    }

    #[test]
    fn test_html_encode() {
        assert_eq!(html_encode("<script>alert('xss')</script>"), "&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;");
    }
}
