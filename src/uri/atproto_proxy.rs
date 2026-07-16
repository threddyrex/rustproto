

/// Parsed Atproto-Proxy header value.
/// 
/// example: atproto-proxy: did:web:labeler.example.com#atproto_labeler
/// 
/// spec: <https://atproto.com/specs/xrpc>
pub struct AtprotoProxy {
    /// The DID of the service to proxy to.
    pub did: String,
    /// The service ID within the DID document (e.g., "bsky_appview").
    pub service_id: String,
}

impl AtprotoProxy {
    /// Parse an Atproto-Proxy header value.
    ///
    /// Format: `did:web:api.bsky.app#bsky_appview`
    pub fn from_header(header_value: &str) -> Option<Self> {
        if header_value.is_empty() {
            return None;
        }

        let parts: Vec<&str> = header_value.split('#').collect();
        if parts.len() != 2 {
            return None;
        }

        if !parts[0].starts_with("did:") {
            return None;
        }

        Some(AtprotoProxy {
            did: parts[0].to_string(),
            service_id: parts[1].to_string(),
        })
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_header() {
        let header_value = "did:web:labeler.example.com#atproto_labeler";
        let atproto_proxy = AtprotoProxy::from_header(header_value).unwrap();
        assert_eq!(atproto_proxy.did, "did:web:labeler.example.com");
        assert_eq!(atproto_proxy.service_id, "atproto_labeler");
    }

    #[test]
    fn test_invalid_header_missing_hash() {
        let header_value = "did:web:labeler.example.com";
        assert!(AtprotoProxy::from_header(header_value).is_none());
    }
}

