//! interacting with at:// uris


/// Represents an AT URI, which is a structured identifier used in the atproto ecosystem.
/// spec: <https://atproto.com/specs/at-uri-scheme>
#[derive(Debug)]
pub struct AtUri {
    pub authority: String,
    pub collection: String,
    pub rkey: String,
}


impl AtUri {
    pub fn new(authority: &str, collection: &str, rkey: &str) -> Self {
        AtUri {
            authority: authority.to_string(),
            collection: collection.to_string(),
            rkey: rkey.to_string(),
        }
    }

    /// Parses a Bluesky post URL into an AtUri struct.
    /// Example URL: https://bsky.app/profile/{did or handle}/post/{rkey}
    pub fn from_bsky_post_url(url: &str) -> Option<Self> {
        // Example URL: https://bsky.app/profile/{did or handle}/post/{rkey}
        let parts: Vec<&str> = url.split('/').collect();
        if parts.len() == 7 && parts[3] == "profile" && parts[5] == "post" {
            let authority = parts[4]; // {did or handle}
            let collection = "app.bsky.feed.post"; // collection is fixed for posts
            let rkey = parts[6]; // {rkey}
            Some(AtUri::new(authority, &collection, rkey))
        } else {
            None
        }
    } 

    /// Parses an AT URI string into an AtUri struct.
    /// Example AT URI: at://{authority}/{collection}/{rkey}
    pub fn from_at_uri(uri: &str) -> Option<Self> {
        // Example AT URI: at://{authority}/{collection}/{rkey}
        if !uri.starts_with("at://") {
            return None;
        }
        let parts: Vec<&str> = uri.split('/').collect();
        if parts.len() == 5 {
            let authority = parts[2];
            let collection = parts[3];
            let rkey = parts[4];
            Some(AtUri::new(authority, collection, rkey))
        } else {
            None
        }
    }

    /// Converts the AtUri struct back into an AT URI string.
    pub fn to_at_uri(&self) -> String {
        format!("at://{}/{}/{}", self.authority, self.collection, self.rkey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_bsky_post_url() {
        let bsky_url = "https://bsky.app/profile/did:plc:abc123/post/xyz789";
        let at_uri = AtUri::from_bsky_post_url(bsky_url).unwrap();
        assert_eq!(at_uri.authority, "did:plc:abc123");
        assert_eq!(at_uri.collection, "app.bsky.feed.post");
        assert_eq!(at_uri.rkey, "xyz789");
    }

    #[test]
    fn test_from_bsky_post_url_invalid() {
        let invalid_url = "https://bsky.app/profile/did:plc:abc123/post"; // Missing rkey
        assert!(AtUri::from_bsky_post_url(invalid_url).is_none());
    }

    #[test]
    fn test_from_at_uri() {
        let at_uri_str = "at://did:plc:abc123/app.bsky.feed.post/xyz789";
        let at_uri = AtUri::from_at_uri(at_uri_str).unwrap();
        assert_eq!(at_uri.authority, "did:plc:abc123");
        assert_eq!(at_uri.collection, "app.bsky.feed.post");
        assert_eq!(at_uri.rkey, "xyz789");
    }

    #[test]
    fn test_from_at_uri_invalid() {
        let invalid_at_uri_str = "at://did:plc:abc123/app.bsky.feed.post"; // Missing rkey
        assert!(AtUri::from_at_uri(invalid_at_uri_str).is_none());
    }
}
