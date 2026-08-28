//! interacting with at:// uris for permissioned spaces and their records


/// The fixed marker segment identifying a permissioned-space URI
pub const SPACE_MARKER: &str = "space";


/// URI for a permissioned space.
///
/// Shape: `at://{authority}/space/{spaceType}/{skey}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceUri {
    /// The DID the space is anchored on (its owner / authority).
    pub authority: String,
    /// The NSID of the space type (e.g. `my.bulletin.board`).
    pub space_type: String,
    /// The space key differentiating spaces of the same type under one owner.
    pub skey: String,
}


impl SpaceUri {

    fn new(authority: &str, space_type: &str, skey: &str) -> Self {
        SpaceUri {
            authority: authority.to_string(),
            space_type: space_type.to_string(),
            skey: skey.to_string(),
        }
    }

    /// Parses a space URI string into a `SpaceUri`.
    ///
    /// Example: `at://{authority}/space/{spaceType}/{skey}`.
    pub fn from_string(uri: &str) -> Option<Self> {
        if !uri.starts_with("at://") {
            return None;
        }
        // Parts: ["at:", "", authority, "space", spaceType, skey]
        let parts: Vec<&str> = uri.split('/').collect();
        if parts.len() != 6 || parts[3] != SPACE_MARKER {
            return None;
        }
        let authority = parts[2];
        let space_type = parts[4];
        let skey = parts[5];

        if authority.is_empty() || space_type.is_empty() || skey.is_empty() || !Self::is_valid_nsid(space_type) || !Self::is_valid_skey(skey) {
            return None;
        }

        Some(SpaceUri::new(authority, space_type, skey))
    }

    /// Converts the `SpaceUri` back into a space URI string.
    pub fn to_string(&self) -> String {
        format!(
            "at://{}/{}/{}/{}",
            self.authority, SPACE_MARKER, self.space_type, self.skey
        )
    }

    /// Validate that a string is a plausible NSID (dotted, non-empty segments).
    pub fn is_valid_nsid(nsid: &str) -> bool {
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
    pub fn is_valid_skey(skey: &str) -> bool {
        if skey.is_empty() || skey.len() > 512 {
            return false;
        }
        if skey == "." || skey == ".." {
            return false;
        }
        skey.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':' | '~'))
    }

}


/// URI for a record within a permissioned space.
///
/// Shape: `at://{authority}/space/{spaceType}/{skey}/{repoDid}/{collection}/{rkey}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRecordUri {
    /// The DID the space is anchored on (its owner / authority).
    pub authority: String,
    /// The NSID of the space type (e.g. `my.bulletin.board`).
    pub space_type: String,
    /// The space key differentiating spaces of the same type under one owner.
    pub skey: String,
    /// The author (repo) DID the record was written to.
    pub repo_did: String,
    /// Collection name (NSID).
    pub collection: String,
    /// Record key.
    pub rkey: String,
}

impl SpaceRecordUri {
    pub fn new(
        authority: &str,
        space_type: &str,
        skey: &str,
        repo_did: &str,
        collection: &str,
        rkey: &str,
    ) -> Self {
        SpaceRecordUri {
            authority: authority.to_string(),
            space_type: space_type.to_string(),
            skey: skey.to_string(),
            repo_did: repo_did.to_string(),
            collection: collection.to_string(),
            rkey: rkey.to_string(),
        }
    }

    /// Builds a space record URI from a space reference plus the author (repo)
    /// DID, collection and record key.
    pub fn from_space_uri(
        space: &SpaceUri,
        repo_did: &str,
        collection: &str,
        rkey: &str,
    ) -> Self {
        SpaceRecordUri::new(
            &space.authority,
            &space.space_type,
            &space.skey,
            repo_did,
            collection,
            rkey,
        )
    }

    /// Parses a full space record URI string into a `SpaceRecordUri`.
    ///
    /// Example: `at://{authority}/space/{spaceType}/{skey}/{repoDid}/{collection}/{rkey}`.
    pub fn from_string(uri: &str) -> Option<Self> {
        if !uri.starts_with("at://") {
            return None;
        }
        // Parts: ["at:", "", authority, "space", spaceType, skey, repoDid, collection, rkey]
        let parts: Vec<&str> = uri.split('/').collect();
        if parts.len() != 9 || parts[3] != SPACE_MARKER {
            return None;
        }
        let authority = parts[2];
        let space_type = parts[4];
        let skey = parts[5];
        let repo_did = parts[6];
        let collection = parts[7];
        let rkey = parts[8];
        if authority.is_empty()
            || space_type.is_empty()
            || skey.is_empty()
            || repo_did.is_empty()
            || collection.is_empty()
            || rkey.is_empty()
        {
            return None;
        }
        Some(SpaceRecordUri::new(
            authority, space_type, skey, repo_did, collection, rkey,
        ))
    }

    /// Converts the `SpaceRecordUri` back into a full space record URI string.
    pub fn to_string(&self) -> String {
        format!(
            "at://{}/{}/{}/{}/{}/{}/{}",
            self.authority,
            SPACE_MARKER,
            self.space_type,
            self.skey,
            self.repo_did,
            self.collection,
            self.rkey
        )
    }

    /// Returns the space URI this record belongs to.
    pub fn space_uri(&self) -> SpaceUri {
        SpaceUri::new(&self.authority, &self.space_type, &self.skey)
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_space_uri_roundtrip() {
        let uri = "at://did:web:testuser.rustproto.com/space/my.bulletin.board/self";
        let space = SpaceUri::from_string(uri).unwrap();
        assert_eq!(space.authority, "did:web:testuser.rustproto.com");
        assert_eq!(space.space_type, "my.bulletin.board");
        assert_eq!(space.skey, "self");
        assert_eq!(space.to_string(), uri);
    }


    #[test]
    fn test_space_ref_invalid() {
        // Missing the `space` marker.
        assert!(SpaceUri::from_string("at://did:plc:abc/my.type/self").is_none());
        // Missing skey.
        assert!(SpaceUri::from_string("at://did:plc:abc/space/my.type").is_none());
        // Extra segment.
        assert!(SpaceUri::from_string("at://did:plc:abc/space/my.type/self/extra").is_none());
        // Not an at:// uri.
        assert!(SpaceUri::from_string("did:plc:abc/space/my.type/self").is_none());
    }

    #[test]
    fn test_space_record_uri_roundtrip() {
        let uri = "at://did:web:testuser.rustproto.com/space/my.bulletin.board/self/did:web:testuser.rustproto.com/my.bulletin.post/3mtzj6wohu6yq";
        let record = SpaceRecordUri::from_string(uri).unwrap();
        assert_eq!(record.authority, "did:web:testuser.rustproto.com");
        assert_eq!(record.space_type, "my.bulletin.board");
        assert_eq!(record.skey, "self");
        assert_eq!(record.repo_did, "did:web:testuser.rustproto.com");
        assert_eq!(record.collection, "my.bulletin.post");
        assert_eq!(record.rkey, "3mtzj6wohu6yq");
        assert_eq!(record.to_string(), uri);
    }

    #[test]
    fn test_space_record_uri_from_space_ref() {
        let space =
            SpaceUri::from_string("at://did:plc:abc/space/my.bulletin.board/self").unwrap();
        let record =
            SpaceRecordUri::from_space_uri(&space, "did:plc:abc", "my.bulletin.post", "rkey1");
        assert_eq!(
            record.to_string(),
            "at://did:plc:abc/space/my.bulletin.board/self/did:plc:abc/my.bulletin.post/rkey1"
        );
        assert_eq!(record.space_uri(), space);
    }

    #[test]
    fn test_space_record_uri_invalid() {
        // Space reference only (missing repoDid/collection/rkey).
        assert!(SpaceRecordUri::from_string("at://did:plc:abc/space/my.type/self").is_none());
        // Missing rkey.
        assert!(SpaceRecordUri::from_string(
            "at://did:plc:abc/space/my.type/self/did:plc:abc/my.bulletin.post"
        )
        .is_none());
        // Missing the `space` marker.
        assert!(SpaceRecordUri::from_string(
            "at://did:plc:abc/notspace/my.type/self/did:plc:abc/my.bulletin.post/rkey1"
        )
        .is_none());
    }

    #[test]
    fn validates_nsid() {
        assert!(SpaceUri::is_valid_nsid("my.bulletin.board"));
        assert!(SpaceUri::is_valid_nsid("app.bsky.group"));
        assert!(!SpaceUri::is_valid_nsid("board"));
        assert!(!SpaceUri::is_valid_nsid(""));
        assert!(!SpaceUri::is_valid_nsid("my..board"));
        assert!(!SpaceUri::is_valid_nsid("my.1bad.board"));
    }

    #[test]
    fn validates_skey() {
        assert!(SpaceUri::is_valid_skey("self"));
        assert!(SpaceUri::is_valid_skey("3kabc"));
        assert!(!SpaceUri::is_valid_skey(""));
        assert!(!SpaceUri::is_valid_skey("."));
        assert!(!SpaceUri::is_valid_skey(".."));
        assert!(!SpaceUri::is_valid_skey("has/slash"));
    }

}

