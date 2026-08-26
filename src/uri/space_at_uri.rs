//! interacting with permissioned-space at:// uris
//!
//! Permissioned spaces use two related URI shapes:
//!
//! * A **space reference** identifies a space itself:
//!   `at://{authority}/space/{spaceType}/{skey}`
//! * A **space record URI** identifies a single record written to a member's
//!   permissioned repo within a space:
//!   `at://{authority}/space/{spaceType}/{skey}/{repoDid}/{collection}/{rkey}`
//!
//! The record URI is the space reference with the author (repo) DID, collection
//! and record key appended. Space clients parse records back out of this shape,
//! so the author-DID segment between the space reference and the collection is
//! required.

/// The fixed marker segment identifying a permissioned-space URI
/// (`at://{authority}/space/...`).
const SPACE_MARKER: &str = "space";

/// A reference to a permissioned space.
///
/// Shape: `at://{authority}/space/{spaceType}/{skey}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRef {
    /// The DID the space is anchored on (its owner / authority).
    pub authority: String,
    /// The NSID of the space type (e.g. `my.bulletin.board`).
    pub space_type: String,
    /// The space key differentiating spaces of the same type under one owner.
    pub skey: String,
}

impl SpaceRef {
    pub fn new(authority: &str, space_type: &str, skey: &str) -> Self {
        SpaceRef {
            authority: authority.to_string(),
            space_type: space_type.to_string(),
            skey: skey.to_string(),
        }
    }

    /// Parses a space reference string into a `SpaceRef`.
    ///
    /// Example: `at://{authority}/space/{spaceType}/{skey}`.
    pub fn from_space_ref(uri: &str) -> Option<Self> {
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
        if authority.is_empty() || space_type.is_empty() || skey.is_empty() {
            return None;
        }
        Some(SpaceRef::new(authority, space_type, skey))
    }

    /// Converts the `SpaceRef` back into a space reference string.
    pub fn to_space_ref(&self) -> String {
        format!(
            "at://{}/{}/{}/{}",
            self.authority, SPACE_MARKER, self.space_type, self.skey
        )
    }
}

/// A record within a permissioned space.
///
/// Shape: `at://{authority}/space/{spaceType}/{skey}/{repoDid}/{collection}/{rkey}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceAtUri {
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

impl SpaceAtUri {
    pub fn new(
        authority: &str,
        space_type: &str,
        skey: &str,
        repo_did: &str,
        collection: &str,
        rkey: &str,
    ) -> Self {
        SpaceAtUri {
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
    pub fn from_space_ref(
        space: &SpaceRef,
        repo_did: &str,
        collection: &str,
        rkey: &str,
    ) -> Self {
        SpaceAtUri::new(
            &space.authority,
            &space.space_type,
            &space.skey,
            repo_did,
            collection,
            rkey,
        )
    }

    /// Parses a full space record URI string into a `SpaceAtUri`.
    ///
    /// Example: `at://{authority}/space/{spaceType}/{skey}/{repoDid}/{collection}/{rkey}`.
    pub fn from_space_at_uri(uri: &str) -> Option<Self> {
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
        Some(SpaceAtUri::new(
            authority, space_type, skey, repo_did, collection, rkey,
        ))
    }

    /// Converts the `SpaceAtUri` back into a full space record URI string.
    pub fn to_space_at_uri(&self) -> String {
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

    /// Returns the space reference this record belongs to.
    pub fn space_ref(&self) -> SpaceRef {
        SpaceRef::new(&self.authority, &self.space_type, &self.skey)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_space_ref_roundtrip() {
        let uri = "at://did:web:testuser.rustproto.com/space/my.bulletin.board/self";
        let space = SpaceRef::from_space_ref(uri).unwrap();
        assert_eq!(space.authority, "did:web:testuser.rustproto.com");
        assert_eq!(space.space_type, "my.bulletin.board");
        assert_eq!(space.skey, "self");
        assert_eq!(space.to_space_ref(), uri);
    }

    #[test]
    fn test_space_ref_invalid() {
        // Missing the `space` marker.
        assert!(SpaceRef::from_space_ref("at://did:plc:abc/my.type/self").is_none());
        // Missing skey.
        assert!(SpaceRef::from_space_ref("at://did:plc:abc/space/my.type").is_none());
        // Extra segment.
        assert!(SpaceRef::from_space_ref("at://did:plc:abc/space/my.type/self/extra").is_none());
        // Not an at:// uri.
        assert!(SpaceRef::from_space_ref("did:plc:abc/space/my.type/self").is_none());
    }

    #[test]
    fn test_space_at_uri_roundtrip() {
        let uri = "at://did:web:testuser.rustproto.com/space/my.bulletin.board/self/did:web:testuser.rustproto.com/my.bulletin.post/3mtzj6wohu6yq";
        let record = SpaceAtUri::from_space_at_uri(uri).unwrap();
        assert_eq!(record.authority, "did:web:testuser.rustproto.com");
        assert_eq!(record.space_type, "my.bulletin.board");
        assert_eq!(record.skey, "self");
        assert_eq!(record.repo_did, "did:web:testuser.rustproto.com");
        assert_eq!(record.collection, "my.bulletin.post");
        assert_eq!(record.rkey, "3mtzj6wohu6yq");
        assert_eq!(record.to_space_at_uri(), uri);
    }

    #[test]
    fn test_space_at_uri_from_space_ref() {
        let space =
            SpaceRef::from_space_ref("at://did:plc:abc/space/my.bulletin.board/self").unwrap();
        let record =
            SpaceAtUri::from_space_ref(&space, "did:plc:abc", "my.bulletin.post", "rkey1");
        assert_eq!(
            record.to_space_at_uri(),
            "at://did:plc:abc/space/my.bulletin.board/self/did:plc:abc/my.bulletin.post/rkey1"
        );
        assert_eq!(record.space_ref(), space);
    }

    #[test]
    fn test_space_at_uri_invalid() {
        // Space reference only (missing repoDid/collection/rkey).
        assert!(SpaceAtUri::from_space_at_uri("at://did:plc:abc/space/my.type/self").is_none());
        // Missing rkey.
        assert!(SpaceAtUri::from_space_at_uri(
            "at://did:plc:abc/space/my.type/self/did:plc:abc/my.bulletin.post"
        )
        .is_none());
        // Missing the `space` marker.
        assert!(SpaceAtUri::from_space_at_uri(
            "at://did:plc:abc/notspace/my.type/self/did:plc:abc/my.bulletin.post/rkey1"
        )
        .is_none());
    }
}
