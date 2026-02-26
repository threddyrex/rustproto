//! Actor information struct representing resolved identity data.

use serde::{Deserialize, Serialize};

/// Result from resolving actor information.
///
/// Contains the resolved identity data including handle, DID,
/// DID document, PDS endpoint, and public key.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActorInfo {
    /// The original actor string (handle or DID) that was provided.
    #[serde(skip_serializing_if = "Option::is_none", alias = "Actor")]
    pub actor: Option<String>,

    /// The resolved handle (e.g., "alice.bsky.social").
    #[serde(skip_serializing_if = "Option::is_none", alias = "Handle")]
    pub handle: Option<String>,

    /// The resolved DID (preferred resolution).
    #[serde(skip_serializing_if = "Option::is_none", alias = "Did")]
    pub did: Option<String>,

    /// DID resolved via Bluesky API.
    #[serde(skip_serializing_if = "Option::is_none", alias = "Did_Bsky")]
    pub did_bsky: Option<String>,

    /// DID resolved via HTTP (.well-known/atproto-did).
    #[serde(skip_serializing_if = "Option::is_none", alias = "Did_Http")]
    pub did_http: Option<String>,

    /// DID resolved via DNS TXT record.
    #[serde(skip_serializing_if = "Option::is_none", alias = "Did_Dns")]
    pub did_dns: Option<String>,

    /// The full DID document as a JSON string.
    #[serde(skip_serializing_if = "Option::is_none", alias = "DidDoc")]
    pub did_doc: Option<String>,

    /// The PDS (Personal Data Server) endpoint hostname.
    #[serde(skip_serializing_if = "Option::is_none", alias = "Pds")]
    pub pds: Option<String>,

    /// The public key in multibase format from the DID document.
    #[serde(skip_serializing_if = "Option::is_none", alias = "PublicKeyMultibase")]
    pub public_key_multibase: Option<String>,
}

impl ActorInfo {
    /// Creates a new empty ActorInfo.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new ActorInfo with the given actor string.
    pub fn with_actor(actor: impl Into<String>) -> Self {
        Self {
            actor: Some(actor.into()),
            ..Default::default()
        }
    }

    /// Serializes the ActorInfo to a JSON string.
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Deserializes an ActorInfo from a JSON string.
    pub fn from_json_string(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Returns true if a DID was successfully resolved.
    pub fn has_did(&self) -> bool {
        self.did.as_ref().map_or(false, |d| d.starts_with("did:"))
    }

    /// Returns true if a PDS endpoint was resolved.
    pub fn has_pds(&self) -> bool {
        self.pds.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_info_serialization() {
        let info = ActorInfo {
            actor: Some("alice.bsky.social".to_string()),
            did: Some("did:plc:abc123".to_string()),
            pds: Some("bsky.social".to_string()),
            ..Default::default()
        };

        let json = info.to_json_string().unwrap();
        let parsed = ActorInfo::from_json_string(&json).unwrap();

        assert_eq!(parsed.actor, info.actor);
        assert_eq!(parsed.did, info.did);
        assert_eq!(parsed.pds, info.pds);
    }

    #[test]
    fn test_has_did() {
        let mut info = ActorInfo::new();
        assert!(!info.has_did());

        info.did = Some("did:plc:abc123".to_string());
        assert!(info.has_did());
    }
}
