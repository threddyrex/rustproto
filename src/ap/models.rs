//! Strongly-typed serde models for ActivityPub actor objects and WebFinger.
//!
//! These types deserialize directly from the JSON returned by WebFinger and
//! ActivityPub actor endpoints. Unknown fields are preserved on each struct
//! via a flattened `extra` map so nothing is silently dropped.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An ActivityPub `publicKey` entry. May appear on an actor as a single object
/// or as an array of objects.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActivityPubPublicKey {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_key_pem: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// `publicKey` may be a single object or an array of objects in the wild.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ActivityPubPublicKeyField {
    One(Box<ActivityPubPublicKey>),
    Many(Vec<ActivityPubPublicKey>),
}

impl ActivityPubPublicKeyField {
    /// Returns the first key, regardless of representation.
    pub fn first(&self) -> Option<&ActivityPubPublicKey> {
        match self {
            ActivityPubPublicKeyField::One(k) => Some(k),
            ActivityPubPublicKeyField::Many(v) => v.first(),
        }
    }
}

/// Typed model of an ActivityPub actor object (`Person`, `Service`, `Group`, ...).
///
/// Only the most commonly used fields are typed explicitly; anything else
/// from the JSON body is preserved in `extra`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ActivityPubActor {
    #[serde(rename = "@context", default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub actor_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_username: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbox: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub followers: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub following: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub featured: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_key: Option<ActivityPubPublicKeyField>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub icon: Option<Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manually_approves_followers: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discoverable: Option<bool>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// Typed model of a WebFinger `links[]` entry.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebFingerLink {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rel: Option<String>,

    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub link_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub href: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// Typed model of a WebFinger JRD response.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebFingerResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<WebFingerLink>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_minimal_actor() {
        let json = r#"{
            "@context": ["https://www.w3.org/ns/activitystreams"],
            "id": "https://example.com/users/alice",
            "type": "Person",
            "preferredUsername": "alice",
            "name": "Alice",
            "inbox": "https://example.com/users/alice/inbox",
            "outbox": "https://example.com/users/alice/outbox",
            "publicKey": {
                "id": "https://example.com/users/alice#main-key",
                "owner": "https://example.com/users/alice",
                "publicKeyPem": "-----BEGIN PUBLIC KEY-----\nABC\n-----END PUBLIC KEY-----"
            },
            "manuallyApprovesFollowers": false,
            "weirdCustomField": 42
        }"#;

        let actor: ActivityPubActor = serde_json::from_str(json).unwrap();
        assert_eq!(actor.id.as_deref(), Some("https://example.com/users/alice"));
        assert_eq!(actor.actor_type.as_deref(), Some("Person"));
        assert_eq!(actor.preferred_username.as_deref(), Some("alice"));
        assert_eq!(actor.manually_approves_followers, Some(false));

        let pk = actor.public_key.as_ref().unwrap().first().unwrap();
        assert_eq!(pk.id.as_deref(), Some("https://example.com/users/alice#main-key"));

        // Unknown field preserved.
        assert!(actor.extra.contains_key("weirdCustomField"));
    }

    #[test]
    fn deserialize_actor_with_publickey_array() {
        let json = r#"{
            "id": "https://example.com/users/bob",
            "publicKey": [
                { "id": "k1", "publicKeyPem": "PEM1" },
                { "id": "k2", "publicKeyPem": "PEM2" }
            ]
        }"#;
        let actor: ActivityPubActor = serde_json::from_str(json).unwrap();
        let pk = actor.public_key.as_ref().unwrap();
        assert_eq!(pk.first().unwrap().id.as_deref(), Some("k1"));
    }

    #[test]
    fn deserialize_webfinger() {
        let json = r#"{
            "subject": "acct:alice@example.com",
            "aliases": ["https://example.com/@alice"],
            "links": [
                { "rel": "self", "type": "application/activity+json", "href": "https://example.com/users/alice" },
                { "rel": "http://webfinger.net/rel/profile-page", "type": "text/html", "href": "https://example.com/@alice" }
            ]
        }"#;
        let wf: WebFingerResponse = serde_json::from_str(json).unwrap();
        assert_eq!(wf.subject.as_deref(), Some("acct:alice@example.com"));
        assert_eq!(wf.links.len(), 2);
        assert_eq!(wf.links[0].rel.as_deref(), Some("self"));
        assert_eq!(wf.links[0].link_type.as_deref(), Some("application/activity+json"));
        assert_eq!(wf.links[0].href.as_deref(), Some("https://example.com/users/alice"));
    }
}
