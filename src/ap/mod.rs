//! ActivityPub client utilities.
//!
//! Provides functionality for resolving ActivityPub actors via WebFinger
//! followed by an actor object fetch.

mod client;
mod models;

pub use client::{parse_account_id_from_actor_url, parse_actor_handle, ApActor, ApClient, ApClientError};
pub use models::{
    ActivityPubActor, ActivityPubPublicKey, ActivityPubPublicKeyField, MastodonAccount,
    WebFingerLink, WebFingerResponse,
};
