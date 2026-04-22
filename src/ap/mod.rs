//! ActivityPub client utilities.
//!
//! Provides functionality for resolving ActivityPub actors via WebFinger
//! followed by an actor object fetch.

mod client;
mod models;

pub use client::{ApActor, ApClient, ApClientError};
pub use models::{
    ActivityPubActor, ActivityPubPublicKey, ActivityPubPublicKeyField, WebFingerLink,
    WebFingerResponse,
};
