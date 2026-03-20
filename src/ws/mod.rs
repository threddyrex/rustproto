//! Web services module for AT Protocol / Bluesky interactions.
//!
//! This module provides functionality to resolve actor information,
//! including handle-to-DID resolution, DID document retrieval, and PDS lookup.

mod actor_info;
mod actor_query_options;
mod bluesky_client;

pub use actor_info::ActorInfo;
pub use actor_query_options::ActorQueryOptions;
pub use bluesky_client::{BlueskyClient, BlueskyClientError, DEFAULT_APP_VIEW_HOST_NAME};

