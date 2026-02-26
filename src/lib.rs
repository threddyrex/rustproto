//! rstproto - Rust AT Protocol / Bluesky SDK
//!
//! This crate provides utilities for working with the AT Protocol and Bluesky,
//! including actor resolution, identity lookup, and related functionality.
//!
//! # Example
//!
//! ```no_run
//! use rstproto::ws::{BlueskyClient, ActorQueryOptions};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = BlueskyClient::new();
//!
//!     // Resolve actor info using default options
//!     let info = client.resolve_actor_info("alice.bsky.social", None).await.unwrap();
//!     println!("DID: {:?}", info.did);
//!     println!("PDS: {:?}", info.pds);
//!
//!     // Resolve with all methods enabled
//!     let info = client.resolve_actor_info(
//!         "bob.bsky.social",
//!         Some(ActorQueryOptions::all())
//!     ).await.unwrap();
//! }
//! ```

pub mod ws;

// Re-export commonly used types at crate root
pub use ws::{ActorInfo, ActorQueryOptions, BlueskyClient, BlueskyClientError};
