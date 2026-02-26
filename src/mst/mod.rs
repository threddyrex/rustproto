//! Merkle Search Tree (MST) module for AT Protocol.
//!
//! This module provides types and functions for working with Merkle Search Trees,
//! which are used by the AT Protocol for repositories.
//!
//! # Overview
//!
//! The MST is a deterministic tree structure used to organize records in a repository.
//! Keys are sorted and placed at specific depths based on a SHA-256 hash of the key.
//!
//! # Example
//!
//! ```
//! use rstproto::mst::{Mst, MstItem};
//!
//! // Create items from repo records
//! let items = vec![
//!     MstItem::new("app.bsky.feed.post/abc123", "bafyreiabc..."),
//!     MstItem::new("app.bsky.feed.post/def456", "bafyreidef..."),
//! ];
//!
//! // Assemble the tree
//! let mst = Mst::assemble_tree_from_items(&items);
//!
//! // Find all nodes
//! let nodes = mst.find_all_nodes();
//! println!("Tree has {} nodes", nodes.len());
//! ```

mod mst_item;
mod mst_entry;
mod mst_node;
mod mst;

// Re-exports
pub use mst_item::MstItem;
pub use mst_entry::MstEntry;
pub use mst_node::MstNode;
pub use mst::Mst;
