//! MST Item - a simple key/value pair for building the MST.
//!
//! This represents a single record in the MST before assembly into the tree structure.

/// A simple key/value item used as input when assembling an MST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MstItem {
    /// The record key (e.g., "app.bsky.feed.post/3abc123")
    pub key: String,
    /// The value (typically a CID in base32 format)
    pub value: String,
}

impl MstItem {
    /// Creates a new MstItem.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}
