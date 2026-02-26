//! MST Entry - represents an entry within an MST node.
//!
//! Each entry contains a key/value pair and optionally a right subtree.

use super::mst_node::MstNode;

/// Represents an entry in an MST node.
#[derive(Debug, Clone)]
pub struct MstEntry {
    /// The record key
    pub key: String,
    /// The value (typically a CID in base32 format)
    pub value: String,
    /// Optional right subtree
    pub right_tree: Option<Box<MstNode>>,
}

impl MstEntry {
    /// Creates a new MstEntry with no right subtree.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            right_tree: None,
        }
    }

    /// Creates a new MstEntry with a right subtree.
    pub fn with_right_tree(
        key: impl Into<String>,
        value: impl Into<String>,
        right_tree: MstNode,
    ) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            right_tree: Some(Box::new(right_tree)),
        }
    }
}

impl PartialEq for MstEntry {
    fn eq(&self, other: &Self) -> bool {
        if self.key != other.key {
            return false;
        }
        if self.value != other.value {
            return false;
        }
        match (&self.right_tree, &other.right_tree) {
            (None, None) => true,
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for MstEntry {}

impl std::hash::Hash for MstEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.value.hash(state);
        // Note: right_tree is not hashed to avoid infinite recursion concerns
    }
}
