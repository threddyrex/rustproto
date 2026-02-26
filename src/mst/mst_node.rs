//! MST Node - represents a node in the Merkle Search Tree.
//!
//! Each node has a key depth and contains entries that may have subtrees.

use super::mst_entry::MstEntry;

/// Represents a node in the MST.
#[derive(Debug, Clone)]
pub struct MstNode {
    /// The key depth of this node in the tree
    pub key_depth: i32,
    /// Optional left subtree
    pub left_tree: Option<Box<MstNode>>,
    /// Entries in this node (sorted by key)
    pub entries: Vec<MstEntry>,
}

impl MstNode {
    /// Creates a new empty MstNode at the given key depth.
    pub fn new(key_depth: i32) -> Self {
        Self {
            key_depth,
            left_tree: None,
            entries: Vec::new(),
        }
    }

    /// Creates a new MstNode with entries.
    pub fn with_entries(key_depth: i32, entries: Vec<MstEntry>) -> Self {
        Self {
            key_depth,
            left_tree: None,
            entries,
        }
    }

    /// Sets the left subtree.
    pub fn set_left_tree(&mut self, left: MstNode) {
        self.left_tree = Some(Box::new(left));
    }
}

impl PartialEq for MstNode {
    fn eq(&self, other: &Self) -> bool {
        if self.key_depth != other.key_depth {
            return false;
        }

        match (&self.left_tree, &other.left_tree) {
            (None, None) => {}
            (Some(a), Some(b)) if a == b => {}
            _ => return false,
        }

        if self.entries.len() != other.entries.len() {
            return false;
        }

        for (a, b) in self.entries.iter().zip(other.entries.iter()) {
            if a != b {
                return false;
            }
        }

        true
    }
}

impl Eq for MstNode {}

impl std::hash::Hash for MstNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key_depth.hash(state);
        // Note: left_tree and entries are not fully hashed to avoid complexity
        self.entries.len().hash(state);
    }
}
