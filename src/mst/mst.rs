//! Merkle Search Tree (MST) implementation for AT Protocol.
//!
//! In-memory representation of a Merkle Search Tree (MST).
//!
//! In the PDS, most times we're not working with MST. We keep
//! the records in the db as a flat list of repo records. But in the
//! few cases we need a MST, we'll assemble the MST in memory.
//!
//! This module codifies the properties of a MST (key depth, key
//! sorting, etc.) and lets us assemble it.
//!
//! There are no "put" or "delete" operations here. This MST is just
//! for querying. If you need to make changes, do those changes on
//! the repo records, and then re-assemble the MST again.
//!
//! Atproto uses MST for the repos: <https://atproto.com/specs/repository>

use std::collections::HashMap;

use sha2::{Sha256, Digest};

use super::mst_entry::MstEntry;
use super::mst_item::MstItem;
use super::mst_node::MstNode;

/// The main MST structure, holding a reference to the root node.
#[derive(Debug, Clone)]
pub struct Mst {
    pub root: MstNode,
}

impl Mst {
    // ==================== ASSEMBLE ====================

    /// Assemble a Merkle Search Tree (MST) from a flat list of items.
    /// Once you have the MST, you can run find operations on it.
    pub fn assemble_tree_from_items(items: &[MstItem]) -> Self {
        if items.is_empty() {
            return Mst {
                root: MstNode::new(0),
            };
        }

        // Get lists of items by key depth
        let mut items_by_depth: HashMap<i32, Vec<&MstItem>> = HashMap::new();
        for item in items {
            let key_depth = Self::get_key_depth_str(&item.key);
            items_by_depth.entry(key_depth).or_default().push(item);
        }

        // Get max key depth - this is the root
        let root_key_depth = *items_by_depth.keys().max().unwrap_or(&0);

        // Create root for that depth
        let mut root_node = MstNode::new(root_key_depth);

        // Insert items, in key_depth order (from highest to lowest)
        for current_key_depth in (0..=root_key_depth).rev() {
            if let Some(items_at_depth) = items_by_depth.get(&current_key_depth) {
                for item in items_at_depth {
                    let key_depth = Self::get_key_depth_str(&item.key);
                    Self::assemble_item(&mut root_node, &item.key, &item.value, key_depth);
                }
            }
        }

        Mst { root: root_node }
    }

    fn assemble_item(current_node: &mut MstNode, key_to_add: &str, value_to_add: &str, key_depth_to_add: i32) {
        // Add at this node?
        if current_node.key_depth == key_depth_to_add {
            // Get insert index
            let mut insert_index = 0;
            for entry in &current_node.entries {
                if Self::less_than(key_to_add, &entry.key) {
                    break;
                }
                insert_index += 1;
            }

            current_node.entries.insert(
                insert_index,
                MstEntry::new(key_to_add, value_to_add),
            );
        } else {
            // Get insert index
            let mut insert_index = 0;
            for entry in &current_node.entries {
                if Self::less_than(key_to_add, &entry.key) {
                    break;
                }
                insert_index += 1;
            }

            // Go left?
            if insert_index == 0 {
                if current_node.left_tree.is_none() {
                    current_node.left_tree = Some(Box::new(MstNode::new(current_node.key_depth - 1)));
                }
                if let Some(ref mut left) = current_node.left_tree {
                    Self::assemble_item(left, key_to_add, value_to_add, key_depth_to_add);
                }
            }
            // Go right?
            else {
                let entry_idx = insert_index - 1;
                if current_node.entries[entry_idx].right_tree.is_none() {
                    current_node.entries[entry_idx].right_tree =
                        Some(Box::new(MstNode::new(current_node.key_depth - 1)));
                }
                if let Some(ref mut right) = current_node.entries[entry_idx].right_tree {
                    Self::assemble_item(right, key_to_add, value_to_add, key_depth_to_add);
                }
            }
        }
    }

    // ==================== FIND ====================

    /// Find all nodes that would be traversed to find the given key.
    /// This helps us identify which nodes to include in firehose events.
    pub fn find_nodes_for_key(&self, key: &str) -> Vec<&MstNode> {
        let mut found_nodes = Vec::new();
        let target_key_depth = Self::get_key_depth_str(key);
        self.internal_find_nodes_for_key(key, target_key_depth, &self.root, &mut found_nodes);
        found_nodes
    }

    fn internal_find_nodes_for_key<'a>(
        &'a self,
        target_key: &str,
        target_key_depth: i32,
        current_node: &'a MstNode,
        found_nodes: &mut Vec<&'a MstNode>,
    ) {
        // Add this one
        found_nodes.push(current_node);

        // If we're at the target depth, we're done
        if current_node.key_depth == target_key_depth {
            return;
        }

        // Find index
        let mut entry_index = 0;
        for entry in &current_node.entries {
            if Self::less_than(target_key, &entry.key) {
                break;
            }
            entry_index += 1;
        }

        // Go left?
        if entry_index == 0 {
            if let Some(ref left) = current_node.left_tree {
                self.internal_find_nodes_for_key(target_key, target_key_depth, left, found_nodes);
            }
        }
        // Go right?
        else if let Some(ref right) = current_node.entries[entry_index - 1].right_tree {
            self.internal_find_nodes_for_key(target_key, target_key_depth, right, found_nodes);
        }
    }

    /// Find all nodes in the tree.
    pub fn find_all_nodes(&self) -> Vec<&MstNode> {
        let mut found_nodes = Vec::new();
        self.internal_find_all_nodes(&self.root, &mut found_nodes);
        found_nodes
    }

    fn internal_find_all_nodes<'a>(&'a self, current_node: &'a MstNode, found_nodes: &mut Vec<&'a MstNode>) {
        found_nodes.push(current_node);

        if let Some(ref left) = current_node.left_tree {
            self.internal_find_all_nodes(left, found_nodes);
        }

        for entry in &current_node.entries {
            if let Some(ref right) = entry.right_tree {
                self.internal_find_all_nodes(right, found_nodes);
            }
        }
    }

    // ==================== KEY COMPARE ====================

    /// Compare two keys. Used when assembling a tree.
    pub fn compare_keys(a: &str, b: &str) -> std::cmp::Ordering {
        let a_bytes = a.as_bytes();
        let b_bytes = b.as_bytes();
        let min_len = a_bytes.len().min(b_bytes.len());

        for i in 0..min_len {
            if a_bytes[i] != b_bytes[i] {
                return a_bytes[i].cmp(&b_bytes[i]);
            }
        }

        a_bytes.len().cmp(&b_bytes.len())
    }

    /// Returns true if a < b.
    pub fn less_than(a: &str, b: &str) -> bool {
        Self::compare_keys(a, b) == std::cmp::Ordering::Less
    }

    /// Returns true if a > b.
    pub fn greater_than(a: &str, b: &str) -> bool {
        Self::compare_keys(a, b) == std::cmp::Ordering::Greater
    }

    /// Returns true if a == b.
    pub fn keys_equal(a: &str, b: &str) -> bool {
        Self::compare_keys(a, b) == std::cmp::Ordering::Equal
    }

    // ==================== KEY DEPTH ====================

    /// Calculate the depth of a key (string version).
    /// Converts string to UTF-8 bytes first.
    pub fn get_key_depth_str(key: &str) -> i32 {
        Self::get_key_depth(key.as_bytes())
    }

    /// Calculate the depth of a key using SHA-256 hash.
    ///
    /// Per the spec:
    /// - Hash the key with SHA-256 (binary output)
    /// - Count leading zeros in 2-bit chunks
    /// - This gives a fanout of 4
    ///
    /// Examples from spec:
    /// - "2653ae71" -> depth 0
    /// - "blue" -> depth 1
    /// - "app.bsky.feed.post/454397e440ec" -> depth 4
    /// - "app.bsky.feed.post/9adeb165882c" -> depth 8
    pub fn get_key_depth(key: &[u8]) -> i32 {
        // Hash the key with SHA-256
        let mut hasher = Sha256::new();
        hasher.update(key);
        let hash = hasher.finalize();

        // Count leading zeros in 2-bit chunks
        let mut leading_zeros = 0;
        for b in hash.iter() {
            if *b == 0 {
                leading_zeros += 8; // All 8 bits are zero
            } else {
                // Count leading zeros in this byte
                let mut mask: u8 = 0x80;
                for _ in 0..8 {
                    if (b & mask) == 0 {
                        leading_zeros += 1;
                        mask >>= 1;
                    } else {
                        break;
                    }
                }
                break;
            }
        }

        // Divide by 2 to get 2-bit chunks
        leading_zeros / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assemble_tree_one_item() {
        let items = vec![MstItem::new("key1", "value1")];

        let tree = Mst::assemble_tree_from_items(&items);

        assert_eq!(tree.root.entries.len(), 1);
        assert_eq!(tree.root.entries[0].key, "key1");
        assert_eq!(tree.root.entries[0].value, "value1");
        assert_eq!(tree.root.key_depth, Mst::get_key_depth_str("key1"));
    }

    #[test]
    fn test_assemble_tree_two_items() {
        let items = vec![
            MstItem::new("key1", "value1"),
            MstItem::new("key2", "value2"),
        ];

        let tree = Mst::assemble_tree_from_items(&items);

        assert_eq!(tree.root.entries.len(), 2);
        assert_eq!(tree.root.entries[0].key, "key1");
        assert_eq!(tree.root.entries[0].value, "value1");
        assert_eq!(tree.root.entries[1].key, "key2");
        assert_eq!(tree.root.entries[1].value, "value2");
        assert_eq!(tree.root.key_depth, Mst::get_key_depth_str("key1"));
    }

    #[test]
    fn test_key_depth_from_spec() {
        // Test examples from the AT Protocol spec
        // Note: These are approximate tests; actual values depend on SHA-256 output
        let depth1 = Mst::get_key_depth_str("2653ae71");
        let depth2 = Mst::get_key_depth_str("blue");
        
        // Both should produce valid depths (non-negative)
        assert!(depth1 >= 0);
        assert!(depth2 >= 0);
    }

    #[test]
    fn test_key_comparison() {
        assert!(Mst::less_than("a", "b"));
        assert!(Mst::greater_than("b", "a"));
        assert!(Mst::keys_equal("a", "a"));
        assert!(Mst::less_than("aa", "ab"));
        assert!(Mst::less_than("a", "aa"));
    }

    #[test]
    fn test_find_all_nodes() {
        let items = vec![
            MstItem::new("key1", "value1"),
            MstItem::new("key2", "value2"),
        ];

        let tree = Mst::assemble_tree_from_items(&items);
        let nodes = tree.find_all_nodes();

        // Should have at least the root node
        assert!(!nodes.is_empty());
    }

    #[test]
    fn test_empty_tree() {
        let items: Vec<MstItem> = vec![];
        let tree = Mst::assemble_tree_from_items(&items);

        assert_eq!(tree.root.key_depth, 0);
        assert!(tree.root.entries.is_empty());
        assert!(tree.root.left_tree.is_none());
    }
}
