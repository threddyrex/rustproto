//! Helper functions for working with Merkle Search Trees (MST) in AT Protocol repos.
//!
//! The "mst" module has the core MST in-memory structures and functions,
//! but it doesn't know about the rest of the modules in this crate (repo, log, etc.)
//! This module helps transform the MST into things that atproto needs.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use crate::log::Logger;
use crate::mst::{Mst, MstEntry, MstItem, MstNode};
use super::{CidV1, DagCborObject, DagCborValue, Repo};

/// Helper functions for working with MST in repos.
pub struct RepoMst;

impl RepoMst {
    // ==================== LOAD REPO ====================

    /// Load a CAR repo file and extract the MST items from it.
    pub fn load_mst_items_from_repo_file<P: AsRef<Path>>(
        repo_file: P,
        _logger: &Logger,
    ) -> io::Result<Vec<MstItem>> {
        let file = File::open(repo_file)?;
        Self::load_mst_items_from_repo(file, _logger)
    }

    /// Load a CAR repo stream and extract the MST items from it.
    pub fn load_mst_items_from_repo<R: Read>(
        reader: R,
        _logger: &Logger,
    ) -> io::Result<Vec<MstItem>> {
        let mut mst_items = Vec::new();

        Repo::walk_repo(
            reader,
            |_header| true,
            |record| {
                if Self::is_mst_node(record) {
                    // Get entries array
                    if let Some(entries_obj) = record.data_block.select_array(&["e"]) {
                        let mut full_keys: Vec<String> = Vec::new();

                        for (i, entry) in entries_obj.iter().enumerate() {
                            // "p" - prefix length
                            let prefix_length = entry.select_int(&["p"]).unwrap_or(0) as usize;

                            // "k" - key suffix (as bytes)
                            let key_suffix = if let Some(key_bytes) = entry.select_bytes(&["k"]) {
                                String::from_utf8_lossy(key_bytes).to_string()
                            } else {
                                continue;
                            };

                            // "v" - record CID
                            let cid = match entry.select_cid(&["v"]) {
                                Some(c) => c,
                                None => continue,
                            };

                            // Reconstruct full key
                            let full_key = if i == 0 {
                                key_suffix.clone()
                            } else {
                                let prev_key = &full_keys[i - 1];
                                let prefix = &prev_key[..prefix_length.min(prev_key.len())];
                                format!("{}{}", prefix, key_suffix)
                            };

                            full_keys.push(full_key.clone());

                            mst_items.push(MstItem::new(full_key, cid.get_base32()));
                        }
                    }
                }

                true
            },
        )?;

        Ok(mst_items)
    }

    // ==================== DAG CBOR ====================

    /// Convert the entire MST to DAG-CBOR objects and cache them.
    ///
    /// Returns a map from node references to (CID, DagCborObject) tuples.
    pub fn convert_mst_to_dag_cbor(mst: &Mst) -> io::Result<HashMap<MstNodeKey, (CidV1, DagCborObject)>> {
        let mut cache = HashMap::new();
        Self::convert_mst_node_to_dag_cbor(&mut cache, &mst.root)?;
        Ok(cache)
    }

    /// Convert a single MST node to DAG-CBOR, recursively converting children.
    ///
    /// Uses the cache to avoid re-creating nodes.
    pub fn convert_mst_node_to_dag_cbor(
        cache: &mut HashMap<MstNodeKey, (CidV1, DagCborObject)>,
        node: &MstNode,
    ) -> io::Result<()> {
        let node_key = MstNodeKey::from_node(node);

        // If already cached, skip
        if cache.contains_key(&node_key) {
            return Ok(());
        }

        // Create empty map for this node
        let mut node_map: HashMap<String, DagCborObject> = HashMap::new();

        // Add left link - always include "l" key (null if no left tree)
        // This matches dnproto's serialization for consistent CIDs
        if let Some(ref left) = node.left_tree {
            Self::convert_mst_node_to_dag_cbor(cache, left)?;
            let left_key = MstNodeKey::from_node(left);
            let (left_cid, _) = cache.get(&left_key).unwrap();

            node_map.insert(
                "l".to_string(),
                DagCborObject::new_cid(left_cid.clone()),
            );
        } else {
            node_map.insert(
                "l".to_string(),
                DagCborObject::new_null(),
            );
        }

        // Build entries array
        let mut entries_array: Vec<DagCborObject> = Vec::new();
        let prefix_lengths = Self::get_prefix_lengths(&node.entries);
        let key_suffixes = Self::get_key_suffixes(&node.entries);

        for (i, entry) in node.entries.iter().enumerate() {
            let mut entry_map: HashMap<String, DagCborObject> = HashMap::new();

            // "p" - prefix length
            entry_map.insert(
                "p".to_string(),
                DagCborObject::new_unsigned_int(prefix_lengths[i] as i64),
            );

            // "k" - key suffix (byte string)
            entry_map.insert(
                "k".to_string(),
                DagCborObject::new_byte_string(key_suffixes[i].as_bytes().to_vec()),
            );

            // "v" - value CID
            let value_cid = CidV1::from_base32(&entry.value)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            entry_map.insert("v".to_string(), DagCborObject::new_cid(value_cid));

            // "t" - tree CID - always include (null if no right tree)
            // This matches dnproto's serialization for consistent CIDs
            if let Some(ref right) = entry.right_tree {
                Self::convert_mst_node_to_dag_cbor(cache, right)?;
                let right_key = MstNodeKey::from_node(right);
                let (right_cid, _) = cache.get(&right_key).unwrap();

                entry_map.insert(
                    "t".to_string(),
                    DagCborObject::new_cid(right_cid.clone()),
                );
            } else {
                entry_map.insert(
                    "t".to_string(),
                    DagCborObject::new_null(),
                );
            }

            entries_array.push(DagCborObject::new_map(entry_map));
        }

        node_map.insert("e".to_string(), DagCborObject::new_array(entries_array));

        // Make enclosing MAP object
        let node_obj = DagCborObject::new_map(node_map);

        // Compute CID and cache
        let cid = CidV1::compute_cid_for_dag_cbor(&node_obj)?;
        cache.insert(node_key, (cid, node_obj));

        Ok(())
    }

    // ==================== ENTRIES ====================

    /// Check if a repo record is an MST node.
    pub fn is_mst_node(record: &super::RepoRecord) -> bool {
        if let DagCborValue::Map(ref map) = record.data_block.value {
            map.contains_key("e")
        } else {
            false
        }
    }

    /// Get prefix lengths for a list of entries.
    pub fn get_prefix_lengths(entries: &[MstEntry]) -> Vec<usize> {
        let mut prefix_lengths = Vec::new();
        let mut previous_full_key = String::new();

        for (i, entry) in entries.iter().enumerate() {
            if i == 0 {
                prefix_lengths.push(0);
                previous_full_key = entry.key.clone();
            } else {
                let prefix_len = Self::get_common_prefix_length(&previous_full_key, &entry.key);
                prefix_lengths.push(prefix_len);
                previous_full_key = entry.key.clone();
            }
        }

        prefix_lengths
    }

    /// Get key suffixes for a list of entries.
    pub fn get_key_suffixes(entries: &[MstEntry]) -> Vec<String> {
        let mut key_suffixes = Vec::new();
        let mut previous_full_key = String::new();

        for (i, entry) in entries.iter().enumerate() {
            if i == 0 {
                key_suffixes.push(entry.key.clone());
                previous_full_key = entry.key.clone();
            } else {
                let prefix_len = Self::get_common_prefix_length(&previous_full_key, &entry.key);
                key_suffixes.push(entry.key[prefix_len..].to_string());
                previous_full_key = entry.key.clone();
            }
        }

        key_suffixes
    }

    /// Get the length of the common prefix between two keys.
    pub fn get_common_prefix_length(a: &str, b: &str) -> usize {
        let a_bytes = a.as_bytes();
        let b_bytes = b.as_bytes();
        let min_len = a_bytes.len().min(b_bytes.len());

        let mut len = 0;
        for i in 0..min_len {
            if a_bytes[i] == b_bytes[i] {
                len += 1;
            } else {
                break;
            }
        }

        len
    }
}

/// A key for identifying MST nodes in the cache.
///
/// This is needed because we can't use references as HashMap keys directly
/// when the nodes are mutable during tree construction.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MstNodeKey {
    /// The key depth of the node
    pub key_depth: i32,
    /// The keys of all entries in this node
    pub entry_keys: Vec<String>,
}

impl MstNodeKey {
    /// Create a key from an MST node.
    pub fn from_node(node: &MstNode) -> Self {
        Self {
            key_depth: node.key_depth,
            entry_keys: node.entries.iter().map(|e| e.key.clone()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_common_prefix_length() {
        assert_eq!(RepoMst::get_common_prefix_length("abc", "abd"), 2);
        assert_eq!(RepoMst::get_common_prefix_length("abc", "abc"), 3);
        assert_eq!(RepoMst::get_common_prefix_length("abc", "xyz"), 0);
        assert_eq!(RepoMst::get_common_prefix_length("", "abc"), 0);
        assert_eq!(RepoMst::get_common_prefix_length("abc", ""), 0);
    }

    #[test]
    fn test_get_prefix_lengths() {
        let entries = vec![
            MstEntry::new("app.bsky.feed.post/abc", "value1"),
            MstEntry::new("app.bsky.feed.post/abd", "value2"),
            MstEntry::new("app.bsky.feed.post/xyz", "value3"),
        ];

        let prefix_lengths = RepoMst::get_prefix_lengths(&entries);

        assert_eq!(prefix_lengths[0], 0); // First entry has no prefix
        assert_eq!(prefix_lengths[1], 21); // "app.bsky.feed.post/ab" = 21 chars
        assert_eq!(prefix_lengths[2], 19); // "app.bsky.feed.post/" = 19 chars
    }

    #[test]
    fn test_get_key_suffixes() {
        let entries = vec![
            MstEntry::new("app.bsky.feed.post/abc", "value1"),
            MstEntry::new("app.bsky.feed.post/abd", "value2"),
        ];

        let suffixes = RepoMst::get_key_suffixes(&entries);

        assert_eq!(suffixes[0], "app.bsky.feed.post/abc"); // Full key for first
        assert_eq!(suffixes[1], "d"); // Just the suffix for second
    }
}
