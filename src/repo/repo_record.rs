//! Repository record.
//!
//! Represents a single record (block) within a CAR file.
//! Each item in a repo (post, like, follow, etc.) is stored as a record.

use std::io::{self, Read, Cursor};

use super::cid::CidV1;
use super::dag_cbor::{DagCborObject, DagCborValue};
use super::varint::VarInt;

/// A record within a repository.
#[derive(Debug, Clone)]
pub struct RepoRecord {
    /// The CID of this record.
    pub cid: CidV1,
    /// The DAG-CBOR data block.
    pub data_block: DagCborObject,
    /// JSON string representation of the data (for display).
    pub json_string: String,
    /// The $type field in an AT Proto record (e.g., "app.bsky.feed.post").
    pub at_proto_type: Option<String>,
    /// The createdAt timestamp if present.
    pub created_at: Option<String>,
    /// Whether there was an error parsing this record.
    pub is_error: bool,
    /// Error message if is_error is true.
    pub error_message: Option<String>,
}

/// Well-known AT Protocol record types.
pub struct AtProtoType;

impl AtProtoType {
    pub const BLUESKY_FOLLOW: &'static str = "app.bsky.graph.follow";
    pub const BLUESKY_LIKE: &'static str = "app.bsky.feed.like";
    pub const BLUESKY_POST: &'static str = "app.bsky.feed.post";
    pub const BLUESKY_REPOST: &'static str = "app.bsky.feed.repost";
    pub const BLUESKY_BLOCK: &'static str = "app.bsky.graph.block";
    pub const FLASHES_POST: &'static str = "blue.flashes.feed.post";
    pub const VERIFICATION: &'static str = "app.bsky.graph.verification";
}

impl RepoRecord {
    /// Reads a RepoRecord from a stream.
    pub fn read_from_stream<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read block length
        let block_length = VarInt::read_varint(reader)?;

        // Read the entire block into a buffer
        let length = block_length.value as usize;
        let mut buffer = vec![0u8; length];
        reader.read_exact(&mut buffer)?;

        let mut cursor = Cursor::new(&buffer);

        // Read CID
        let cid = CidV1::read_cid(&mut cursor)?;

        // Try to read the data block
        let (data_block, is_error, error_message) = match DagCborObject::read_from_stream(&mut cursor) {
            Ok(obj) => (obj, false, None),
            Err(e) => {
                // Create an error placeholder object
                let error_msg = format!("Parse error: {}", e);
                let error_obj = DagCborObject {
                    cbor_type: super::dag_cbor::DagCborType {
                        major_type: super::dag_cbor::DagCborMajorType::Map,
                        additional_info: 0,
                        original_byte: 0,
                    },
                    value: DagCborValue::Map(std::collections::HashMap::new()),
                };
                (error_obj, true, Some(error_msg))
            }
        };

        Self::from_dag_cbor_object(cid, data_block, is_error, error_message)
    }

    /// Creates a RepoRecord from its components.
    fn from_dag_cbor_object(
        cid: CidV1,
        data_block: DagCborObject,
        is_error: bool,
        error_message: Option<String>,
    ) -> io::Result<Self> {
        let at_proto_type = data_block.select_string(&["$type"]);
        let created_at = data_block.select_string(&["createdAt"]);
        let json_string = data_block.to_json_string();

        Ok(RepoRecord {
            cid,
            data_block,
            json_string,
            at_proto_type,
            created_at,
            is_error,
            error_message,
        })
    }

    /// Returns whether this record is an AT Protocol record (has $type field).
    pub fn is_at_proto_record(&self) -> bool {
        self.at_proto_type.is_some()
    }

    /// Returns whether this record looks like a repo commit.
    pub fn is_repo_commit(&self) -> bool {
        // A repo commit has "did", "data", "rev", and "version" fields
        if let DagCborValue::Map(map) = &self.data_block.value {
            map.contains_key("did")
                && map.contains_key("data")
                && map.contains_key("rev")
                && map.contains_key("version")
        } else {
            false
        }
    }

    /// Returns whether this record looks like an MST node.
    pub fn is_mst_node(&self) -> bool {
        // An MST node has optional "l" (left pointer) and "e" (entries) array
        if let DagCborValue::Map(map) = &self.data_block.value {
            map.contains_key("e") || map.contains_key("l")
        } else {
            false
        }
    }

    /// Returns a string describing the record type.
    pub fn get_record_type_string(&self) -> &'static str {
        if self.is_at_proto_record() {
            "ATPROTO RECORD"
        } else if self.is_mst_node() {
            "MST NODE"
        } else if self.is_repo_commit() {
            "REPO COMMIT"
        } else {
            "REPO RECORD (GENERIC)"
        }
    }
}

impl std::fmt::Display for RepoRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RepoRecord {{ cid: {}, type: {:?} }}",
            self.cid.get_base32(),
            self.at_proto_type
        )
    }
}
