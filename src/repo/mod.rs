//! Repository parsing module for AT Protocol CAR files.
//! 
//! ----------------------------------------------------------------------------------------------------
//! CAR format
//! ----------------------------------------------------------------------------------------------------
//!
//! Let's start by describing CAR.
//!
//! CAR Specs:
//!
//! - <https://ipld.io/specs/transport/car/carv1/>
//! - <https://ipld.io/specs/codecs/dag-cbor/spec/>
//!
//! A CAR contains one header, followed by a set of data items:
//!
//! ```text
//! [---  header  -------- ]   [----------------- data ---------------------------------]
//! [varint | header block ]   [varint | cid | data block]....[varint | cid | data block]
//! ```
//!
//! We represent this in rustproto using the VarInt, CidV1, and DagCborObject structs:
//!
//! ```text
//! [---  header  -------- ]   [----------------- data -------------------------------------------]
//! [VarInt | DagCborObject]   [VarInt | CidV1 | DagCborObject]....[VarInt | CidV1 | DagCborObject]
//! ```
//!
//! and then wrap them up in RepoHeader and RepoRecord structs:
//!
//! ```text
//! [---  header  -------- ]   [----------------- data -------------------------------------------]
//! [RepoHeader]               [RepoRecord]....[RepoRecord][RepoRecord][RepoRecord][RepoRecord]
//! ```
//!
//! The `Repo::walk_repo` function returns those RepoHeader and RepoRecord structs.
//!
//!
//!
//! ----------------------------------------------------------------------------------------------------
//! atproto repo format
//! ----------------------------------------------------------------------------------------------------
//!
//! atproto uses CAR. atproto repo spec:
//!     <https://atproto.com/specs/repository>
//!
//! Each RepoRecord data item is either a repo commit, MST node,
//! or atproto record. Look for the `RepoRecord::is_repo_commit()`,
//! `RepoRecord::is_at_proto_record()`, and `RepoRecord::is_mst_node()` methods.
//!
//! An atproto repo has the following format:
//!
//! ```text
//! RepoHeader (only 1 in the repo)
//!     repo_commit_cid: CidV1 (points to repo commit RepoRecord)
//!     version: i32
//!
//! Repo commit RepoRecord (only 1, where is_repo_commit() is true)
//!     cid: CidV1
//!     "did" -> String (user's did)
//!     "rev" -> String (increases monotonically, typically timestamp)
//!     "sig" -> bytes (computed each time repo changes, from the private key)
//!     "data" -> CidV1 (points to root MST node cid)
//!     "prev" -> Option<CidV1> (usually None)
//!     "version" -> i32 (always 3)
//!
//! MST node RepoRecord (1 or more, where is_mst_node() is true)
//!     cid: CidV1
//!     "e" -> Vec of MST entries (see mst::MstEntry)
//!     "l" -> Option<CidV1> (optional left subtree pointer)
//!     For in-memory representation, see mst::MstNode (key_depth, left_tree, entries)
//!
//! MST entries (within MST node, see mst::MstEntry)
//!     key: String (the full key, built from the collection, suffix, and prefix length)
//!     value: String (the cid of the repo record, in base32)
//!     right_tree: Option<Box<MstNode>> (optional right subtree)
//!     In CAR: "k" -> key suffix, "p" -> prefix length, "t" -> right tree, "v" -> value cid
//!
//! atproto RepoRecord (1 or more, where is_at_proto_record() is true)
//!     cid: CidV1
//!     data_block: DagCborObject (the actual atproto record)
//! ```
//!
//! ----------------------------------------------------------------------------------------------------

mod base32;
mod varint;
mod cid;
mod dag_cbor;
mod repo_header;
mod repo_record;
mod repo;
mod repo_mst;

// Re-exports
pub use base32::Base32Encoding;
pub use varint::VarInt;
pub use cid::CidV1;
pub use dag_cbor::{DagCborObject, DagCborType, DagCborMajorType, DagCborValue};
pub use repo_header::RepoHeader;
pub use repo_record::{RepoRecord, AtProtoType};
pub use repo::Repo;
pub use repo_mst::{RepoMst, MstNodeKey};
