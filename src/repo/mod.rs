//! Repository parsing module for AT Protocol CAR files.
//!
//! This module provides types and functions for parsing CAR (Content Addressable aRchive)
//! files used by the AT Protocol. This includes:
//!
//! - VarInt: Variable-length integers used throughout CAR format
//! - CidV1: Content identifiers (CID version 1)
//! - DagCborObject: DAG-CBOR encoded data blocks
//! - RepoHeader: CAR file header
//! - RepoRecord: Individual records within a repository
//! - RepoMst: MST integration helpers
//!

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
