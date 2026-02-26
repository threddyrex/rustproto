//! CAR file header.
//!
//! The header for a CAR (Content Addressable aRchive) file.
//! Contains the version and the root CID(s) pointing to the repo commit.

use std::io::{self, Read};

use super::cid::CidV1;
use super::dag_cbor::{DagCborObject, DagCborValue};
use super::varint::VarInt;

/// The header of a CAR file.
#[derive(Debug, Clone)]
pub struct RepoHeader {
    /// Points to the CID of the root commit for the repo.
    pub repo_commit_cid: CidV1,
    /// CAR format version (always 1 for now).
    pub version: i64,
}

impl RepoHeader {
    /// Reads a RepoHeader from a stream.
    pub fn read_from_stream<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read header length (varint)
        let _header_length = VarInt::read_varint(reader)?;
        
        // Read header as DAG-CBOR
        let header = DagCborObject::read_from_stream(reader)?;
        
        Self::from_dag_cbor_object(&header)
    }

    /// Creates a RepoHeader from a DAG-CBOR object.
    fn from_dag_cbor_object(dag_cbor: &DagCborObject) -> io::Result<Self> {
        let map = match &dag_cbor.value {
            DagCborValue::Map(m) => m,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RepoHeader must be a map",
                ))
            }
        };

        // Extract version
        let version = map
            .get("version")
            .and_then(|v| match &v.value {
                DagCborValue::UnsignedInt(n) => Some(*n),
                _ => None,
            })
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Missing or invalid 'version' field")
            })?;

        // Extract roots array and get the first root CID
        let repo_commit_cid = map
            .get("roots")
            .and_then(|roots_obj| match &roots_obj.value {
                DagCborValue::Array(arr) => arr.first(),
                _ => None,
            })
            .and_then(|first_root| match &first_root.value {
                DagCborValue::Cid(cid) => Some(cid.clone()),
                _ => None,
            })
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "Missing or invalid 'roots' field")
            })?;

        Ok(RepoHeader {
            repo_commit_cid,
            version,
        })
    }
}

impl std::fmt::Display for RepoHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RepoHeader {{ version: {}, root: {} }}",
            self.version,
            self.repo_commit_cid.get_base32()
        )
    }
}
