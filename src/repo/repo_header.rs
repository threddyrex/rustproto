//! CAR file header.
//!
//! The header for a CAR (Content Addressable aRchive) file.
//! Contains the version and the root CID(s) pointing to the repo commit.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use super::cid::CidV1;
use super::dag_cbor::{DagCborObject, DagCborType, DagCborMajorType, DagCborValue};
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

    /// Converts this RepoHeader to a DAG-CBOR object.
    pub fn to_dag_cbor_object(&self) -> DagCborObject {
        let mut header_map = HashMap::new();

        // Create roots array with the CID
        let roots_array = vec![DagCborObject {
            cbor_type: DagCborType {
                major_type: DagCborMajorType::Tag,
                additional_info: 24,
                original_byte: 0,
            },
            value: DagCborValue::Cid(self.repo_commit_cid.clone()),
        }];

        header_map.insert(
            "roots".to_string(),
            DagCborObject {
                cbor_type: DagCborType {
                    major_type: DagCborMajorType::Array,
                    additional_info: 1,
                    original_byte: 0,
                },
                value: DagCborValue::Array(roots_array),
            },
        );

        header_map.insert(
            "version".to_string(),
            DagCborObject {
                cbor_type: DagCborType {
                    major_type: DagCborMajorType::UnsignedInt,
                    additional_info: self.version as u8,
                    original_byte: 0,
                },
                value: DagCborValue::UnsignedInt(self.version),
            },
        );

        DagCborObject {
            cbor_type: DagCborType {
                major_type: DagCborMajorType::Map,
                additional_info: 2,
                original_byte: 0,
            },
            value: DagCborValue::Map(header_map),
        }
    }

    /// Writes this RepoHeader to a stream.
    pub fn write_to_stream<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let header_dag_cbor = self.to_dag_cbor_object();
        let header_bytes = header_dag_cbor.to_bytes()?;
        let header_length = VarInt::from_long(header_bytes.len() as i64);
        header_length.write_varint(writer)?;
        writer.write_all(&header_bytes)?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_cid() -> CidV1 {
        CidV1 {
            version: VarInt::from_long(1),
            multicodec: VarInt::from_long(0x71), // dag-cbor
            hash_function: VarInt::from_long(0x12), // sha2-256
            digest_size: VarInt::from_long(32),
            digest_bytes: vec![0xAB; 32],
            all_bytes: Vec::new(),
            base32: String::new(),
        }
    }

    #[test]
    fn test_repo_header_roundtrip() {
        let header = RepoHeader {
            repo_commit_cid: create_test_cid(),
            version: 1,
        };

        // Write to bytes
        let mut buf = Vec::new();
        header.write_to_stream(&mut buf).unwrap();

        // Read back
        let mut cursor = Cursor::new(&buf);
        let decoded = RepoHeader::read_from_stream(&mut cursor).unwrap();

        assert_eq!(header.version, decoded.version);
        assert_eq!(header.repo_commit_cid.digest_bytes, decoded.repo_commit_cid.digest_bytes);
        assert_eq!(header.repo_commit_cid.multicodec.value, decoded.repo_commit_cid.multicodec.value);
    }

    #[test]
    fn test_repo_header_to_dag_cbor_object() {
        let header = RepoHeader {
            repo_commit_cid: create_test_cid(),
            version: 1,
        };

        let dag_cbor = header.to_dag_cbor_object();
        
        // Verify it's a map with the expected keys
        if let DagCborValue::Map(map) = &dag_cbor.value {
            assert!(map.contains_key("roots"));
            assert!(map.contains_key("version"));
            
            // Verify version
            if let Some(version_obj) = map.get("version") {
                if let DagCborValue::UnsignedInt(v) = &version_obj.value {
                    assert_eq!(*v, 1);
                } else {
                    panic!("Expected UnsignedInt for version");
                }
            } else {
                panic!("Missing version key");
            }
        } else {
            panic!("Expected Map");
        }
    }
}
