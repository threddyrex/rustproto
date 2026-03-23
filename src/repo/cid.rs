//! Content Identifier (CID) version 1 implementation.
//!
//! Represents a CID in AT Protocol. Only CID version 1 is supported.
//! Two types of CIDs are used:
//! - dag-cbor (0x71) for records
//! - raw (0x55) for blobs
//!
//! Reference: <https://github.com/multiformats/cid>

use std::io::{self, Read, Write, Cursor};

use sha2::{Sha256, Digest};

use super::base32::Base32Encoding;
use super::varint::VarInt;
use super::dag_cbor::DagCborObject;

/// Multicodec values for CID types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Multicodec {
    /// DAG-CBOR codec (0x71) - used for records
    DagCbor = 0x71,
    /// Raw codec (0x55) - used for blobs
    Raw = 0x55,
}

impl Multicodec {
    fn from_value(value: i64) -> Result<Self, String> {
        match value {
            0x71 => Ok(Multicodec::DagCbor),
            0x55 => Ok(Multicodec::Raw),
            _ => Err(format!("Unknown multicodec value: 0x{:X}", value)),
        }
    }
}

/// A CID version 1 structure.
#[derive(Debug, Clone)]
pub struct CidV1 {
    /// CID version (always 1)
    pub version: VarInt,
    /// Multicodec (dag-cbor or raw)
    pub multicodec: VarInt,
    /// Hash function (typically sha2-256 = 0x12)
    pub hash_function: VarInt,
    /// Digest size (typically 32 bytes)
    pub digest_size: VarInt,
    /// The actual hash digest bytes
    pub digest_bytes: Vec<u8>,
    /// Complete encoded CID bytes (for caching)
    pub all_bytes: Vec<u8>,
    /// Base32 representation (with 'b' prefix)
    pub base32: String,
}

impl CidV1 {
    /// Reads a CID from a stream (e.g., from a CAR file).
    pub fn read_cid<R: Read>(reader: &mut R) -> io::Result<Self> {
        let version = VarInt::read_varint(reader)?;
        let multicodec = VarInt::read_varint(reader)?;
        let hash_function = VarInt::read_varint(reader)?;
        let digest_size = VarInt::read_varint(reader)?;

        if version.value != 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Only CID v1 is supported. Got version: {}",
                    version.value
                ),
            ));
        }

        // Validate multicodec
        if let Err(e) = Multicodec::from_value(multicodec.value) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }

        let mut digest_bytes = vec![0u8; digest_size.value as usize];
        reader.read_exact(&mut digest_bytes)?;

        // Build all_bytes
        let mut all_bytes = Vec::new();
        all_bytes.push(version.value as u8);
        all_bytes.push(multicodec.value as u8);
        all_bytes.push(hash_function.value as u8);
        all_bytes.push(digest_size.value as u8);
        all_bytes.extend(&digest_bytes);

        let base32 = format!("b{}", Base32Encoding::bytes_to_base32(&all_bytes));

        Ok(CidV1 {
            version,
            multicodec,
            hash_function,
            digest_size,
            digest_bytes,
            all_bytes,
            base32,
        })
    }

    /// Creates a CID from a base32 string (must start with 'b').
    pub fn from_base32(base32: &str) -> Result<Self, String> {
        if !base32.starts_with('b') {
            return Err("CID base32 string must start with 'b'".to_string());
        }

        let original_bytes = Base32Encoding::base32_to_bytes(&base32[1..])?;
        let mut cursor = Cursor::new(&original_bytes);
        
        let cid = Self::read_cid(&mut cursor)
            .map_err(|e| format!("Failed to parse CID: {}", e))?;
        
        // Use original bytes and base32 string to preserve exact encoding
        Ok(CidV1 {
            version: cid.version,
            multicodec: cid.multicodec,
            hash_function: cid.hash_function,
            digest_size: cid.digest_size,
            digest_bytes: cid.digest_bytes,
            all_bytes: original_bytes,
            base32: base32.to_string(),
        })
    }

    /// Writes a CID to a stream.
    pub fn write_cid<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.version.write_varint(writer)?;
        self.multicodec.write_varint(writer)?;
        self.hash_function.write_varint(writer)?;
        self.digest_size.write_varint(writer)?;
        writer.write_all(&self.digest_bytes)?;
        Ok(())
    }

    /// Returns the base32 representation with 'b' prefix.
    pub fn get_base32(&self) -> &str {
        &self.base32
    }

    /// Returns whether this CID uses dag-cbor multicodec.
    pub fn is_dag_cbor(&self) -> bool {
        self.multicodec.value == Multicodec::DagCbor as i64
    }

    /// Returns whether this CID uses raw multicodec (for blobs).
    pub fn is_raw(&self) -> bool {
        self.multicodec.value == Multicodec::Raw as i64
    }

    /// Computes a CIDv1 for a DAG-CBOR object.
    ///
    /// This hashes the serialized DAG-CBOR bytes with SHA-256 and creates
    /// a CIDv1 with the dag-cbor multicodec (0x71).
    pub fn compute_cid_for_dag_cbor(dag_cbor_object: &DagCborObject) -> io::Result<Self> {
        let bytes = dag_cbor_object.to_bytes()?;
        
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let hash = hasher.finalize();

        // Create CIDv1 with dag-cbor multicodec (0x71) and sha256 (0x12)
        let version = VarInt::from_long(1);
        let multicodec = VarInt::from_long(0x71);
        let hash_function = VarInt::from_long(0x12);
        let digest_size = VarInt::from_long(32);
        let digest_bytes: Vec<u8> = hash.to_vec();

        // Build all_bytes
        let mut all_bytes = Vec::new();
        version.write_varint(&mut all_bytes)?;
        multicodec.write_varint(&mut all_bytes)?;
        hash_function.write_varint(&mut all_bytes)?;
        digest_size.write_varint(&mut all_bytes)?;
        all_bytes.extend(&digest_bytes);

        let base32 = format!("b{}", Base32Encoding::bytes_to_base32(&all_bytes));

        Ok(CidV1 {
            version,
            multicodec,
            hash_function,
            digest_size,
            digest_bytes,
            all_bytes,
            base32,
        })
    }

    /// Computes a CIDv1 for raw blob bytes.
    ///
    /// This hashes the bytes with SHA-256 and creates a CIDv1 with
    /// the raw multicodec (0x55).
    pub fn compute_cid_for_blob_bytes(blob_bytes: &[u8]) -> io::Result<Self> {
        let mut hasher = Sha256::new();
        hasher.update(blob_bytes);
        let hash = hasher.finalize();

        // Create CIDv1 with raw multicodec (0x55) and sha256 (0x12)
        let version = VarInt::from_long(1);
        let multicodec = VarInt::from_long(0x55);
        let hash_function = VarInt::from_long(0x12);
        let digest_size = VarInt::from_long(32);
        let digest_bytes: Vec<u8> = hash.to_vec();

        // Build all_bytes
        let mut all_bytes = Vec::new();
        version.write_varint(&mut all_bytes)?;
        multicodec.write_varint(&mut all_bytes)?;
        hash_function.write_varint(&mut all_bytes)?;
        digest_size.write_varint(&mut all_bytes)?;
        all_bytes.extend(&digest_bytes);

        let base32 = format!("b{}", Base32Encoding::bytes_to_base32(&all_bytes));

        Ok(CidV1 {
            version,
            multicodec,
            hash_function,
            digest_size,
            digest_bytes,
            all_bytes,
            base32,
        })
    }
}

impl std::fmt::Display for CidV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.base32)
    }
}

impl PartialEq for CidV1 {
    fn eq(&self, other: &Self) -> bool {
        self.base32 == other.base32
    }
}

impl Eq for CidV1 {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cid_roundtrip() {
        // Create a mock CID
        let cid = CidV1 {
            version: VarInt::from_long(1),
            multicodec: VarInt::from_long(0x71), // dag-cbor
            hash_function: VarInt::from_long(0x12), // sha2-256
            digest_size: VarInt::from_long(32),
            digest_bytes: vec![0xAB; 32], // dummy hash
            all_bytes: Vec::new(), // will be rebuilt
            base32: String::new(), // will be rebuilt
        };

        let mut buf = Vec::new();
        cid.write_cid(&mut buf).unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded = CidV1::read_cid(&mut cursor).unwrap();

        assert_eq!(cid.version.value, decoded.version.value);
        assert_eq!(cid.multicodec.value, decoded.multicodec.value);
        assert_eq!(cid.hash_function.value, decoded.hash_function.value);
        assert_eq!(cid.digest_size.value, decoded.digest_size.value);
        assert_eq!(cid.digest_bytes, decoded.digest_bytes);
    }

    #[test]
    fn test_cid_base32_roundtrip() {
        // Create a CID, encode to base32, decode, verify
        let mut buf = Vec::new();
        buf.push(1); // version
        buf.push(0x71); // dag-cbor
        buf.push(0x12); // sha2-256
        buf.push(32); // digest size
        buf.extend(vec![0xAB; 32]); // digest

        let mut cursor = Cursor::new(&buf);
        let cid = CidV1::read_cid(&mut cursor).unwrap();
        
        let base32 = cid.get_base32();
        let decoded = CidV1::from_base32(base32).unwrap();

        assert_eq!(cid.version.value, decoded.version.value);
        assert_eq!(cid.multicodec.value, decoded.multicodec.value);
        assert_eq!(cid.digest_bytes, decoded.digest_bytes);
    }
}
