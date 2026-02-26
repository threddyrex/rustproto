//! DAG-CBOR object encoding/decoding.
//!
//! Represents the data block section of a repository record.
//! Handles converting data between DAG-CBOR binary format and Rust types.
//!
//! Reference: https://ipld.io/specs/codecs/dag-cbor/spec/

use std::collections::HashMap;
use std::io::{self, Read, Write, Cursor};

use super::cid::CidV1;

/// CBOR major types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DagCborMajorType {
    UnsignedInt = 0,
    NegativeInt = 1,
    ByteString = 2,
    Text = 3,
    Array = 4,
    Map = 5,
    Tag = 6,
    SimpleValue = 7,
}

impl DagCborMajorType {
    fn from_value(value: u8) -> Option<Self> {
        match value {
            0 => Some(DagCborMajorType::UnsignedInt),
            1 => Some(DagCborMajorType::NegativeInt),
            2 => Some(DagCborMajorType::ByteString),
            3 => Some(DagCborMajorType::Text),
            4 => Some(DagCborMajorType::Array),
            5 => Some(DagCborMajorType::Map),
            6 => Some(DagCborMajorType::Tag),
            7 => Some(DagCborMajorType::SimpleValue),
            _ => None,
        }
    }

    /// Returns a string representation of the major type.
    pub fn as_str(&self) -> &'static str {
        match self {
            DagCborMajorType::UnsignedInt => "TYPE_UNSIGNED_INT",
            DagCborMajorType::NegativeInt => "TYPE_NEGATIVE_INT",
            DagCborMajorType::ByteString => "TYPE_BYTE_STRING",
            DagCborMajorType::Text => "TYPE_TEXT",
            DagCborMajorType::Array => "TYPE_ARRAY",
            DagCborMajorType::Map => "TYPE_MAP",
            DagCborMajorType::Tag => "TYPE_TAG",
            DagCborMajorType::SimpleValue => "TYPE_SIMPLE_VALUE",
        }
    }
}

/// Represents CBOR type information from the first byte.
#[derive(Debug, Clone)]
pub struct DagCborType {
    pub major_type: DagCborMajorType,
    pub additional_info: u8,
    pub original_byte: u8,
}

impl DagCborType {
    /// Reads the next CBOR type from a stream.
    pub fn read_next_type<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        let b = byte[0];

        let major_type_val = b >> 5;
        let additional_info = b & 0x1F;

        let major_type = DagCborMajorType::from_value(major_type_val).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown CBOR major type: {}", major_type_val),
            )
        })?;

        Ok(DagCborType {
            major_type,
            additional_info,
            original_byte: b,
        })
    }

    /// Returns a string representation of the major type.
    pub fn get_major_type_string(&self) -> &'static str {
        self.major_type.as_str()
    }
}

impl std::fmt::Display for DagCborType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CborType -> {} ({}), AdditionalInfo: {}",
            self.get_major_type_string(),
            self.major_type as u8,
            self.additional_info
        )
    }
}

/// The value stored in a DAG-CBOR object.
#[derive(Debug, Clone)]
pub enum DagCborValue {
    /// Unsigned integer
    UnsignedInt(i64),
    /// Negative integer
    NegativeInt(i64),
    /// Byte string
    ByteString(Vec<u8>),
    /// Text string
    Text(String),
    /// Array of DAG-CBOR objects
    Array(Vec<DagCborObject>),
    /// Map of string keys to DAG-CBOR objects
    Map(HashMap<String, DagCborObject>),
    /// CID tag (tag 42)
    Cid(CidV1),
    /// Boolean value
    Bool(bool),
    /// Null value
    Null,
}

impl DagCborValue {
    /// Tries to get the value as a string.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            DagCborValue::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Tries to get the value as an integer.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            DagCborValue::UnsignedInt(n) => Some(*n),
            DagCborValue::NegativeInt(n) => Some(*n),
            _ => None,
        }
    }

    /// Tries to get the value as a map.
    pub fn as_map(&self) -> Option<&HashMap<String, DagCborObject>> {
        match self {
            DagCborValue::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Tries to get the value as an array.
    pub fn as_array(&self) -> Option<&Vec<DagCborObject>> {
        match self {
            DagCborValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Tries to get the value as a CID.
    pub fn as_cid(&self) -> Option<&CidV1> {
        match self {
            DagCborValue::Cid(cid) => Some(cid),
            _ => None,
        }
    }
}

/// A DAG-CBOR object representing a data block in a repository record.
#[derive(Debug, Clone)]
pub struct DagCborObject {
    pub cbor_type: DagCborType,
    pub value: DagCborValue,
}

impl DagCborObject {
    /// Reads a DAG-CBOR object from a stream.
    pub fn read_from_stream<R: Read>(reader: &mut R) -> io::Result<Self> {
        let cbor_type = DagCborType::read_next_type(reader)?;

        let value = match cbor_type.major_type {
            DagCborMajorType::Map => {
                let length = Self::read_length_from_stream(&cbor_type, reader)?;
                let mut map = HashMap::new();

                for _ in 0..length {
                    let key_obj = DagCborObject::read_from_stream(reader)?;
                    let key = key_obj.try_get_string().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "Map key must be a string")
                    })?;
                    let value = DagCborObject::read_from_stream(reader)?;
                    map.insert(key, value);
                }

                DagCborValue::Map(map)
            }

            DagCborMajorType::Array => {
                let length = Self::read_length_from_stream(&cbor_type, reader)?;
                let mut array = Vec::with_capacity(length);

                for _ in 0..length {
                    array.push(DagCborObject::read_from_stream(reader)?);
                }

                DagCborValue::Array(array)
            }

            DagCborMajorType::Text => {
                let length = Self::read_length_from_stream(&cbor_type, reader)?;
                let mut bytes = vec![0u8; length];
                reader.read_exact(&mut bytes)?;
                let text = String::from_utf8(bytes).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Invalid UTF-8: {}", e))
                })?;
                DagCborValue::Text(text)
            }

            DagCborMajorType::Tag => {
                // Read the tag value
                let mut tag_byte = [0u8; 1];
                reader.read_exact(&mut tag_byte)?;
                let tag = tag_byte[0];

                if tag != 42 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unknown tag: {}. Only tag 42 (CID) is supported.", tag),
                    ));
                }

                // Read byte string type
                let _byte_string_type = DagCborType::read_next_type(reader)?;
                let _length = Self::read_length_from_stream(&_byte_string_type, reader)?;
                
                // Read and discard the multibase prefix (should be 0)
                let mut prefix = [0u8; 1];
                reader.read_exact(&mut prefix)?;

                // Read the CID
                let cid = CidV1::read_cid(reader)?;

                DagCborValue::Cid(cid)
            }

            DagCborMajorType::UnsignedInt => {
                let value = Self::read_length_from_stream(&cbor_type, reader)? as i64;
                DagCborValue::UnsignedInt(value)
            }

            DagCborMajorType::NegativeInt => {
                let value = Self::read_length_from_stream(&cbor_type, reader)? as i64;
                // CBOR negative int encoding: -1 - n
                DagCborValue::NegativeInt(-1 - value)
            }

            DagCborMajorType::ByteString => {
                let length = Self::read_length_from_stream(&cbor_type, reader)?;
                let mut bytes = vec![0u8; length];
                reader.read_exact(&mut bytes)?;
                DagCborValue::ByteString(bytes)
            }

            DagCborMajorType::SimpleValue => {
                match cbor_type.additional_info {
                    0x16 => DagCborValue::Null,
                    0x14 => DagCborValue::Bool(false),
                    0x15 => DagCborValue::Bool(true),
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Unknown simple value: {}", cbor_type.additional_info),
                        ))
                    }
                }
            }
        };

        Ok(DagCborObject { cbor_type, value })
    }

    /// Reads a DAG-CBOR object from bytes.
    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(data);
        Self::read_from_stream(&mut cursor)
    }

    /// Writes this DAG-CBOR object to a stream.
    pub fn write_to_stream<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match &self.value {
            DagCborValue::Map(map) => {
                Self::write_length_to_stream(DagCborMajorType::Map as u8, map.len(), writer)?;

                // DAG-CBOR requires map keys to be sorted in canonical order:
                // first by byte length, then lexicographically
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort_by(|a, b| {
                    let a_len = a.as_bytes().len();
                    let b_len = b.as_bytes().len();
                    a_len.cmp(&b_len).then_with(|| a.cmp(b))
                });

                for key in keys {
                    // Write key as text string
                    let key_bytes = key.as_bytes();
                    Self::write_length_to_stream(DagCborMajorType::Text as u8, key_bytes.len(), writer)?;
                    writer.write_all(key_bytes)?;

                    // Write value
                    map.get(key).unwrap().write_to_stream(writer)?;
                }
            }

            DagCborValue::Array(array) => {
                Self::write_length_to_stream(DagCborMajorType::Array as u8, array.len(), writer)?;
                for item in array {
                    item.write_to_stream(writer)?;
                }
            }

            DagCborValue::Text(text) => {
                let bytes = text.as_bytes();
                Self::write_length_to_stream(DagCborMajorType::Text as u8, bytes.len(), writer)?;
                writer.write_all(bytes)?;
            }

            DagCborValue::Cid(cid) => {
                // Write tag type and tag number (42 for CID)
                let tag_byte = (DagCborMajorType::Tag as u8) << 5 | 24;
                writer.write_all(&[tag_byte, 42])?;

                // Calculate CID bytes
                let mut cid_bytes = Vec::new();
                cid.write_cid(&mut cid_bytes)?;

                // Write byte string type for CID (with 0 prefix)
                Self::write_length_to_stream(
                    DagCborMajorType::ByteString as u8,
                    cid_bytes.len() + 1,
                    writer,
                )?;
                writer.write_all(&[0])?; // multibase prefix
                writer.write_all(&cid_bytes)?;
            }

            DagCborValue::UnsignedInt(value) => {
                Self::write_length_to_stream(DagCborMajorType::UnsignedInt as u8, *value as usize, writer)?;
            }

            DagCborValue::NegativeInt(value) => {
                // CBOR negative int encoding: store (-1 - n)
                let encoded = (-1 - value) as usize;
                Self::write_length_to_stream(DagCborMajorType::NegativeInt as u8, encoded, writer)?;
            }

            DagCborValue::ByteString(bytes) => {
                Self::write_length_to_stream(DagCborMajorType::ByteString as u8, bytes.len(), writer)?;
                writer.write_all(bytes)?;
            }

            DagCborValue::Bool(true) => {
                let byte = (DagCborMajorType::SimpleValue as u8) << 5 | 0x15;
                writer.write_all(&[byte])?;
            }

            DagCborValue::Bool(false) => {
                let byte = (DagCborMajorType::SimpleValue as u8) << 5 | 0x14;
                writer.write_all(&[byte])?;
            }

            DagCborValue::Null => {
                let byte = (DagCborMajorType::SimpleValue as u8) << 5 | 0x16;
                writer.write_all(&[byte])?;
            }
        }

        Ok(())
    }

    /// Converts this object to bytes.
    pub fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.write_to_stream(&mut buf)?;
        Ok(buf)
    }

    /// Reads the length value from the stream based on additional info.
    fn read_length_from_stream<R: Read>(cbor_type: &DagCborType, reader: &mut R) -> io::Result<usize> {
        let info = cbor_type.additional_info;

        if info < 24 {
            Ok(info as usize)
        } else if info == 24 {
            let mut byte = [0u8; 1];
            reader.read_exact(&mut byte)?;
            Ok(byte[0] as usize)
        } else if info == 25 {
            let mut bytes = [0u8; 2];
            reader.read_exact(&mut bytes)?;
            Ok(u16::from_be_bytes(bytes) as usize)
        } else if info == 26 {
            let mut bytes = [0u8; 4];
            reader.read_exact(&mut bytes)?;
            Ok(u32::from_be_bytes(bytes) as usize)
        } else if info == 27 {
            let mut bytes = [0u8; 8];
            reader.read_exact(&mut bytes)?;
            Ok(u64::from_be_bytes(bytes) as usize)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown additional info: {}", info),
            ))
        }
    }

    /// Writes the length value to the stream with appropriate encoding.
    fn write_length_to_stream<W: Write>(
        major_type: u8,
        length: usize,
        writer: &mut W,
    ) -> io::Result<()> {
        if length < 24 {
            let byte = (major_type << 5) | (length as u8);
            writer.write_all(&[byte])?;
        } else if length < 256 {
            let byte = (major_type << 5) | 24;
            writer.write_all(&[byte, length as u8])?;
        } else if length < 65536 {
            let byte = (major_type << 5) | 25;
            writer.write_all(&[byte])?;
            writer.write_all(&(length as u16).to_be_bytes())?;
        } else if length < 0x1_0000_0000 {
            let byte = (major_type << 5) | 26;
            writer.write_all(&[byte])?;
            writer.write_all(&(length as u32).to_be_bytes())?;
        } else {
            let byte = (major_type << 5) | 27;
            writer.write_all(&[byte])?;
            writer.write_all(&(length as u64).to_be_bytes())?;
        }

        Ok(())
    }

    /// Tries to get the value as a string.
    pub fn try_get_string(&self) -> Option<String> {
        match &self.value {
            DagCborValue::Text(s) => Some(s.clone()),
            DagCborValue::UnsignedInt(n) => Some(n.to_string()),
            DagCborValue::NegativeInt(n) => Some(n.to_string()),
            DagCborValue::Bool(b) => Some(b.to_string()),
            DagCborValue::Cid(cid) => Some(cid.get_base32().to_string()),
            _ => None,
        }
    }

    /// Selects a string value at the given property path.
    pub fn select_string(&self, property_names: &[&str]) -> Option<String> {
        let obj = self.select_object(property_names)?;
        obj.try_get_string()
    }

    /// Selects an object at the given property path.
    pub fn select_object(&self, property_names: &[&str]) -> Option<&DagCborObject> {
        let mut current = self;

        for name in property_names {
            match &current.value {
                DagCborValue::Map(map) => {
                    current = map.get(*name)?;
                }
                _ => return None,
            }
        }

        Some(current)
    }

    /// Converts this DAG-CBOR object to a JSON-compatible value for display.
    pub fn to_json_value(&self) -> serde_json::Value {
        match &self.value {
            DagCborValue::Text(s) => serde_json::Value::String(s.clone()),
            DagCborValue::UnsignedInt(n) => serde_json::Value::Number((*n).into()),
            DagCborValue::NegativeInt(n) => serde_json::Value::Number((*n).into()),
            DagCborValue::Bool(b) => serde_json::Value::Bool(*b),
            DagCborValue::Null => serde_json::Value::Null,
            DagCborValue::ByteString(bytes) => {
                // Encode bytes as base64 for JSON
                serde_json::Value::String(format!("base64:{}", base64_encode(bytes)))
            }
            DagCborValue::Cid(cid) => {
                // Return CID as a "$link" object (AT Protocol convention)
                serde_json::json!({ "$link": cid.get_base32() })
            }
            DagCborValue::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|item| item.to_json_value()).collect())
            }
            DagCborValue::Map(map) => {
                let mut json_map = serde_json::Map::new();
                for (key, value) in map {
                    json_map.insert(key.clone(), value.to_json_value());
                }
                serde_json::Value::Object(json_map)
            }
        }
    }

    /// Converts this DAG-CBOR object to a JSON string.
    pub fn to_json_string(&self) -> String {
        let json_value = self.to_json_value();
        serde_json::to_string_pretty(&json_value).unwrap_or_else(|_| "{}".to_string())
    }
}

impl std::fmt::Display for DagCborObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DagCborObject -> {:?}", self.value)
    }
}

/// Simple base64 encoding for byte strings.
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    
    for chunk in bytes.chunks(3) {
        let mut buf = [0u8; 3];
        buf[..chunk.len()].copy_from_slice(chunk);
        
        let n = (buf[0] as u32) << 16 | (buf[1] as u32) << 8 | buf[2] as u32;
        
        result.push(ALPHABET[(n >> 18) as usize & 63] as char);
        result.push(ALPHABET[(n >> 12) as usize & 63] as char);
        
        if chunk.len() > 1 {
            result.push(ALPHABET[(n >> 6) as usize & 63] as char);
        } else {
            result.push('=');
        }
        
        if chunk.len() > 2 {
            result.push(ALPHABET[n as usize & 63] as char);
        } else {
            result.push('=');
        }
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_simple_text() {
        // CBOR encoding of the string "hello"
        let data = vec![0x65, b'h', b'e', b'l', b'l', b'o'];
        let obj = DagCborObject::from_bytes(&data).unwrap();
        
        assert_eq!(obj.try_get_string(), Some("hello".to_string()));
    }

    #[test]
    fn test_read_unsigned_int() {
        // CBOR encoding of the integer 42
        let data = vec![0x18, 42];
        let obj = DagCborObject::from_bytes(&data).unwrap();
        
        match obj.value {
            DagCborValue::UnsignedInt(n) => assert_eq!(n, 42),
            _ => panic!("Expected UnsignedInt"),
        }
    }

    #[test]
    fn test_read_simple_map() {
        // CBOR encoding of {"a": 1}
        // a1 (map with 1 item) 61 (text, 1 char) 61 (ASCII 'a') 01 (unsigned int 1)
        let data = vec![0xA1, 0x61, b'a', 0x01];
        let obj = DagCborObject::from_bytes(&data).unwrap();
        
        match &obj.value {
            DagCborValue::Map(map) => {
                assert_eq!(map.len(), 1);
                let a = map.get("a").unwrap();
                match a.value {
                    DagCborValue::UnsignedInt(n) => assert_eq!(n, 1),
                    _ => panic!("Expected UnsignedInt"),
                }
            }
            _ => panic!("Expected Map"),
        }
    }

    #[test]
    fn test_roundtrip() {
        // Create a simple map and verify roundtrip
        let data = vec![0xA1, 0x61, b'a', 0x01]; // {"a": 1}
        let obj = DagCborObject::from_bytes(&data).unwrap();
        let encoded = obj.to_bytes().unwrap();
        let obj2 = DagCborObject::from_bytes(&encoded).unwrap();
        
        assert_eq!(obj.to_json_string(), obj2.to_json_string());
    }
}
