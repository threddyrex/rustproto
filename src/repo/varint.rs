//! Variable-length integer encoding/decoding.
//!
//! In CAR files, several values are stored as variable-length integers (varints).
//! A varint is a sequence of bytes where the lower 7 bits of each byte are data
//! and the high bit is a flag indicating whether there are more bytes in the sequence.
//!
//! Reference: <https://protobuf.dev/programming-guides/encoding/#varints>

use std::io::{self, Read, Write};

/// A variable-length integer value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VarInt {
    pub value: i64,
}

impl VarInt {
    /// Creates a new VarInt from a value.
    pub fn from_long(value: i64) -> Self {
        VarInt { value }
    }

    /// Reads a varint from a stream.
    pub fn read_varint<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut value: i64 = 0;
        let mut shift = 0;
        
        loop {
            let mut byte = [0u8; 1];
            reader.read_exact(&mut byte)?;
            let b = byte[0];
            
            value |= ((b & 0x7F) as i64) << shift;
            shift += 7;
            
            // Check if the high bit is set (more bytes to come)
            if (b & 0x80) == 0 {
                break;
            }
        }
        
        Ok(VarInt { value })
    }

    /// Writes a varint to a stream.
    pub fn write_varint<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut value = self.value;
        
        while value >= 0x80 {
            // Write the lower 7 bits with the high bit set (continuation flag)
            writer.write_all(&[((value & 0x7F) | 0x80) as u8])?;
            value >>= 7;
        }
        
        // Write the final byte without the high bit set
        writer.write_all(&[value as u8])?;
        Ok(())
    }
}

impl std::fmt::Display for VarInt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (hex:0x{:X})", self.value, self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_varint_roundtrip() {
        let values = [0, 1, 127, 128, 255, 256, 16383, 16384, 1000000];
        
        for &val in &values {
            let varint = VarInt::from_long(val);
            let mut buf = Vec::new();
            varint.write_varint(&mut buf).unwrap();
            
            let mut cursor = Cursor::new(buf);
            let decoded = VarInt::read_varint(&mut cursor).unwrap();
            
            assert_eq!(varint.value, decoded.value, "Failed for value {}", val);
        }
    }

    #[test]
    fn test_varint_small_values() {
        // Values < 128 should encode to single byte
        for val in 0..128i64 {
            let varint = VarInt::from_long(val);
            let mut buf = Vec::new();
            varint.write_varint(&mut buf).unwrap();
            assert_eq!(buf.len(), 1);
            assert_eq!(buf[0], val as u8);
        }
    }
}
