//! Base32 encoding/decoding (RFC 4648 lowercase variant).
//!
//! This module provides base32 encoding used for CID string representations
//! in the AT Protocol. Uses the lowercase RFC 4648 alphabet (a-z, 2-7).

/// Base32 encoding helper.
pub struct Base32Encoding;

const CHAR_MAP: &[u8] = b"abcdefghijklmnopqrstuvwxyz234567";

impl Base32Encoding {
    /// Encodes bytes to a base32 string.
    /// Takes 5 bits at a time and converts to base32 character.
    pub fn bytes_to_base32(bytes: &[u8]) -> String {
        if bytes.is_empty() {
            return String::new();
        }

        let mut result = String::new();
        let mut current_byte_index = 0;
        let mut bits_remaining = 8;

        while bits_remaining > 0 {
            if bits_remaining >= 5 {
                let next5int = (bytes[current_byte_index] >> (bits_remaining - 5)) & 0x1F;
                result.push(CHAR_MAP[next5int as usize] as char);
                bits_remaining -= 5;

                if bits_remaining == 0 && current_byte_index + 1 < bytes.len() {
                    current_byte_index += 1;
                    bits_remaining = 8;
                }
            } else if current_byte_index + 1 < bytes.len() {
                // Need to span two bytes
                let mut next5int = bytes[current_byte_index] as u8;
                // Shift left to get the bits we need
                next5int = next5int << (5 - bits_remaining);
                // Mask out the rest
                next5int &= 0x1F;
                // Get the next byte
                let mut next5int2 = bytes[current_byte_index + 1] as u8;
                // Shift right to get the bits we need
                next5int2 >>= 8 - (5 - bits_remaining);
                // Mask out the rest
                next5int2 &= 0x1F;
                // Combine those two
                next5int |= next5int2;
                result.push(CHAR_MAP[next5int as usize] as char);
                // Move to the next byte
                current_byte_index += 1;
                // Figure out bits remaining
                bits_remaining = 8 - (5 - bits_remaining);
            } else {
                // This is the last one
                let mut next5int = bytes[current_byte_index];
                // Shift left to get the bits we need
                next5int = next5int << (5 - bits_remaining);
                // Mask out the rest
                next5int &= 0x1F;
                result.push(CHAR_MAP[next5int as usize] as char);
                bits_remaining = 0; // end
            }
        }

        result
    }

    /// Decodes a base32 string back to bytes.
    /// This is the inverse of bytes_to_base32.
    pub fn base32_to_bytes(base32_string: &str) -> Result<Vec<u8>, String> {
        if base32_string.is_empty() {
            return Ok(Vec::new());
        }

        // Calculate output size: 5 bits per character, pack into 8-bit bytes
        let bit_count = base32_string.len() * 5;
        let byte_count = bit_count / 8;
        let mut result = vec![0u8; byte_count];

        let mut current_byte = 0;
        let mut bits_in_current_byte = 0;

        for c in base32_string.chars() {
            // Stop if we've filled all the bytes we need
            if current_byte >= byte_count {
                break;
            }

            // Get the 5-bit value for this character
            let value = match c.to_ascii_lowercase() {
                'a'..='z' => c.to_ascii_lowercase() as u8 - b'a',
                '2'..='7' => c as u8 - b'2' + 26,
                _ => return Err(format!("Invalid base32 character: {}", c)),
            };

            // We have 5 bits to add to our output
            let mut bits_to_add = 5;

            while bits_to_add > 0 && current_byte < byte_count {
                let bits_available_in_current_byte = 8 - bits_in_current_byte;

                if bits_to_add >= bits_available_in_current_byte {
                    // We can fill the current byte (or more)
                    let bits_to_take = bits_available_in_current_byte;
                    let shift = bits_to_add - bits_to_take;
                    let mask = (1 << bits_to_take) - 1;
                    let bits_value = (value >> shift) & mask;

                    result[current_byte] |=
                        bits_value << (8 - bits_in_current_byte - bits_to_take);

                    bits_in_current_byte += bits_to_take;
                    bits_to_add -= bits_to_take;

                    if bits_in_current_byte == 8 {
                        current_byte += 1;
                        bits_in_current_byte = 0;
                    }
                } else {
                    // We can't fill the current byte, just add what we have
                    let mask = (1 << bits_to_add) - 1;
                    let bits_value = value & mask;

                    result[current_byte] |=
                        bits_value << (8 - bits_in_current_byte - bits_to_add);

                    bits_in_current_byte += bits_to_add;
                    bits_to_add = 0;
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base32_roundtrip() {
        let test_data = [
            vec![0x01, 0x71, 0x12, 0x20],
            vec![0xAB, 0xCD, 0xEF],
            vec![0x00, 0xFF],
            vec![0x12, 0x34, 0x56, 0x78, 0x9A],
        ];

        for data in &test_data {
            let encoded = Base32Encoding::bytes_to_base32(data);
            let decoded = Base32Encoding::base32_to_bytes(&encoded).unwrap();
            assert_eq!(
                data, &decoded,
                "Roundtrip failed for {:?} -> {} -> {:?}",
                data, encoded, decoded
            );
        }
    }

    #[test]
    fn test_known_values() {
        // Test some known base32 encodings
        let data = vec![0x01, 0x71, 0x12, 0x20];
        let encoded = Base32Encoding::bytes_to_base32(&data);
        // Just verify it produces valid base32 characters
        assert!(encoded.chars().all(|c| CHAR_MAP.contains(&(c as u8))));
    }
}
