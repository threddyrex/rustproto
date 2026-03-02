//! Password verification utilities.
//!
//! This module provides password hashing and verification using PBKDF2-SHA256,
//! matching the implementation in dnproto's PasswordHasher.

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use pbkdf2::pbkdf2_hmac;
use sha2::Sha256;

/// Password hasher constants (must match dnproto).
const SALT_SIZE: usize = 16; // 128 bits
const HASH_SIZE: usize = 32; // 256 bits
const ITERATIONS: u32 = 100_000; // OWASP recommendation

/// Verify a password against a stored PBKDF2 hash.
///
/// This implementation matches dnproto's PasswordHasher.VerifyPassword.
///
/// # Arguments
///
/// * `stored_hash` - The base64-encoded hash from the database (salt + hash)
/// * `password` - The plaintext password to verify
///
/// # Returns
///
/// true if the password matches, false otherwise.
pub fn verify_password(stored_hash: Option<&str>, password: &str) -> bool {
    let Some(stored_hash) = stored_hash else {
        return false;
    };

    if password.is_empty() {
        return false;
    }

    // Decode the base64 hash
    let hash_bytes = match BASE64.decode(stored_hash) {
        Ok(bytes) => bytes,
        Err(_) => return false,
    };

    // Ensure the hash is the correct length
    if hash_bytes.len() != SALT_SIZE + HASH_SIZE {
        return false;
    }

    // Extract salt and stored hash
    let salt = &hash_bytes[..SALT_SIZE];
    let stored_hash_bytes = &hash_bytes[SALT_SIZE..];

    // Compute the hash with the same parameters
    let mut computed_hash = [0u8; HASH_SIZE];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, ITERATIONS, &mut computed_hash);

    // Constant-time comparison to prevent timing attacks
    constant_time_eq(stored_hash_bytes, &computed_hash)
}

/// Constant-time comparison to prevent timing attacks.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_password_empty_hash() {
        assert!(!verify_password(None, "password"));
    }

    #[test]
    fn test_verify_password_empty_password() {
        assert!(!verify_password(Some("somehash"), ""));
    }

    #[test]
    fn test_verify_password_invalid_base64() {
        assert!(!verify_password(Some("not-valid-base64!!!"), "password"));
    }

    #[test]
    fn test_verify_password_wrong_length() {
        // Just the salt, no hash
        let short_hash = BASE64.encode([0u8; SALT_SIZE]);
        assert!(!verify_password(Some(&short_hash), "password"));
    }
}
