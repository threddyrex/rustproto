//! Cryptographic signing utilities for AT Protocol.
//!
//! This module provides ES256 (ECDSA with P-256) signing for service auth tokens.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as BASE64URL};
use p256::ecdsa::{SigningKey, signature::hazmat::PrehashSigner};
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Error type for signing operations.
#[derive(Debug)]
pub enum SignerError {
    InvalidKey(String),
    SigningFailed(String),
    EncodingError(String),
}

impl std::fmt::Display for SignerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SignerError::InvalidKey(msg) => write!(f, "Invalid key: {}", msg),
            SignerError::SigningFailed(msg) => write!(f, "Signing failed: {}", msg),
            SignerError::EncodingError(msg) => write!(f, "Encoding error: {}", msg),
        }
    }
}

impl std::error::Error for SignerError {}

/// JWT header for ES256 signing.
#[derive(Serialize)]
struct JwtHeader {
    alg: &'static str,
    typ: &'static str,
}

/// JWT payload for service auth tokens.
#[derive(Serialize)]
struct ServiceAuthPayload {
    /// Issuer - the user's DID (the one requesting the token)
    iss: String,
    /// Audience - the service DID that will validate this token
    aud: String,
    /// Issued at timestamp
    iat: i64,
    /// Expiration timestamp
    exp: i64,
    /// Lexicon method (optional binding)
    #[serde(skip_serializing_if = "Option::is_none")]
    lxm: Option<String>,
}

/// Sign a service auth token using ES256 (ECDSA with P-256).
///
/// # Arguments
///
/// * `private_key_multibase` - The user's private key in multibase format (z prefix = base58btc)
/// * `issuer` - The user's DID (iss claim)
/// * `audience` - The target service's DID (aud claim)
/// * `lxm` - Optional lexicon method to bind the token to
/// * `expires_in_seconds` - Token lifetime in seconds
///
/// # Returns
///
/// A signed JWT token string.
pub fn sign_service_auth_token(
    private_key_multibase: &str,
    issuer: &str,
    audience: &str,
    lxm: Option<&str>,
    expires_in_seconds: i64,
) -> Result<String, SignerError> {
    // Decode the multibase private key (z prefix = base58btc)
    if !private_key_multibase.starts_with('z') {
        return Err(SignerError::InvalidKey(
            "Private key must be multibase (base58btc, z prefix)".to_string(),
        ));
    }

    let private_key_with_prefix = bs58::decode(&private_key_multibase[1..])
        .into_vec()
        .map_err(|e| SignerError::InvalidKey(format!("Invalid base58: {}", e)))?;

    // Check for P-256 private key prefix (0x86 0x26)
    if private_key_with_prefix.len() < 34 {
        return Err(SignerError::InvalidKey("Private key too short".to_string()));
    }

    if private_key_with_prefix[0] != 0x86 || private_key_with_prefix[1] != 0x26 {
        return Err(SignerError::InvalidKey(format!(
            "Expected P-256 private key prefix (0x86 0x26), got 0x{:02X} 0x{:02X}",
            private_key_with_prefix[0], private_key_with_prefix[1]
        )));
    }

    let private_key_bytes = &private_key_with_prefix[2..];
    if private_key_bytes.len() != 32 {
        return Err(SignerError::InvalidKey(format!(
            "Expected 32-byte private key, got {} bytes",
            private_key_bytes.len()
        )));
    }

    // Create signing key
    let signing_key = SigningKey::from_slice(private_key_bytes)
        .map_err(|e| SignerError::InvalidKey(format!("Invalid P-256 key: {}", e)))?;

    // Create header
    let header = JwtHeader {
        alg: "ES256",
        typ: "JWT",
    };

    // Create payload
    let now = chrono::Utc::now().timestamp();
    let payload = ServiceAuthPayload {
        iss: issuer.to_string(),
        aud: audience.to_string(),
        iat: now,
        exp: now + expires_in_seconds,
        lxm: lxm.map(|s| s.to_string()),
    };

    // Encode header and payload
    let header_json = serde_json::to_string(&header)
        .map_err(|e| SignerError::EncodingError(format!("Header serialization failed: {}", e)))?;
    let payload_json = serde_json::to_string(&payload)
        .map_err(|e| SignerError::EncodingError(format!("Payload serialization failed: {}", e)))?;

    let header_b64 = BASE64URL.encode(header_json.as_bytes());
    let payload_b64 = BASE64URL.encode(payload_json.as_bytes());

    // Create signing input
    let signing_input = format!("{}.{}", header_b64, payload_b64);

    // Hash the input
    let mut hasher = Sha256::new();
    hasher.update(signing_input.as_bytes());
    let hash: [u8; 32] = hasher.finalize().into();

    // Sign the hash
    let signature: p256::ecdsa::Signature = signing_key
        .sign_prehash(&hash)
        .map_err(|e| SignerError::SigningFailed(format!("Signing failed: {}", e)))?;

    // Get signature bytes and normalize to low-S form
    let signature_bytes = signature.to_bytes();
    let normalized_sig = normalize_low_s(&signature_bytes);

    // Encode signature
    let signature_b64 = BASE64URL.encode(&normalized_sig);

    // Assemble JWT
    Ok(format!("{}.{}.{}", header_b64, payload_b64, signature_b64))
}

/// Normalize ECDSA signature to low-S form (BIP-62 compliance).
fn normalize_low_s(signature: &[u8]) -> Vec<u8> {
    if signature.len() != 64 {
        return signature.to_vec();
    }

    let r = &signature[0..32];
    let s = &signature[32..64];

    // P-256 curve order
    let order: [u8; 32] = [
        0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xBC, 0xE6, 0xFA, 0xAD, 0xA7, 0x17, 0x9E, 0x84,
        0xF3, 0xB9, 0xCA, 0xC2, 0xFC, 0x63, 0x25, 0x51,
    ];

    // half_order = order / 2
    let half_order = div_by_2(&order);

    // Check if s > half_order (need to normalize)
    if compare_be(s, &half_order) > 0 {
        // s = order - s
        let normalized_s = subtract_be(&order, s);
        let mut result = Vec::with_capacity(64);
        result.extend_from_slice(r);
        result.extend_from_slice(&normalized_s);
        result
    } else {
        signature.to_vec()
    }
}

/// Compare two big-endian byte arrays.
fn compare_be(a: &[u8], b: &[u8]) -> i32 {
    for i in 0..a.len().min(b.len()) {
        if a[i] > b[i] {
            return 1;
        }
        if a[i] < b[i] {
            return -1;
        }
    }
    0
}

/// Divide a big-endian number by 2.
fn div_by_2(n: &[u8]) -> Vec<u8> {
    let mut result = vec![0u8; n.len()];
    let mut carry = 0u8;

    for i in 0..n.len() {
        let new_val = (n[i] >> 1) | (carry << 7);
        carry = n[i] & 1;
        result[i] = new_val;
    }

    result
}

/// Subtract two big-endian numbers: a - b.
fn subtract_be(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut result = vec![0u8; a.len()];
    let mut borrow = 0i32;

    for i in (0..a.len()).rev() {
        let diff = (a[i] as i32) - (b[i] as i32) - borrow;
        if diff < 0 {
            result[i] = (diff + 256) as u8;
            borrow = 1;
        } else {
            result[i] = diff as u8;
            borrow = 0;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_service_auth_token_invalid_key() {
        let result = sign_service_auth_token(
            "not-multibase",
            "did:plc:test",
            "did:plc:service",
            None,
            60,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_normalize_low_s_short_signature() {
        let short = vec![0x01, 0x02];
        let result = normalize_low_s(&short);
        assert_eq!(result, short);
    }
}
