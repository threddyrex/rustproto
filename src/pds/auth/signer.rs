//! Cryptographic signing utilities for AT Protocol.
//!
//! This module provides ES256 (ECDSA with P-256) signing and verification for service auth tokens.

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as BASE64URL};
use p256::ecdsa::{SigningKey, VerifyingKey, signature::hazmat::PrehashSigner, signature::hazmat::PrehashVerifier};
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

/// JWT header for space delegation tokens.
///
/// Delegation tokens carry a distinguishing `typ` and a `kid` naming the
/// account's `#atproto` signing key.
#[derive(Serialize)]
struct DelegationJwtHeader {
    alg: &'static str,
    typ: &'static str,
    kid: &'static str,
}

/// JWT payload for space delegation tokens (AT Protocol permissioned spaces).
#[derive(Serialize)]
struct DelegationPayload {
    /// Issuer - the user's DID.
    iss: String,
    /// Subject - the target space URI (`at://{authority}/space/{type}/{skey}`).
    sub: String,
    /// Audience - the space authority's space host (`{authority}#atproto_space_host`).
    aud: String,
    /// Issued at timestamp.
    iat: i64,
    /// Expiration timestamp.
    exp: i64,
    /// Random single-use identifier.
    jti: String,
}

/// The `typ` header value identifying a space delegation token.
pub const DELEGATION_TOKEN_TYP: &str = "atproto-space-delegation+jwt";

/// Default lifetime of a delegation token, in seconds (spec default).
pub const DELEGATION_TOKEN_TTL_SECS: i64 = 60;

/// The `typ` header value identifying a space credential.
pub const SPACE_CREDENTIAL_TYP: &str = "atproto-space-credential+jwt";

/// Default lifetime of a space credential, in seconds (spec default, 2 hours).
pub const SPACE_CREDENTIAL_TTL_SECS: i64 = 7200;

/// JWT header for space credentials.
///
/// Credentials carry a distinguishing `typ` and a `kid` naming the space
/// authority's signing key.
#[derive(Serialize)]
struct SpaceCredentialJwtHeader {
    alg: &'static str,
    typ: &'static str,
    kid: &'static str,
}

/// DPoP confirmation claim (RFC 7800), binding a credential to a key.
#[derive(Serialize)]
struct Confirmation {
    /// JWK thumbprint (RFC 7638) of the bound key.
    jkt: String,
}

/// JWT payload for space credentials (AT Protocol permissioned spaces).
#[derive(Serialize)]
struct SpaceCredentialPayload {
    /// Issuer - the space authority's DID.
    iss: String,
    /// Subject - the target space URI (`at://{authority}/space/{type}/{skey}`).
    sub: String,
    /// Confirmation - binds the credential to the application's DPoP key.
    cnf: Confirmation,
    /// Issued at timestamp.
    iat: i64,
    /// Expiration timestamp.
    exp: i64,
    /// Random single-use identifier.
    jti: String,
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
    // Load the P-256 signing key from the multibase-encoded private key.
    let signing_key = load_p256_signing_key(private_key_multibase)?;

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

    sign_es256_jwt(&signing_key, &header_json, &payload_json)
}

/// Sign a space delegation token using ES256 (ECDSA with P-256).
///
/// Mints the single-use, short-lived JWT a client presents to a space
/// authority to prove it is acting on the user's behalf when requesting a
/// space credential (`com.atproto.space.getDelegationToken`).
///
/// # Arguments
///
/// * `private_key_multibase` - The user's private signing key in multibase format.
/// * `issuer` - The user's DID (`iss` claim).
/// * `space_uri` - The target space URI (`sub` claim), in the form
///   `at://{authority}/space/{spaceType}/{skey}`.
/// * `authority` - The space authority DID; the token's audience is
///   `{authority}#atproto_space_host`.
/// * `expires_in_seconds` - Token lifetime in seconds.
///
/// # Returns
///
/// A signed JWT delegation token string.
pub fn sign_delegation_token(
    private_key_multibase: &str,
    issuer: &str,
    space_uri: &str,
    authority: &str,
    expires_in_seconds: i64,
) -> Result<String, SignerError> {
    // Load the P-256 signing key from the multibase-encoded private key.
    let signing_key = load_p256_signing_key(private_key_multibase)?;

    // Create header
    let header = DelegationJwtHeader {
        alg: "ES256",
        typ: DELEGATION_TOKEN_TYP,
        kid: "#atproto",
    };

    // Create payload
    let now = chrono::Utc::now().timestamp();
    let payload = DelegationPayload {
        iss: issuer.to_string(),
        sub: space_uri.to_string(),
        aud: format!("{}#atproto_space_host", authority),
        iat: now,
        exp: now + expires_in_seconds,
        jti: uuid::Uuid::new_v4().to_string(),
    };

    // Encode header and payload
    let header_json = serde_json::to_string(&header)
        .map_err(|e| SignerError::EncodingError(format!("Header serialization failed: {}", e)))?;
    let payload_json = serde_json::to_string(&payload)
        .map_err(|e| SignerError::EncodingError(format!("Payload serialization failed: {}", e)))?;

    sign_es256_jwt(&signing_key, &header_json, &payload_json)
}

/// Sign a space credential using ES256 (ECDSA with P-256).
///
/// The space authority mints this token in exchange for a delegation token
/// (`com.atproto.space.getSpaceCredential`). It grants whole-space read/sync
/// access and is DPoP-bound to the requesting application via its `cnf.jkt`
/// claim, so it can be presented to any repo host serving a repo in the space.
///
/// # Arguments
///
/// * `private_key_multibase` - The space authority's private signing key in
///   multibase format. When the authority publishes no dedicated
///   `#atproto_space` key, this is the account's `#atproto` signing key.
/// * `authority` - The space authority DID (`iss` claim).
/// * `space_uri` - The target space URI (`sub` claim), in the form
///   `at://{authority}/space/{spaceType}/{skey}`.
/// * `dpop_jkt` - JWK thumbprint (RFC 7638) of the application's DPoP key, copied
///   into the credential's `cnf.jkt` to bind it to that key.
/// * `expires_in_seconds` - Token lifetime in seconds.
///
/// # Returns
///
/// A signed JWT space credential string.
pub fn sign_space_credential(
    private_key_multibase: &str,
    authority: &str,
    space_uri: &str,
    dpop_jkt: &str,
    expires_in_seconds: i64,
) -> Result<String, SignerError> {
    // Load the P-256 signing key from the multibase-encoded private key.
    let signing_key = load_p256_signing_key(private_key_multibase)?;

    // Create header. The credential is signed by the account's `#atproto` key,
    // which is the fallback space signing key when no `#atproto_space` key is
    // published.
    let header = SpaceCredentialJwtHeader {
        alg: "ES256",
        typ: SPACE_CREDENTIAL_TYP,
        kid: "#atproto",
    };

    // Create payload. A space credential has no `aud`: it is presented to any
    // repo host serving a repo in the space, not to a single recipient.
    let now = chrono::Utc::now().timestamp();
    let payload = SpaceCredentialPayload {
        iss: authority.to_string(),
        sub: space_uri.to_string(),
        cnf: Confirmation {
            jkt: dpop_jkt.to_string(),
        },
        iat: now,
        exp: now + expires_in_seconds,
        jti: uuid::Uuid::new_v4().to_string(),
    };

    // Encode header and payload
    let header_json = serde_json::to_string(&header)
        .map_err(|e| SignerError::EncodingError(format!("Header serialization failed: {}", e)))?;
    let payload_json = serde_json::to_string(&payload)
        .map_err(|e| SignerError::EncodingError(format!("Payload serialization failed: {}", e)))?;

    sign_es256_jwt(&signing_key, &header_json, &payload_json)
}

/// Decode a multibase (base58btc, `z` prefix) P-256 private key and construct a
/// [`SigningKey`].
fn load_p256_signing_key(private_key_multibase: &str) -> Result<SigningKey, SignerError> {
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

    SigningKey::from_slice(private_key_bytes)
        .map_err(|e| SignerError::InvalidKey(format!("Invalid P-256 key: {}", e)))
}

/// Sign a JWT (`header.payload.signature`) with a P-256 key using ES256.
///
/// The signature is computed over `sha256(header_b64.payload_b64)` and
/// normalized to low-S form per the atproto convention.
fn sign_es256_jwt(
    signing_key: &SigningKey,
    header_json: &str,
    payload_json: &str,
) -> Result<String, SignerError> {
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

/// Verify a service auth token signature using ES256 (ECDSA with P-256).
///
/// # Arguments
///
/// * `token` - The JWT token to verify
/// * `public_key_multibase` - The public key in multibase format (z prefix = base58btc)
///
/// # Returns
///
/// `Ok(true)` if signature is valid, `Ok(false)` if invalid, `Err` if verification failed.
pub fn verify_service_auth_token(
    token: &str,
    public_key_multibase: &str,
) -> Result<bool, SignerError> {
    // Decode the multibase public key (z prefix = base58btc)
    if !public_key_multibase.starts_with('z') {
        return Err(SignerError::InvalidKey(
            "Public key must be multibase (base58btc, z prefix)".to_string(),
        ));
    }

    let public_key_with_prefix = bs58::decode(&public_key_multibase[1..])
        .into_vec()
        .map_err(|e| SignerError::InvalidKey(format!("Invalid base58: {}", e)))?;

    // Check for P-256 public key prefix (0x80 0x24) - compressed
    // or uncompressed prefix
    if public_key_with_prefix.len() < 2 {
        return Err(SignerError::InvalidKey("Public key too short".to_string()));
    }

    // Determine key format and extract bytes
    let public_key_bytes = if public_key_with_prefix[0] == 0x80 && public_key_with_prefix[1] == 0x24 {
        // Compressed P-256 public key with multicodec prefix
        &public_key_with_prefix[2..]
    } else {
        // Try without prefix
        &public_key_with_prefix[..]
    };

    // Create verifying key from bytes
    let verifying_key = VerifyingKey::from_sec1_bytes(public_key_bytes)
        .map_err(|e| SignerError::InvalidKey(format!("Invalid P-256 public key: {}", e)))?;

    // Parse the JWT parts
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(SignerError::EncodingError("Invalid JWT format".to_string()));
    }

    // Decode the signature
    let signature_bytes = BASE64URL
        .decode(parts[2])
        .map_err(|e| SignerError::EncodingError(format!("Invalid signature encoding: {}", e)))?;

    // Signature should be 64 bytes (32 bytes r + 32 bytes s)
    if signature_bytes.len() != 64 {
        return Err(SignerError::EncodingError(format!(
            "Invalid signature length: expected 64, got {}",
            signature_bytes.len()
        )));
    }

    // Create the signature from bytes
    let signature = p256::ecdsa::Signature::from_slice(&signature_bytes)
        .map_err(|e| SignerError::EncodingError(format!("Invalid signature format: {}", e)))?;

    // Create signing input (header.payload)
    let signing_input = format!("{}.{}", parts[0], parts[1]);

    // Hash the input
    let mut hasher = Sha256::new();
    hasher.update(signing_input.as_bytes());
    let hash: [u8; 32] = hasher.finalize().into();

    // Verify the signature
    match verifying_key.verify_prehash(&hash, &signature) {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
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

    /// Encode a P-256 signing key as a multibase (base58btc) private key with
    /// the `0x86 0x26` multicodec prefix, as stored in `UserPrivateKeyMultibase`.
    fn multibase_private_key(signing_key: &SigningKey) -> String {
        let mut bytes = vec![0x86u8, 0x26u8];
        bytes.extend_from_slice(&signing_key.to_bytes());
        format!("z{}", bs58::encode(bytes).into_string())
    }

    fn decode_jwt_part(part: &str) -> serde_json::Value {
        let bytes = BASE64URL.decode(part).expect("valid base64url");
        serde_json::from_slice(&bytes).expect("valid json")
    }

    #[test]
    fn test_sign_delegation_token_invalid_key() {
        let result = sign_delegation_token(
            "not-multibase",
            "did:plc:user",
            "at://did:plc:authority/space/my.bulletin.board/self",
            "did:plc:authority",
            60,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_sign_delegation_token_header_and_claims() {
        let signing_key = SigningKey::from_slice(&[0x42u8; 32]).unwrap();
        let private_key = multibase_private_key(&signing_key);
        let space_uri = "at://did:plc:authority/space/my.bulletin.board/self";

        let token = sign_delegation_token(
            &private_key,
            "did:plc:user",
            space_uri,
            "did:plc:authority",
            DELEGATION_TOKEN_TTL_SECS,
        )
        .expect("delegation token mints");

        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let header = decode_jwt_part(parts[0]);
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["typ"], DELEGATION_TOKEN_TYP);
        assert_eq!(header["kid"], "#atproto");

        let claims = decode_jwt_part(parts[1]);
        assert_eq!(claims["iss"], "did:plc:user");
        assert_eq!(claims["sub"], space_uri);
        assert_eq!(claims["aud"], "did:plc:authority#atproto_space_host");
        assert!(claims["jti"].is_string());
        let iat = claims["iat"].as_i64().unwrap();
        let exp = claims["exp"].as_i64().unwrap();
        assert_eq!(exp - iat, DELEGATION_TOKEN_TTL_SECS);

        // The signature verifies against the account's public key.
        let verifying_key = signing_key.verifying_key();
        let mut pub_bytes = vec![0x80u8, 0x24u8];
        pub_bytes.extend_from_slice(verifying_key.to_encoded_point(true).as_bytes());
        let public_key = format!("z{}", bs58::encode(pub_bytes).into_string());
        assert!(verify_service_auth_token(&token, &public_key).unwrap());
    }

    #[test]
    fn test_delegation_token_jti_is_unique() {
        let signing_key = SigningKey::from_slice(&[0x42u8; 32]).unwrap();
        let private_key = multibase_private_key(&signing_key);
        let space_uri = "at://did:plc:authority/space/my.bulletin.board/self";

        let first = sign_delegation_token(&private_key, "did:plc:user", space_uri, "did:plc:authority", 60).unwrap();
        let second = sign_delegation_token(&private_key, "did:plc:user", space_uri, "did:plc:authority", 60).unwrap();
        let jti_a = decode_jwt_part(first.split('.').nth(1).unwrap())["jti"].clone();
        let jti_b = decode_jwt_part(second.split('.').nth(1).unwrap())["jti"].clone();
        assert_ne!(jti_a, jti_b);
    }

    #[test]
    fn test_sign_space_credential_invalid_key() {
        let result = sign_space_credential(
            "not-multibase",
            "did:plc:authority",
            "at://did:plc:authority/space/my.bulletin.board/self",
            "0ZcOCORZNYy-DWpqq30jZyJGHTN0d2HglBV3uiguA4I",
            SPACE_CREDENTIAL_TTL_SECS,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_sign_space_credential_header_and_claims() {
        let signing_key = SigningKey::from_slice(&[0x42u8; 32]).unwrap();
        let private_key = multibase_private_key(&signing_key);
        let space_uri = "at://did:plc:authority/space/my.bulletin.board/self";
        let jkt = "0ZcOCORZNYy-DWpqq30jZyJGHTN0d2HglBV3uiguA4I";

        let token = sign_space_credential(
            &private_key,
            "did:plc:authority",
            space_uri,
            jkt,
            SPACE_CREDENTIAL_TTL_SECS,
        )
        .expect("space credential mints");

        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let header = decode_jwt_part(parts[0]);
        assert_eq!(header["alg"], "ES256");
        assert_eq!(header["typ"], SPACE_CREDENTIAL_TYP);
        assert_eq!(header["kid"], "#atproto");

        let claims = decode_jwt_part(parts[1]);
        assert_eq!(claims["iss"], "did:plc:authority");
        assert_eq!(claims["sub"], space_uri);
        assert_eq!(claims["cnf"]["jkt"], jkt);
        // A space credential is presented to any repo host, so it carries no aud.
        assert!(claims.get("aud").is_none());
        assert!(claims["jti"].is_string());
        let iat = claims["iat"].as_i64().unwrap();
        let exp = claims["exp"].as_i64().unwrap();
        assert_eq!(exp - iat, SPACE_CREDENTIAL_TTL_SECS);

        // The signature verifies against the authority's public key.
        let verifying_key = signing_key.verifying_key();
        let mut pub_bytes = vec![0x80u8, 0x24u8];
        pub_bytes.extend_from_slice(verifying_key.to_encoded_point(true).as_bytes());
        let public_key = format!("z{}", bs58::encode(pub_bytes).into_string());
        assert!(verify_service_auth_token(&token, &public_key).unwrap());
    }
}
