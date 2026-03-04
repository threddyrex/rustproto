//! DPoP (Demonstrating Proof of Possession) validation.
//!
//! Implements RFC 9449 DPoP proof validation for OAuth 2.0.
//! See: https://datatracker.ietf.org/doc/html/rfc9449

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use p256::ecdsa::{Signature as P256Signature, VerifyingKey as P256VerifyingKey, signature::Verifier};
use k256::ecdsa::{Signature as K256Signature, VerifyingKey as K256VerifyingKey};
use sha2::{Sha256, Digest};

/// Result of DPoP validation.
#[derive(Debug, Default)]
pub struct DpopValidationResult {
    /// Whether the DPoP proof is valid.
    pub is_valid: bool,
    /// Error message if validation failed.
    pub error: Option<String>,
    /// JWK thumbprint (RFC 7638) for binding tokens.
    pub jwk_thumbprint: Option<String>,
    /// Debug info for troubleshooting.
    pub debug_info: Option<String>,
    /// The JTI (unique identifier) from the proof.
    pub jti: Option<String>,
    /// The HTTP method (htm) from the proof.
    pub htm: Option<String>,
    /// The HTTP URI (htu) from the proof.
    pub htu: Option<String>,
    /// The issued-at timestamp (iat) from the proof.
    pub iat: Option<i64>,
}

/// Validates a DPoP (Demonstrating Proof of Possession) proof.
///
/// # Arguments
///
/// * `dpop` - The DPoP JWT from the DPoP header
/// * `expected_method` - Expected HTTP method (e.g., "POST")
/// * `expected_uri` - Expected HTTP URI (e.g., "https://pds.example.com/oauth/par")
/// * `max_age_seconds` - Maximum age of the proof in seconds (default: 300)
///
/// # Returns
///
/// A DpopValidationResult with the validation result and parsed claims.
pub fn validate_dpop(
    dpop: Option<&str>,
    expected_method: &str,
    expected_uri: &str,
    max_age_seconds: i64,
) -> DpopValidationResult {
    let mut result = DpopValidationResult::default();

    let dpop = match dpop {
        Some(d) if !d.is_empty() => d,
        _ => {
            result.error = Some("DPoP header is missing".to_string());
            return result;
        }
    };

    // Split JWT into parts
    let parts: Vec<&str> = dpop.split('.').collect();
    if parts.len() != 3 {
        result.error = Some("DPoP proof is not a valid JWT (expected 3 parts)".to_string());
        return result;
    }

    // Decode header
    let header_json = match URL_SAFE_NO_PAD.decode(parts[0]) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => {
                result.error = Some("Invalid DPoP header encoding".to_string());
                return result;
            }
        },
        Err(_) => {
            result.error = Some("Invalid DPoP header base64".to_string());
            return result;
        }
    };

    let header: serde_json::Value = match serde_json::from_str(&header_json) {
        Ok(v) => v,
        Err(_) => {
            result.error = Some("Invalid DPoP header JSON".to_string());
            return result;
        }
    };

    // Validate typ
    let typ = header.get("typ").and_then(|v| v.as_str());
    if typ != Some("dpop+jwt") {
        result.error = Some(format!("Invalid DPoP typ: expected 'dpop+jwt', got {:?}", typ));
        return result;
    }

    // Validate alg (must be asymmetric)
    let alg = match header.get("alg").and_then(|v| v.as_str()) {
        Some(a) => a,
        None => {
            result.error = Some("DPoP header missing 'alg'".to_string());
            return result;
        }
    };

    if !is_allowed_dpop_algorithm(alg) {
        result.error = Some(format!("Invalid or unsupported DPoP algorithm: '{}'", alg));
        return result;
    }

    // Extract JWK from header
    let jwk = match header.get("jwk") {
        Some(j) => j,
        None => {
            result.error = Some("DPoP header missing 'jwk'".to_string());
            return result;
        }
    };

    // Decode payload
    let payload_json = match URL_SAFE_NO_PAD.decode(parts[1]) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => {
                result.error = Some("Invalid DPoP payload encoding".to_string());
                return result;
            }
        },
        Err(_) => {
            result.error = Some("Invalid DPoP payload base64".to_string());
            return result;
        }
    };

    let payload: serde_json::Value = match serde_json::from_str(&payload_json) {
        Ok(v) => v,
        Err(_) => {
            result.error = Some("Invalid DPoP payload JSON".to_string());
            return result;
        }
    };

    // Extract and validate jti
    let jti = match payload.get("jti").and_then(|v| v.as_str()) {
        Some(j) => j.to_string(),
        None => {
            result.error = Some("DPoP payload missing 'jti'".to_string());
            return result;
        }
    };
    result.jti = Some(jti);

    // Extract and validate htm
    let htm = match payload.get("htm").and_then(|v| v.as_str()) {
        Some(h) => h.to_string(),
        None => {
            result.error = Some("DPoP payload missing 'htm'".to_string());
            return result;
        }
    };

    if !htm.eq_ignore_ascii_case(expected_method) {
        result.error = Some(format!(
            "DPoP htm mismatch: expected '{}', got '{}'",
            expected_method, htm
        ));
        return result;
    }
    result.htm = Some(htm);

    // Extract and validate htu
    let htu = match payload.get("htu").and_then(|v| v.as_str()) {
        Some(h) => h.to_string(),
        None => {
            result.error = Some("DPoP payload missing 'htu'".to_string());
            return result;
        }
    };

    if !compare_dpop_htu(&htu, expected_uri) {
        result.error = Some(format!(
            "DPoP htu mismatch: expected '{}', got '{}'",
            expected_uri, htu
        ));
        return result;
    }
    result.htu = Some(htu);

    // Extract and validate iat
    let iat = match payload.get("iat").and_then(|v| v.as_i64()) {
        Some(i) => i,
        None => {
            result.error = Some("DPoP payload missing 'iat'".to_string());
            return result;
        }
    };
    result.iat = Some(iat);

    // Check iat timing
    let now = chrono::Utc::now().timestamp();
    if iat > now + 60 {
        // Allow 60 seconds clock skew
        result.error = Some("DPoP iat is in the future".to_string());
        return result;
    }
    if now - iat > max_age_seconds {
        result.error = Some(format!("DPoP proof is too old ({} seconds)", now - iat));
        return result;
    }

    // Verify signature using embedded JWK
    let signed_data = format!("{}.{}", parts[0], parts[1]);
    let signature_bytes = match URL_SAFE_NO_PAD.decode(parts[2]) {
        Ok(b) => b,
        Err(_) => {
            result.error = Some("Invalid DPoP signature base64".to_string());
            return result;
        }
    };

    let sig_valid = verify_dpop_signature(&signed_data, &signature_bytes, alg, jwk);
    if let Err(e) = sig_valid {
        result.error = Some(format!("DPoP signature verification failed: {}", e));
        result.debug_info = Some(format!("alg={}, kty={:?}", alg, jwk.get("kty")));
        return result;
    }

    // Calculate JWK thumbprint (RFC 7638)
    match calculate_jwk_thumbprint(jwk) {
        Ok(thumbprint) => result.jwk_thumbprint = Some(thumbprint),
        Err(e) => {
            result.error = Some(format!("Failed to calculate JWK thumbprint: {}", e));
            return result;
        }
    }

    result.is_valid = true;
    result
}

/// Check if algorithm is allowed for DPoP (asymmetric only).
fn is_allowed_dpop_algorithm(alg: &str) -> bool {
    matches!(
        alg,
        "RS256"
            | "RS384"
            | "RS512"
            | "PS256"
            | "PS384"
            | "PS512"
            | "ES256"
            | "ES256K"
            | "ES384"
            | "ES512"
    )
}

/// Compare htu values, ignoring query string and fragment per RFC 9449.
fn compare_dpop_htu(htu: &str, expected_uri: &str) -> bool {
    // Parse both URIs and compare scheme, host, port, and path
    let htu_parsed = match url::Url::parse(htu) {
        Ok(u) => u,
        Err(_) => return htu.eq_ignore_ascii_case(expected_uri),
    };

    let expected_parsed = match url::Url::parse(expected_uri) {
        Ok(u) => u,
        Err(_) => return htu.eq_ignore_ascii_case(expected_uri),
    };

    htu_parsed.scheme().eq_ignore_ascii_case(expected_parsed.scheme())
        && htu_parsed.host_str() == expected_parsed.host_str()
        && htu_parsed.port() == expected_parsed.port()
        && htu_parsed.path() == expected_parsed.path()
}

/// Verify DPoP JWT signature using embedded JWK.
fn verify_dpop_signature(
    signed_data: &str,
    signature: &[u8],
    alg: &str,
    jwk: &serde_json::Value,
) -> Result<(), String> {
    let kty = jwk
        .get("kty")
        .and_then(|v| v.as_str())
        .ok_or("JWK missing 'kty'")?;

    match kty {
        "EC" => verify_ec_signature(signed_data, signature, alg, jwk),
        "RSA" => verify_rsa_signature(signed_data, signature, alg, jwk),
        _ => Err(format!("Unsupported key type: {}", kty)),
    }
}

/// Verify EC signature (ES256, ES384, ES512).
fn verify_ec_signature(
    signed_data: &str,
    signature: &[u8],
    alg: &str,
    jwk: &serde_json::Value,
) -> Result<(), String> {
    let x = jwk
        .get("x")
        .and_then(|v| v.as_str())
        .ok_or("JWK missing 'x'")?;
    let y = jwk
        .get("y")
        .and_then(|v| v.as_str())
        .ok_or("JWK missing 'y'")?;

    let x_bytes = URL_SAFE_NO_PAD
        .decode(x)
        .map_err(|_| "Invalid x coordinate")?;
    let y_bytes = URL_SAFE_NO_PAD
        .decode(y)
        .map_err(|_| "Invalid y coordinate")?;

    // Get curve from JWK
    let crv = jwk.get("crv").and_then(|v| v.as_str());

    // Support ES256 (P-256) and ES256K (secp256k1)
    match crv.unwrap_or(alg) {
        "P-256" | "ES256" => verify_es256_signature(signed_data, signature, &x_bytes, &y_bytes),
        "secp256k1" | "ES256K" => verify_es256k_signature(signed_data, signature, &x_bytes, &y_bytes),
        _ => Err(format!("Unsupported EC curve/algorithm: {:?}/{}", crv, alg)),
    }
}

/// Verify ES256 (P-256 / secp256r1) signature.
fn verify_es256_signature(
    signed_data: &str,
    signature: &[u8],
    x_bytes: &[u8],
    y_bytes: &[u8],
) -> Result<(), String> {
    // Pad coordinates to 32 bytes if needed
    let x_padded = pad_bytes(x_bytes, 32);
    let y_padded = pad_bytes(y_bytes, 32);

    // Create public key point (uncompressed format: 04 || x || y)
    let mut point_bytes = vec![0x04];
    point_bytes.extend_from_slice(&x_padded);
    point_bytes.extend_from_slice(&y_padded);

    let verifying_key = P256VerifyingKey::from_sec1_bytes(&point_bytes)
        .map_err(|e| format!("Invalid EC public key: {}", e))?;

    // Parse the signature (IEEE P1363 format: r || s)
    let sig = P256Signature::from_slice(signature)
        .map_err(|e| format!("Invalid signature format: {}", e))?;

    // Try to normalize the signature to low-S form if needed
    // Some clients send high-S signatures which some verifiers reject
    let normalized_sig = sig.normalize_s().unwrap_or(sig);

    // Verify
    verifying_key
        .verify(signed_data.as_bytes(), &normalized_sig)
        .map_err(|e| format!("Signature verification failed: {}", e))
}

/// Verify ES256K (secp256k1) signature.
fn verify_es256k_signature(
    signed_data: &str,
    signature: &[u8],
    x_bytes: &[u8],
    y_bytes: &[u8],
) -> Result<(), String> {
    // Pad coordinates to 32 bytes if needed
    let x_padded = pad_bytes(x_bytes, 32);
    let y_padded = pad_bytes(y_bytes, 32);

    // Create public key point (uncompressed format: 04 || x || y)
    let mut point_bytes = vec![0x04];
    point_bytes.extend_from_slice(&x_padded);
    point_bytes.extend_from_slice(&y_padded);

    let verifying_key = K256VerifyingKey::from_sec1_bytes(&point_bytes)
        .map_err(|e| format!("Invalid secp256k1 public key: {}", e))?;

    // Parse the signature (IEEE P1363 format: r || s)
    let sig = K256Signature::from_slice(signature)
        .map_err(|e| format!("Invalid ES256K signature format: {}", e))?;

    // Try to normalize the signature to low-S form if needed
    // Some clients send high-S signatures which some verifiers reject
    let normalized_sig = sig.normalize_s().unwrap_or(sig);

    // Verify
    verifying_key
        .verify(signed_data.as_bytes(), &normalized_sig)
        .map_err(|e| format!("ES256K signature verification failed: {}", e))
}

/// Verify RSA signature (RS256, PS256, etc.).
fn verify_rsa_signature(
    signed_data: &str,
    signature: &[u8],
    alg: &str,
    jwk: &serde_json::Value,
) -> Result<(), String> {
    let n = jwk
        .get("n")
        .and_then(|v| v.as_str())
        .ok_or("JWK missing 'n'")?;
    let e = jwk
        .get("e")
        .and_then(|v| v.as_str())
        .ok_or("JWK missing 'e'")?;

    let n_bytes = URL_SAFE_NO_PAD
        .decode(n)
        .map_err(|_| "Invalid modulus")?;
    let e_bytes = URL_SAFE_NO_PAD
        .decode(e)
        .map_err(|_| "Invalid exponent")?;

    // Use rsa crate for RSA verification
    use rsa::{RsaPublicKey, BigUint, pkcs1v15::VerifyingKey as RsaVerifyingKey, signature::Verifier as RsaVerifier};
    
    let n_int = BigUint::from_bytes_be(&n_bytes);
    let e_int = BigUint::from_bytes_be(&e_bytes);
    
    let public_key = RsaPublicKey::new(n_int, e_int)
        .map_err(|e| format!("Invalid RSA public key: {}", e))?;

    match alg {
        "RS256" => {
            use rsa::sha2::Sha256;
            use rsa::pkcs1v15::Signature;
            
            let verifying_key = RsaVerifyingKey::<Sha256>::new(public_key);
            let sig = Signature::try_from(signature)
                .map_err(|e| format!("Invalid RS256 signature: {}", e))?;
            verifying_key
                .verify(signed_data.as_bytes(), &sig)
                .map_err(|e| format!("RS256 verification failed: {}", e))
        }
        _ => Err(format!("Unsupported RSA algorithm: {}", alg)),
    }
}

/// Pad byte array to expected size (prepends zeros).
fn pad_bytes(data: &[u8], size: usize) -> Vec<u8> {
    if data.len() >= size {
        return data.to_vec();
    }
    let mut padded = vec![0u8; size];
    padded[size - data.len()..].copy_from_slice(data);
    padded
}

/// Calculate JWK thumbprint per RFC 7638.
pub fn calculate_jwk_thumbprint(jwk: &serde_json::Value) -> Result<String, String> {
    let kty = jwk
        .get("kty")
        .and_then(|v| v.as_str())
        .ok_or("JWK missing 'kty'")?;

    let canonical_json = match kty {
        "EC" => {
            let crv = jwk.get("crv").and_then(|v| v.as_str()).ok_or("JWK missing 'crv'")?;
            let x = jwk.get("x").and_then(|v| v.as_str()).ok_or("JWK missing 'x'")?;
            let y = jwk.get("y").and_then(|v| v.as_str()).ok_or("JWK missing 'y'")?;
            format!(r#"{{"crv":"{}","kty":"EC","x":"{}","y":"{}"}}"#, crv, x, y)
        }
        "RSA" => {
            let e = jwk.get("e").and_then(|v| v.as_str()).ok_or("JWK missing 'e'")?;
            let n = jwk.get("n").and_then(|v| v.as_str()).ok_or("JWK missing 'n'")?;
            format!(r#"{{"e":"{}","kty":"RSA","n":"{}"}}"#, e, n)
        }
        _ => return Err(format!("Unsupported key type: {}", kty)),
    };

    let mut hasher = Sha256::new();
    hasher.update(canonical_json.as_bytes());
    let hash = hasher.finalize();

    Ok(URL_SAFE_NO_PAD.encode(hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_allowed_dpop_algorithm() {
        assert!(is_allowed_dpop_algorithm("ES256"));
        assert!(is_allowed_dpop_algorithm("ES256K")); // secp256k1
        assert!(is_allowed_dpop_algorithm("RS256"));
        assert!(is_allowed_dpop_algorithm("PS256"));
        assert!(!is_allowed_dpop_algorithm("HS256")); // Symmetric not allowed
        assert!(!is_allowed_dpop_algorithm("none"));
    }

    #[test]
    fn test_compare_dpop_htu() {
        // Same URL
        assert!(compare_dpop_htu(
            "https://example.com/oauth/par",
            "https://example.com/oauth/par"
        ));

        // Query string should be ignored
        assert!(compare_dpop_htu(
            "https://example.com/oauth/par?foo=bar",
            "https://example.com/oauth/par"
        ));

        // Different path
        assert!(!compare_dpop_htu(
            "https://example.com/oauth/token",
            "https://example.com/oauth/par"
        ));
    }

    #[test]
    fn test_validate_dpop_missing() {
        let result = validate_dpop(None, "POST", "https://example.com/oauth/par", 300);
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("missing"));
    }
}
