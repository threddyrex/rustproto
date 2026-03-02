//! JWT token generation and validation for AT Protocol authentication.
//!
//! This module provides functions to generate and validate JWT tokens
//! for the legacy (non-OAuth) authentication flow.

use chrono::{Duration, Utc};
use jsonwebtoken::{
    decode, encode, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Claims for an access JWT token.
#[derive(Debug, Serialize, Deserialize)]
pub struct AccessClaims {
    /// Token scope - always "com.atproto.access" for access tokens.
    pub scope: String,
    /// Audience - the PDS DID.
    pub aud: String,
    /// Subject - the user's DID.
    pub sub: String,
    /// Issued at timestamp (Unix seconds).
    pub iat: i64,
    /// Expiration timestamp (Unix seconds).
    pub exp: i64,
    /// JWT ID - unique identifier for the token.
    pub jti: String,
}

/// Claims for a refresh JWT token.
#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshClaims {
    /// Token scope - always "com.atproto.refresh" for refresh tokens.
    pub scope: String,
    /// Audience - the PDS DID.
    pub aud: String,
    /// Subject - the user's DID.
    pub sub: String,
    /// Issued at timestamp (Unix seconds).
    pub iat: i64,
    /// Expiration timestamp (Unix seconds).
    pub exp: i64,
    /// JWT ID - unique identifier for the token.
    pub jti: String,
}

/// Result of JWT validation.
pub struct JwtValidationResult {
    /// Whether the token is valid.
    pub is_valid: bool,
    /// The subject (user DID) from the token.
    pub sub: Option<String>,
    /// Error message if validation failed.
    pub error: Option<String>,
}

/// Generate an access JWT token.
///
/// Access tokens are short-lived (2 hours) and used for API authentication.
///
/// # Arguments
///
/// * `user_did` - The DID of the user
/// * `pds_did` - The DID of the PDS server
/// * `jwt_secret` - The secret key for signing
///
/// # Returns
///
/// The signed JWT token string, or None if inputs are invalid.
pub fn generate_access_jwt(user_did: &str, pds_did: &str, jwt_secret: &str) -> Option<String> {
    if user_did.is_empty() || pds_did.is_empty() || jwt_secret.is_empty() {
        return None;
    }

    let now = Utc::now();
    let expiry = now + Duration::hours(2);
    let jti = Uuid::new_v4().to_string();

    let claims = AccessClaims {
        scope: "com.atproto.access".to_string(),
        aud: pds_did.to_string(),
        sub: user_did.to_string(),
        iat: now.timestamp(),
        exp: expiry.timestamp(),
        jti,
    };

    // Create header with typ: "at+jwt"
    let mut header = Header::new(Algorithm::HS256);
    header.typ = Some("at+jwt".to_string());

    let key = EncodingKey::from_secret(jwt_secret.as_bytes());

    encode(&header, &claims, &key).ok()
}

/// Generate a refresh JWT token.
///
/// Refresh tokens are long-lived (90 days) and used to obtain new access tokens.
///
/// # Arguments
///
/// * `user_did` - The DID of the user
/// * `pds_did` - The DID of the PDS server
/// * `jwt_secret` - The secret key for signing
///
/// # Returns
///
/// The signed JWT token string, or None if inputs are invalid.
pub fn generate_refresh_jwt(user_did: &str, pds_did: &str, jwt_secret: &str) -> Option<String> {
    if user_did.is_empty() || pds_did.is_empty() || jwt_secret.is_empty() {
        return None;
    }

    let now = Utc::now();
    let expiry = now + Duration::days(90);

    // Generate a unique JTI using random bytes
    let jti_bytes: [u8; 32] = rand::random();
    let jti = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, jti_bytes);

    let claims = RefreshClaims {
        scope: "com.atproto.refresh".to_string(),
        aud: pds_did.to_string(),
        sub: user_did.to_string(),
        iat: now.timestamp(),
        exp: expiry.timestamp(),
        jti,
    };

    // Create header with typ: "refresh+jwt"
    let mut header = Header::new(Algorithm::HS256);
    header.typ = Some("refresh+jwt".to_string());

    let key = EncodingKey::from_secret(jwt_secret.as_bytes());

    encode(&header, &claims, &key).ok()
}

/// Validate an access JWT token.
///
/// # Arguments
///
/// * `access_jwt` - The access JWT token to validate
/// * `jwt_secret` - The secret key for verification
/// * `user_did` - The expected user DID
/// * `validate_expiry` - Whether to check token expiration
///
/// # Returns
///
/// A JwtValidationResult indicating whether the token is valid.
pub fn validate_access_jwt(
    access_jwt: &str,
    jwt_secret: &str,
    user_did: &str,
    validate_expiry: bool,
) -> JwtValidationResult {
    if access_jwt.is_empty() || jwt_secret.is_empty() || user_did.is_empty() {
        return JwtValidationResult {
            is_valid: false,
            sub: None,
            error: Some("Empty input".to_string()),
        };
    }

    let key = DecodingKey::from_secret(jwt_secret.as_bytes());
    let mut validation = Validation::new(Algorithm::HS256);

    // Disable audience validation since we check manually
    validation.validate_aud = false;

    // Optionally disable expiry validation
    if !validate_expiry {
        validation.validate_exp = false;
    }

    let token_data: Result<TokenData<AccessClaims>, _> = decode(access_jwt, &key, &validation);

    match token_data {
        Ok(data) => {
            // Verify scope
            if data.claims.scope != "com.atproto.access" {
                return JwtValidationResult {
                    is_valid: false,
                    sub: Some(data.claims.sub),
                    error: Some("Invalid scope".to_string()),
                };
            }

            // Verify DID
            if data.claims.sub != user_did {
                return JwtValidationResult {
                    is_valid: false,
                    sub: Some(data.claims.sub),
                    error: Some("DID mismatch".to_string()),
                };
            }

            JwtValidationResult {
                is_valid: true,
                sub: Some(data.claims.sub),
                error: None,
            }
        }
        Err(e) => JwtValidationResult {
            is_valid: false,
            sub: None,
            error: Some(format!("JWT decode error: {}", e)),
        },
    }
}

/// Validate a refresh JWT token.
///
/// # Arguments
///
/// * `refresh_jwt` - The refresh JWT token to validate
/// * `jwt_secret` - The secret key for verification
///
/// # Returns
///
/// A JwtValidationResult indicating whether the token is valid.
pub fn validate_refresh_jwt(refresh_jwt: &str, jwt_secret: &str) -> JwtValidationResult {
    if refresh_jwt.is_empty() || jwt_secret.is_empty() {
        return JwtValidationResult {
            is_valid: false,
            sub: None,
            error: Some("Empty input".to_string()),
        };
    }

    let key = DecodingKey::from_secret(jwt_secret.as_bytes());
    let mut validation = Validation::new(Algorithm::HS256);

    // Disable audience validation since we check manually
    validation.validate_aud = false;

    let token_data: Result<TokenData<RefreshClaims>, _> = decode(refresh_jwt, &key, &validation);

    match token_data {
        Ok(data) => {
            // Verify scope
            if data.claims.scope != "com.atproto.refresh" {
                return JwtValidationResult {
                    is_valid: false,
                    sub: Some(data.claims.sub),
                    error: Some("Invalid scope".to_string()),
                };
            }

            JwtValidationResult {
                is_valid: true,
                sub: Some(data.claims.sub),
                error: None,
            }
        }
        Err(e) => JwtValidationResult {
            is_valid: false,
            sub: None,
            error: Some(format!("JWT decode error: {}", e)),
        },
    }
}

/// Extract the subject (DID) from an access token without full validation.
///
/// This is useful for getting the DID even from an expired token.
#[allow(dead_code)]
pub fn get_did_from_access_jwt(access_jwt: &str, jwt_secret: &str) -> Option<String> {
    if access_jwt.is_empty() || jwt_secret.is_empty() {
        return None;
    }

    let key = DecodingKey::from_secret(jwt_secret.as_bytes());
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_aud = false;
    validation.validate_exp = false;

    let token_data: Result<TokenData<AccessClaims>, _> = decode(access_jwt, &key, &validation);

    token_data.ok().map(|data| data.claims.sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_USER_DID: &str = "did:plc:testuser123";
    const TEST_PDS_DID: &str = "did:web:test.pds.com";
    const TEST_SECRET: &str = "test-jwt-secret-key-256-bits-long";

    #[test]
    fn test_generate_access_jwt() {
        let token = generate_access_jwt(TEST_USER_DID, TEST_PDS_DID, TEST_SECRET);
        assert!(token.is_some());

        let token = token.unwrap();
        assert!(!token.is_empty());

        // Validate the token
        let result = validate_access_jwt(&token, TEST_SECRET, TEST_USER_DID, true);
        assert!(result.is_valid);
        assert_eq!(result.sub, Some(TEST_USER_DID.to_string()));
    }

    #[test]
    fn test_generate_refresh_jwt() {
        let token = generate_refresh_jwt(TEST_USER_DID, TEST_PDS_DID, TEST_SECRET);
        assert!(token.is_some());

        let token = token.unwrap();
        assert!(!token.is_empty());

        // Validate the token
        let result = validate_refresh_jwt(&token, TEST_SECRET);
        assert!(result.is_valid);
        assert_eq!(result.sub, Some(TEST_USER_DID.to_string()));
    }

    #[test]
    fn test_invalid_secret() {
        let token = generate_access_jwt(TEST_USER_DID, TEST_PDS_DID, TEST_SECRET).unwrap();

        // Try to validate with wrong secret
        let result = validate_access_jwt(&token, "wrong-secret", TEST_USER_DID, true);
        assert!(!result.is_valid);
    }

    #[test]
    fn test_did_mismatch() {
        let token = generate_access_jwt(TEST_USER_DID, TEST_PDS_DID, TEST_SECRET).unwrap();

        // Try to validate with wrong DID
        let result = validate_access_jwt(&token, TEST_SECRET, "did:plc:wronguser", true);
        assert!(!result.is_valid);
        assert!(result.error.unwrap().contains("DID mismatch"));
    }

    #[test]
    fn test_empty_inputs() {
        assert!(generate_access_jwt("", TEST_PDS_DID, TEST_SECRET).is_none());
        assert!(generate_access_jwt(TEST_USER_DID, "", TEST_SECRET).is_none());
        assert!(generate_access_jwt(TEST_USER_DID, TEST_PDS_DID, "").is_none());
    }
}
