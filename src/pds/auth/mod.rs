//! auth utilities
//!
//! This module provides JWT token generation, validation, and password
//! verification utilities for AT Protocol authentication.

mod jwt;
mod password;
mod signer;

pub use jwt::{
    generate_access_jwt, generate_refresh_jwt, validate_access_jwt, validate_refresh_jwt,
    JwtValidationResult,
};
pub use password::verify_password;
pub use signer::{
    sign_delegation_token, sign_service_auth_token, sign_space_credential, verify_service_auth_token,
    SignerError, DELEGATION_TOKEN_TTL_SECS, DELEGATION_TOKEN_TYP, SPACE_CREDENTIAL_TTL_SECS,
};
