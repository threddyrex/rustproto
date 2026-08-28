//! auth utilities
//!
//! This module provides JWT token generation, validation, and password
//! verification utilities for AT Protocol authentication.

mod auth_flow;
mod jwt;
mod password;
mod signer;

pub use auth_flow::{
    AuthType,
    check_user_auth, check_user_auth_with_lxm_async, check_legacy_auth, 
    auth_failure_response,
    extract_atproto_public_key, extract_bearer_token};


pub use jwt::{
    generate_access_jwt, generate_refresh_jwt, validate_access_jwt, validate_refresh_jwt,
    JwtValidationResult,
};
pub use password::verify_password;
pub use signer::{
    sign_delegation_token, sign_service_auth_token, sign_space_credential, verify_service_auth_token,
    SignerError, DELEGATION_TOKEN_TTL_SECS, DELEGATION_TOKEN_TYP, SPACE_CREDENTIAL_TTL_SECS,
    SPACE_CREDENTIAL_TYP,
};
