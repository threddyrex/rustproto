//! Authentication module for PDS.
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
pub use signer::{sign_service_auth_token, SignerError};
