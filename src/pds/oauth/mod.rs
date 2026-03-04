//! OAuth module for PDS.
//!
//! This module provides handlers for OAuth 2.0 endpoints as specified
//! in the AT Protocol OAuth specification.
//!
//! Endpoints:
//! - `/.well-known/oauth-protected-resource` - OAuth protected resource metadata
//! - `/.well-known/oauth-authorization-server` - OAuth authorization server metadata
//! - `/oauth/jwks` - JSON Web Key Set
//! - `/oauth/par` - Pushed Authorization Request
//! - `/oauth/authorize` - Authorization endpoint (GET/POST)
//! - `/oauth/token` - Token endpoint
//! - `/oauth/passkeyauthenticationoptions` - WebAuthn passkey authentication options
//! - `/oauth/authenticatepasskey` - WebAuthn passkey authentication
//! - `/oauth/revoke` - Token revocation endpoint

mod authenticate_passkey;
mod authorization_server;
mod authorize_get;
mod authorize_post;
mod dpop;
mod helpers;
mod jwks;
mod par;
mod passkey_auth_options;
mod protected_resource;
mod revoke;
mod token;

pub use authenticate_passkey::authenticate_passkey;
pub use authorization_server::oauth_authorization_server;
pub use authorize_get::oauth_authorize_get;
pub use authorize_post::oauth_authorize_post;
pub use dpop::{validate_dpop, DpopValidationResult};
pub use helpers::*;
pub use jwks::oauth_jwks;
pub use par::oauth_par;
pub use passkey_auth_options::passkey_authentication_options;
pub use protected_resource::oauth_protected_resource;
pub use revoke::oauth_revoke;
pub use token::oauth_token;
