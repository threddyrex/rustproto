//! XRPC module for PDS.
//!
//! This module provides handlers for AT Protocol XRPC endpoints.
//! Each endpoint is implemented in its own submodule.

mod activate_account;
mod auth_helpers;
mod check_account_status;
mod create_session;
mod deactivate_account;
mod describe_server;
mod get_service_auth;
mod get_session;
mod health;
mod hello;
mod refresh_session;
mod resolve_handle;

pub use activate_account::activate_account;
pub use check_account_status::check_account_status;
pub use create_session::create_session;
pub use deactivate_account::deactivate_account;
pub use describe_server::describe_server;
pub use get_service_auth::get_service_auth;
pub use get_session::get_session;
pub use health::health;
pub use hello::hello;
pub use refresh_session::refresh_session;
pub use resolve_handle::resolve_handle;
