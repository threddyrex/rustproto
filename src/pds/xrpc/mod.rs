//! XRPC module for PDS.
//!
//! This module provides handlers for AT Protocol XRPC endpoints.
//! Each endpoint is implemented in its own submodule.

mod describe_server;
mod health;
mod hello;
mod resolve_handle;

pub use describe_server::describe_server;
pub use health::health;
pub use hello::hello;
pub use resolve_handle::resolve_handle;
