//! PDS (Personal Data Server) module.
//!
//! This module provides functionality for running a personal AT Protocol server,
//! including database operations, installation, and configuration.

pub mod db;
pub mod installer;

pub use db::{PdsDb, PdsDbError, SqliteDb};
pub use installer::Installer;
