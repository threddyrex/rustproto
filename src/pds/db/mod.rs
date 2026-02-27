//! Database module for PDS.
//!
//! This module provides SQLite database operations for the PDS,
//! including table creation, configuration storage, and data access.

mod pds_db;
mod sqlite_db;

pub use pds_db::{PdsDb, PdsDbError};
pub use sqlite_db::SqliteDb;
