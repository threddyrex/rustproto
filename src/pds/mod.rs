//! PDS (Personal Data Server) module.
//!
//! This module provides functionality for running a personal AT Protocol server,
//! including database operations, installation, and configuration.

pub mod admin;
pub mod auth;
pub mod blob_db;
pub mod db;
pub mod firehose_event_generator;
pub mod installer;
pub mod oauth;
pub mod server;
pub mod user_repo;
pub mod xrpc;

pub use db::{
    AdminSession, Blob, DbRepoCommit, DbRepoHeader, DbRepoRecord, FirehoseEvent, LegacySession,
    OauthRequest, OauthSession, Passkey, PasskeyChallenge, PdsDb, PdsDbError, SqliteDb, Statistic,
    StatisticKey,
};
pub use firehose_event_generator::FirehoseEventGenerator;
pub use installer::Installer;
pub use server::{PdsServer, PdsServerError, PdsState};
pub use user_repo::{ApplyWritesOperation, ApplyWritesResult, UserRepo, UserRepoError};
