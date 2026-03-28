//! rustproto - Rust AT Protocol / Bluesky SDK
//!
//! This crate provides utilities for working with the AT Protocol and Bluesky,
//! including actor resolution, identity lookup, repository parsing, and related functionality.
//!
pub mod cli;
pub mod firehose;
pub mod fs;
pub mod log;
pub mod mst;
pub mod pds;
pub mod repo;
pub mod ws;

// Re-export commonly used types at crate root
pub use firehose::{Firehose, FirehoseError};
pub use fs::LocalFileSystem;
pub use log::{init_logger, logger, ConsoleDestination, FileDestination, LogDestination, LogLevel, Logger};
pub use mst::{Mst, MstEntry, MstItem, MstNode};
pub use pds::{
    AdminSession, Blob, DbRepoCommit, DbRepoHeader, DbRepoRecord, FirehoseEvent, Installer,
    LegacySession, OauthRequest, OauthSession, Passkey, PasskeyChallenge, PdsDb, PdsDbError,
    SqliteDb, Statistic, StatisticKey,
};
pub use repo::{Repo, RepoHeader, RepoRecord, RepoMst, MstNodeKey, CidV1, DagCborObject, VarInt};
pub use ws::{ActorInfo, ActorQueryOptions, BlueskyClient, BlueskyClientError};
