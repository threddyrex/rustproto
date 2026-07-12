//! Personal Data Server (PDS) implementation for atproto
//!
//! This module provides functionality for running a personal AT Protocol server,
//! including database operations, installation, and configuration.

pub mod admin;
pub mod auth;
pub mod background_jobs;
pub mod blob_db;
pub mod db;
pub mod firehose_event_generator;
pub mod http_utils;
pub mod installer;
pub mod oauth;
pub mod server;
pub mod user_repo;
pub mod xrpc;

