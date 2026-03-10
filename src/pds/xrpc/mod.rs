//! XRPC module for PDS.
//!
//! This module provides handlers for AT Protocol XRPC endpoints.
//! Each endpoint is implemented in its own submodule.

mod activate_account;
mod app_bsky_proxy;
mod apply_writes;
pub mod auth_helpers;
mod check_account_status;
mod create_record;
mod create_session;
mod deactivate_account;
mod delete_record;
mod describe_repo;
mod describe_server;
mod get_blob;
mod get_preferences;
mod get_record;
mod get_service_auth;
mod get_session;
mod health;
mod hello;
mod list_blobs;
mod list_records;
mod put_preferences;
mod put_record;
mod refresh_session;
mod resolve_handle;
mod subscribe_repos;
mod sync_get_record;
mod sync_get_repo;
mod sync_get_repo_status;
mod sync_list_repos;
mod upload_blob;
mod well_known_atproto_did;
mod well_known_did;

pub use activate_account::activate_account;
pub use app_bsky_proxy::{app_bsky_fallback, is_valid_outbound_host, is_valid_outbound_url};
pub use apply_writes::apply_writes;
pub use check_account_status::check_account_status;
pub use create_record::create_record;
pub use create_session::create_session;
pub use deactivate_account::deactivate_account;
pub use delete_record::delete_record;
pub use describe_repo::describe_repo;
pub use describe_server::describe_server;
pub use get_blob::get_blob;
pub use get_preferences::get_preferences;
pub use get_record::get_record;
pub use get_service_auth::get_service_auth;
pub use get_session::get_session;
pub use health::health;
pub use hello::hello;
pub use list_blobs::list_blobs;
pub use list_records::list_records;
pub use put_preferences::put_preferences;
pub use put_record::put_record;
pub use refresh_session::refresh_session;
pub use resolve_handle::resolve_handle;
pub use subscribe_repos::subscribe_repos;
pub use sync_get_record::sync_get_record;
pub use sync_get_repo::sync_get_repo;
pub use sync_get_repo_status::sync_get_repo_status;
pub use sync_list_repos::sync_list_repos;
pub use upload_blob::upload_blob;
pub use well_known_atproto_did::well_known_atproto_did;
pub use well_known_did::well_known_did;
