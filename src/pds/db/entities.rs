//! Database entity types for PDS.
//!
//! This module defines the data structures used to store and retrieve
//! data from the PDS SQLite database.

use chrono::{DateTime, Utc};

/// A blob stored in the PDS.
#[derive(Debug, Clone)]
pub struct Blob {
    /// Content identifier (CID) of the blob.
    pub cid: String,
    /// MIME content type.
    pub content_type: String,
    /// Size in bytes.
    pub content_length: i32,
}

/// An OAuth authorization request.
#[derive(Debug, Clone)]
pub struct OauthRequest {
    /// Unique request URI identifier.
    pub request_uri: String,
    /// Expiration date in ISO 8601 format.
    pub expires_date: String,
    /// DPoP proof.
    pub dpop: String,
    /// Request body.
    pub body: String,
    /// Authorization code (set after user authorizes).
    pub authorization_code: Option<String>,
    /// Authentication type (e.g., "Legacy", "Passkey").
    pub auth_type: Option<String>,
}

/// An OAuth session.
#[derive(Debug, Clone)]
pub struct OauthSession {
    /// Unique session identifier.
    pub session_id: String,
    /// OAuth client identifier.
    pub client_id: String,
    /// Granted scopes.
    pub scope: String,
    /// DPoP JWK thumbprint for token binding.
    pub dpop_jwk_thumbprint: String,
    /// Refresh token value.
    pub refresh_token: String,
    /// Refresh token expiration date in ISO 8601 format.
    pub refresh_token_expires_date: String,
    /// Session creation date in ISO 8601 format.
    pub created_date: String,
    /// Client IP address.
    pub ip_address: String,
    /// Authentication type (e.g., "Legacy", "Passkey").
    pub auth_type: String,
}

/// A legacy (non-OAuth) session.
#[derive(Debug, Clone)]
pub struct LegacySession {
    /// Session creation date in ISO 8601 format.
    pub created_date: String,
    /// Access JWT token.
    pub access_jwt: String,
    /// Refresh JWT token.
    pub refresh_jwt: String,
    /// Client IP address.
    pub ip_address: String,
    /// Client user agent string.
    pub user_agent: String,
}

/// An admin session for the PDS admin interface.
#[derive(Debug, Clone)]
pub struct AdminSession {
    /// Unique session identifier.
    pub session_id: String,
    /// Client IP address.
    pub ip_address: String,
    /// Client user agent string.
    pub user_agent: String,
    /// Session creation date in ISO 8601 format.
    pub created_date: String,
    /// Authentication type.
    pub auth_type: String,
}

/// A WebAuthn passkey credential.
#[derive(Debug, Clone)]
pub struct Passkey {
    /// User-friendly name for the passkey.
    pub name: String,
    /// Creation date in ISO 8601 format.
    pub created_date: String,
    /// WebAuthn credential ID.
    pub credential_id: String,
    /// Public key in JWK format.
    pub public_key: String,
}

/// A WebAuthn passkey challenge.
#[derive(Debug, Clone)]
pub struct PasskeyChallenge {
    /// Challenge creation date in ISO 8601 format.
    pub created_date: String,
    /// Challenge value (base64url encoded).
    pub challenge: String,
}

/// Key for identifying a statistic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatisticKey {
    /// Statistic name.
    pub name: String,
    /// Client IP address.
    pub ip_address: String,
    /// Client user agent string.
    pub user_agent: String,
}

/// A statistic record.
#[derive(Debug, Clone)]
pub struct Statistic {
    /// Statistic name.
    pub name: String,
    /// Client IP address.
    pub ip_address: String,
    /// Client user agent string.
    pub user_agent: String,
    /// Current value.
    pub value: i64,
    /// Last update date in ISO 8601 format.
    pub last_updated_date: String,
}

/// A firehose event (AT Protocol event stream frame).
#[derive(Debug, Clone)]
pub struct FirehoseEvent {
    /// Sequence number in the firehose stream.
    pub sequence_number: i64,
    /// Creation date in ISO 8601 format.
    pub created_date: String,
    /// Operation type (1 = message, -1 = error).
    pub header_op: i32,
    /// Event type (e.g., "#commit"). None if op is -1.
    pub header_t: Option<String>,
    /// Full header object in DAG-CBOR format (serialized bytes).
    pub header_dag_cbor_bytes: Vec<u8>,
    /// Full body object in DAG-CBOR format (serialized bytes).
    pub body_dag_cbor_bytes: Vec<u8>,
}

impl FirehoseEvent {
    /// Get the current datetime formatted for database storage.
    pub fn get_new_created_date() -> String {
        format_datetime_for_db(Utc::now())
    }

    /// Get a datetime string for N hours ago.
    pub fn get_created_date_minus_hours(hours: i64) -> String {
        let dt = Utc::now() - chrono::Duration::hours(hours);
        format_datetime_for_db(dt)
    }
}

/// Repository header stored in the database.
#[derive(Debug, Clone)]
pub struct DbRepoHeader {
    /// CID of the repo commit (base32 encoded).
    pub repo_commit_cid: String,
    /// Version number.
    pub version: i32,
}

/// Repository commit stored in the database.
#[derive(Debug, Clone)]
pub struct DbRepoCommit {
    /// Version number.
    pub version: i32,
    /// CID of this commit (base32 encoded).
    pub cid: String,
    /// CID of the root MST node (base32 encoded).
    pub root_mst_node_cid: String,
    /// Revision string (TID).
    pub rev: String,
    /// CID of the previous MST node (base32 encoded), if any.
    pub prev_mst_node_cid: Option<String>,
    /// Signature bytes.
    pub signature: Vec<u8>,
}

/// Repository record stored in the database.
#[derive(Debug, Clone)]
pub struct DbRepoRecord {
    /// Collection name (NSID).
    pub collection: String,
    /// Record key.
    pub rkey: String,
    /// CID of the record (base32 encoded).
    pub cid: String,
    /// DAG-CBOR serialized record data.
    pub dag_cbor_bytes: Vec<u8>,
}

/// Format a DateTime for database storage.
///
/// Uses ISO 8601 format with milliseconds: `yyyy-MM-ddTHH:mm:ss.fffZ`
pub fn format_datetime_for_db(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Get the current datetime formatted for database storage.
pub fn get_current_datetime_for_db() -> String {
    format_datetime_for_db(Utc::now())
}
