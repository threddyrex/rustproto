//! Integration tests for PDS database operations.
//!
//! These tests verify all PdsDb CRUD operations for:
//! - ConfigProperty
//! - Blob
//! - Preferences
//! - RepoHeader
//! - RepoCommit
//! - RepoRecord
//! - SequenceNumber
//! - FirehoseEvent
//! - LogLevel
//! - OauthRequest
//! - OauthSession
//! - LegacySession
//! - AdminSession
//! - Passkey
//! - PasskeyChallenge
//! - Statistic

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use rstproto::fs::LocalFileSystem;
use rstproto::log::Logger;
use rstproto::pds::{
    AdminSession, Blob, DbRepoCommit, DbRepoHeader, FirehoseEvent, Installer,
    LegacySession, OauthRequest, OauthSession, Passkey, PasskeyChallenge, PdsDb, PdsDbError,
    StatisticKey,
};
use rstproto::pds::db::{format_datetime_for_db, get_current_datetime_for_db};
use chrono::{Duration, Utc};
use uuid::Uuid;

// Counter for unique test directories
static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Get a unique test data directory for each test.
fn get_unique_test_dir() -> PathBuf {
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let temp_dir = std::env::temp_dir().join(format!("rstproto-pds-db-tests-{}", counter));
    if !temp_dir.exists() {
        fs::create_dir_all(&temp_dir).unwrap();
    }
    temp_dir
}

/// Create a test PdsDb instance with a fresh database.
fn setup_test_db() -> (PdsDb, LocalFileSystem, Logger) {
    let test_dir = get_unique_test_dir();
    let logger = Logger::default_logger();
    
    // Initialize LFS
    let lfs = LocalFileSystem::initialize_with_create(&test_dir).unwrap();
    
    // Delete existing DB if present
    let db_path = lfs.get_path_pds_db();
    if db_path.exists() {
        fs::remove_file(&db_path).ok();
    }
    
    // Install the database
    Installer::install_db(&lfs, &logger, false).unwrap();
    
    // Connect to the database
    let pds_db = PdsDb::connect(&lfs).unwrap();
    
    // Set up basic config
    pds_db.set_config_property("UserDid", "did:example:testuser").unwrap();
    pds_db.set_config_property("UserHandle", "testuser").unwrap();
    pds_db.set_config_property("UserEmail", "testuser@example.com").unwrap();
    
    (pds_db, lfs, logger)
}

// =========================================================================
// CONFIG PROPERTY TESTS
// =========================================================================

#[test]
fn config_property_get_and_set() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_config_properties().unwrap();
    
    pds_db.set_config_property("testKey", "testValue").unwrap();
    let value = pds_db.get_config_property("testKey").unwrap();
    
    assert_eq!(value, "testValue");
}

#[test]
fn config_property_doesnt_exist_throws() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_config_properties().unwrap();
    
    pds_db.set_config_property("testKey", "testValue").unwrap();
    
    let result = pds_db.get_config_property("nonExistentKey");
    assert!(matches!(result, Err(PdsDbError::ConfigPropertyNotFound(_))));
}

#[test]
fn config_property_set_twice_takes_second() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_config_properties().unwrap();
    
    pds_db.set_config_property("testKey", "testValue").unwrap();
    pds_db.set_config_property("testKey", "newValue").unwrap();
    let value = pds_db.get_config_property("testKey").unwrap();
    
    assert_eq!(value, "newValue");
}

#[test]
fn config_property_get_and_set_bool() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_config_properties().unwrap();
    
    pds_db.set_config_property_bool("testKey", true).unwrap();
    assert!(pds_db.get_config_property_bool("testKey").unwrap());
    
    pds_db.set_config_property_bool("testKey", false).unwrap();
    assert!(!pds_db.get_config_property_bool("testKey").unwrap());
    
    pds_db.set_config_property_bool("testKey", true).unwrap();
    assert!(pds_db.get_config_property_bool("testKey").unwrap());
}

#[test]
fn config_property_get_and_set_int() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_config_properties().unwrap();
    
    pds_db.set_config_property_int("testKey", 1).unwrap();
    assert_eq!(pds_db.get_config_property_int("testKey").unwrap(), 1);
    
    pds_db.set_config_property_int("testKey", 67).unwrap();
    assert_eq!(pds_db.get_config_property_int("testKey").unwrap(), 67);
    
    pds_db.set_config_property_int("testKey1", 68).unwrap();
    assert_eq!(pds_db.get_config_property_int("testKey").unwrap(), 67);
    assert_eq!(pds_db.get_config_property_int("testKey1").unwrap(), 68);
}

#[test]
fn config_property_exists() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_config_properties().unwrap();
    
    assert!(!pds_db.config_property_exists("testKey").unwrap());
    pds_db.set_config_property("testKey", "value").unwrap();
    assert!(pds_db.config_property_exists("testKey").unwrap());
}

// =========================================================================
// BLOB TESTS
// =========================================================================

#[test]
fn blob_insert_and_retrieve() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_blobs().unwrap();
    
    let blob = Blob {
        cid: "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm".to_string(),
        content_type: "image/png".to_string(),
        content_length: 12345,
    };
    
    pds_db.insert_blob(&blob).unwrap();
    
    let retrieved = pds_db.get_blob_by_cid(&blob.cid).unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.cid, blob.cid);
    assert_eq!(retrieved.content_type, blob.content_type);
    assert_eq!(retrieved.content_length, blob.content_length);
}

#[test]
fn blob_exists() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_blobs().unwrap();
    
    let blob = Blob {
        cid: "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm".to_string(),
        content_type: "image/png".to_string(),
        content_length: 12345,
    };
    
    assert!(!pds_db.blob_exists(&blob.cid).unwrap());
    pds_db.insert_blob(&blob).unwrap();
    assert!(pds_db.blob_exists(&blob.cid).unwrap());
}

#[test]
fn blob_list_with_cursor() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_blobs().unwrap();
    
    for i in 0..5 {
        let blob = Blob {
            cid: format!("cid{}", i),
            content_type: "image/png".to_string(),
            content_length: 100 * i,
        };
        pds_db.insert_blob(&blob).unwrap();
    }
    
    let blobs = pds_db.list_blobs_with_cursor(None, 3).unwrap();
    assert_eq!(blobs.len(), 3);
    
    let blobs2 = pds_db.list_blobs_with_cursor(Some(&blobs[2]), 3).unwrap();
    assert_eq!(blobs2.len(), 2);
}

// =========================================================================
// PREFERENCES TESTS
// =========================================================================

#[test]
fn preferences_insert_and_retrieve() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_preferences().unwrap();
    
    let prefs = r#"{"theme": "dark"}"#;
    pds_db.insert_preferences(prefs).unwrap();
    
    let retrieved = pds_db.get_preferences().unwrap();
    assert_eq!(retrieved, prefs);
}

#[test]
fn preferences_update() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_preferences().unwrap();
    
    pds_db.insert_preferences(r#"{"theme": "light"}"#).unwrap();
    pds_db.update_preferences(r#"{"theme": "dark"}"#).unwrap();
    
    let retrieved = pds_db.get_preferences().unwrap();
    assert_eq!(retrieved, r#"{"theme": "dark"}"#);
}

// =========================================================================
// REPO HEADER TESTS
// =========================================================================

#[test]
fn repo_header_insert_and_retrieve() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_repo_header().ok();
    
    let header = DbRepoHeader {
        repo_commit_cid: "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm".to_string(),
        version: 3,
    };
    
    pds_db.insert_update_repo_header(&header).unwrap();
    
    let retrieved = pds_db.get_repo_header().unwrap();
    assert_eq!(retrieved.repo_commit_cid, header.repo_commit_cid);
    assert_eq!(retrieved.version, header.version);
}

#[test]
fn repo_header_delete() {
    let (pds_db, _, _) = setup_test_db();
    
    let header = DbRepoHeader {
        repo_commit_cid: "bafyreiahyzvpofpsudabba2mhjw62k5h6jtotsn7mt7ja7ams5sjqdpbai".to_string(),
        version: 3,
    };
    
    pds_db.insert_update_repo_header(&header).unwrap();
    assert!(pds_db.repo_header_exists().unwrap());
    
    pds_db.delete_repo_header().unwrap();
    
    let result = pds_db.get_repo_header();
    assert!(matches!(result, Err(PdsDbError::RepoHeaderNotFound)));
}

// =========================================================================
// REPO COMMIT TESTS
// =========================================================================

#[test]
fn repo_commit_insert_and_retrieve() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_repo_commit().ok();
    
    let commit = DbRepoCommit {
        version: 3,
        cid: "bafyreiahyzvpofpsudabba2mhjw62k5h6jtotsn7mt7ja7ams5sjqdpbai".to_string(),
        root_mst_node_cid: "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm".to_string(),
        rev: Utc::now().timestamp().to_string(),
        prev_mst_node_cid: None,
        signature: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    };
    
    pds_db.insert_update_repo_commit(&commit).unwrap();
    
    let retrieved = pds_db.get_repo_commit().unwrap();
    assert_eq!(retrieved.cid, commit.cid);
    assert_eq!(retrieved.root_mst_node_cid, commit.root_mst_node_cid);
    assert_eq!(retrieved.rev, commit.rev);
    assert_eq!(retrieved.signature, commit.signature);
    assert_eq!(retrieved.version, commit.version);
}

#[test]
fn repo_commit_delete() {
    let (pds_db, _, _) = setup_test_db();
    
    let commit = DbRepoCommit {
        version: 3,
        cid: "bafyreiahyzvpofpsudabba2mhjw62k5h6jtotsn7mt7ja7ams5sjqdpbai".to_string(),
        root_mst_node_cid: "bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm".to_string(),
        rev: Utc::now().timestamp().to_string(),
        prev_mst_node_cid: None,
        signature: vec![1, 2, 3, 4],
    };
    
    pds_db.insert_update_repo_commit(&commit).unwrap();
    pds_db.delete_repo_commit().unwrap();
    
    let result = pds_db.get_repo_commit();
    assert!(matches!(result, Err(PdsDbError::RepoCommitNotFound)));
}

// =========================================================================
// REPO RECORD TESTS
// =========================================================================

#[test]
fn repo_record_insert_and_retrieve() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_repo_records().unwrap();
    
    let dag_cbor_bytes = vec![0xA1, 0x67, 0x65, 0x78, 0x61, 0x6D, 0x70, 0x6C, 0x65, 0x64, 0x64, 0x61, 0x74, 0x61];
    
    pds_db.insert_repo_record(
        "collection1",
        "rkey1",
        "bafyreifjef7rncdlfq347oislx3qiss2gt5jydzquzpjpwye6tsdf4joom",
        &dag_cbor_bytes,
    ).unwrap();
    
    let retrieved = pds_db.get_repo_record("collection1", "rkey1").unwrap();
    assert_eq!(retrieved.collection, "collection1");
    assert_eq!(retrieved.rkey, "rkey1");
    assert_eq!(retrieved.dag_cbor_bytes, dag_cbor_bytes);
    
    pds_db.delete_repo_record("collection1", "rkey1").unwrap();
}

#[test]
fn repo_record_exists() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_repo_records().unwrap();
    
    let dag_cbor_bytes = vec![0xA1];
    
    pds_db.insert_repo_record(
        "collection1",
        "rkey1",
        "bafyreifjef7rncdlfq347oislx3qiss2gt5jydzquzpjpwye6tsdf4joom",
        &dag_cbor_bytes,
    ).unwrap();
    
    assert!(pds_db.record_exists("collection1", "rkey1").unwrap());
    assert!(!pds_db.record_exists("collection1", "rkey2").unwrap());
}

#[test]
fn repo_record_insert_and_delete() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_repo_records().unwrap();
    
    let dag_cbor_bytes = vec![0xA1];
    
    pds_db.insert_repo_record(
        "collection1",
        "rkey1",
        "bafyreifjef7rncdlfq347oislx3qiss2gt5jydzquzpjpwye6tsdf4joom",
        &dag_cbor_bytes,
    ).unwrap();
    
    assert!(pds_db.record_exists("collection1", "rkey1").unwrap());
    
    pds_db.delete_repo_record("collection1", "rkey1").unwrap();
    
    let result = pds_db.get_repo_record("collection1", "rkey1");
    assert!(matches!(result, Err(PdsDbError::RepoRecordNotFound(_, _))));
    assert!(!pds_db.record_exists("collection1", "rkey1").unwrap());
}

#[test]
fn repo_record_delete_all() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_repo_records().unwrap();
    
    let dag_cbor_bytes = vec![0xA1];
    
    pds_db.insert_repo_record("collection1", "rkey1", "cid1", &dag_cbor_bytes).unwrap();
    pds_db.insert_repo_record("collection2", "rkey2", "cid2", &dag_cbor_bytes).unwrap();
    
    pds_db.delete_all_repo_records().unwrap();
    
    assert!(!pds_db.record_exists("collection1", "rkey1").unwrap());
    assert!(!pds_db.record_exists("collection2", "rkey2").unwrap());
}

// =========================================================================
// SEQUENCE NUMBER TESTS
// =========================================================================

#[test]
fn sequence_number() {
    let (pds_db, _, _) = setup_test_db();
    
    pds_db.delete_sequence_number().unwrap();
    assert_eq!(pds_db.get_most_recently_used_sequence_number().unwrap(), 0);
    assert_eq!(pds_db.get_new_sequence_number_for_firehose().unwrap(), 1);
    assert_eq!(pds_db.get_new_sequence_number_for_firehose().unwrap(), 2);
    assert_eq!(pds_db.get_new_sequence_number_for_firehose().unwrap(), 3);
}

// =========================================================================
// FIREHOSE EVENT TESTS
// =========================================================================

#[test]
fn firehose_event_insert() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_firehose_events().unwrap();
    
    let event = FirehoseEvent {
        sequence_number: 1,
        created_date: get_current_datetime_for_db(),
        header_op: 1,
        header_t: Some("test header_t".to_string()),
        header_dag_cbor_bytes: vec![0xA1, 0x67, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72],
        body_dag_cbor_bytes: vec![0xA1, 0x64, 0x62, 0x6F, 0x64, 0x79],
    };
    
    pds_db.insert_firehose_event(&event).unwrap();
    
    let retrieved = pds_db.get_firehose_event(event.sequence_number).unwrap();
    assert_eq!(retrieved.sequence_number, event.sequence_number);
    assert_eq!(retrieved.created_date, event.created_date);
    assert_eq!(retrieved.header_op, event.header_op);
    assert_eq!(retrieved.header_t, event.header_t);
    assert_eq!(retrieved.header_dag_cbor_bytes, event.header_dag_cbor_bytes);
    assert_eq!(retrieved.body_dag_cbor_bytes, event.body_dag_cbor_bytes);
}

#[test]
fn firehose_event_insert2() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_firehose_events().unwrap();
    
    let event1 = FirehoseEvent {
        sequence_number: 1,
        created_date: get_current_datetime_for_db(),
        header_op: 1,
        header_t: Some("test header_t".to_string()),
        header_dag_cbor_bytes: vec![0xA1],
        body_dag_cbor_bytes: vec![0xA1],
    };
    
    let event2 = FirehoseEvent {
        sequence_number: 2,
        created_date: get_current_datetime_for_db(),
        header_op: 1,
        header_t: Some("test header_t 2".to_string()),
        header_dag_cbor_bytes: vec![0xA2],
        body_dag_cbor_bytes: vec![0xA2],
    };
    
    pds_db.insert_firehose_event(&event1).unwrap();
    pds_db.insert_firehose_event(&event2).unwrap();
    
    let retrieved = pds_db.get_firehose_event(event2.sequence_number).unwrap();
    assert_eq!(retrieved.sequence_number, event2.sequence_number);
}

#[test]
fn firehose_event_doesnt_exist() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_firehose_events().unwrap();
    
    let event = FirehoseEvent {
        sequence_number: 1,
        created_date: get_current_datetime_for_db(),
        header_op: 1,
        header_t: None,
        header_dag_cbor_bytes: vec![0xA1],
        body_dag_cbor_bytes: vec![0xA1],
    };
    
    pds_db.insert_firehose_event(&event).unwrap();
    
    let result = pds_db.get_firehose_event(3);
    assert!(matches!(result, Err(PdsDbError::FirehoseEventNotFound(_))));
}

// =========================================================================
// LOG LEVEL TESTS
// =========================================================================

#[test]
fn log_level_set_level() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_log_level().unwrap();
    
    assert_eq!(pds_db.get_log_level_count().unwrap(), 0);
    assert_eq!(pds_db.get_log_level().unwrap(), "info");
    
    pds_db.set_log_level("trace").unwrap();
    assert_eq!(pds_db.get_log_level_count().unwrap(), 1);
    assert_eq!(pds_db.get_log_level().unwrap(), "trace");
    
    pds_db.set_log_level("trace").unwrap();
    assert_eq!(pds_db.get_log_level_count().unwrap(), 1);
    assert_eq!(pds_db.get_log_level().unwrap(), "trace");
    
    pds_db.set_log_level("debug").unwrap();
    assert_eq!(pds_db.get_log_level_count().unwrap(), 1);
    assert_eq!(pds_db.get_log_level().unwrap(), "debug");
    
    pds_db.set_log_level("error").unwrap();
    assert_eq!(pds_db.get_log_level_count().unwrap(), 1);
    assert_eq!(pds_db.get_log_level().unwrap(), "error");
}

// =========================================================================
// OAUTH REQUEST TESTS
// =========================================================================

#[test]
fn oauth_request_insert_and_get() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_oauth_requests().unwrap();
    
    let request_uri = Uuid::new_v4().to_string();
    let expires_date = format_datetime_for_db(Utc::now() + Duration::minutes(5));
    let request = OauthRequest {
        request_uri: request_uri.clone(),
        expires_date: expires_date.clone(),
        dpop: "dpop".to_string(),
        body: "body".to_string(),
        authorization_code: None,
        auth_type: None,
    };
    
    pds_db.insert_oauth_request(&request).unwrap();
    
    let retrieved = pds_db.get_oauth_request(&request_uri).unwrap();
    assert_eq!(retrieved.request_uri, request.request_uri);
    assert_eq!(retrieved.expires_date, request.expires_date);
    assert_eq!(retrieved.dpop, request.dpop);
    assert_eq!(retrieved.body, request.body);
}

#[test]
fn oauth_request_insert_get_expired() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_oauth_requests().unwrap();
    
    let request_uri = Uuid::new_v4().to_string();
    let expires_date = format_datetime_for_db(Utc::now() - Duration::minutes(1));
    let request = OauthRequest {
        request_uri: request_uri.clone(),
        expires_date,
        dpop: "dpop".to_string(),
        body: "body".to_string(),
        authorization_code: None,
        auth_type: None,
    };
    
    pds_db.insert_oauth_request(&request).unwrap();
    
    let result = pds_db.get_oauth_request(&request_uri);
    assert!(matches!(result, Err(PdsDbError::OauthRequestNotFound(_))));
}

#[test]
fn oauth_request_insert_and_get_by_authorization_code() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_oauth_requests().unwrap();
    
    let request_uri = Uuid::new_v4().to_string();
    let auth_code = Uuid::new_v4().to_string();
    let expires_date = format_datetime_for_db(Utc::now() + Duration::minutes(5));
    let request = OauthRequest {
        request_uri: request_uri.clone(),
        expires_date: expires_date.clone(),
        dpop: "dpop".to_string(),
        body: "body".to_string(),
        authorization_code: Some(auth_code.clone()),
        auth_type: Some("Legacy".to_string()),
    };
    
    pds_db.insert_oauth_request(&request).unwrap();
    
    let retrieved = pds_db.get_oauth_request_by_authorization_code(&auth_code).unwrap();
    assert_eq!(retrieved.request_uri, request.request_uri);
    assert_eq!(retrieved.authorization_code, request.authorization_code);
    assert_eq!(retrieved.auth_type, request.auth_type);
}

#[test]
fn oauth_request_update() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_oauth_requests().unwrap();
    
    let request_uri = Uuid::new_v4().to_string();
    let expires_date = format_datetime_for_db(Utc::now() + Duration::minutes(5));
    let mut request = OauthRequest {
        request_uri: request_uri.clone(),
        expires_date: expires_date.clone(),
        dpop: "dpop".to_string(),
        body: "body".to_string(),
        authorization_code: None,
        auth_type: None,
    };
    
    pds_db.insert_oauth_request(&request).unwrap();
    request.authorization_code = Some("authcode".to_string());
    pds_db.update_oauth_request(&request).unwrap();
    
    let retrieved = pds_db.get_oauth_request(&request_uri).unwrap();
    assert_eq!(retrieved.authorization_code, Some("authcode".to_string()));
}

// =========================================================================
// OAUTH SESSION TESTS
// =========================================================================

#[test]
fn oauth_session_insert_get() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_oauth_sessions().unwrap();
    
    let session_id = Uuid::new_v4().to_string();
    let session = OauthSession {
        session_id: session_id.clone(),
        client_id: "clientId".to_string(),
        scope: "scope".to_string(),
        dpop_jwk_thumbprint: "dpopJwkThumbprint".to_string(),
        refresh_token: "refreshToken".to_string(),
        refresh_token_expires_date: format_datetime_for_db(Utc::now() + Duration::minutes(5)),
        created_date: get_current_datetime_for_db(),
        ip_address: "ipaddr".to_string(),
        auth_type: "Passkey".to_string(),
    };
    
    pds_db.insert_oauth_session(&session).unwrap();
    
    let retrieved = pds_db.get_oauth_session_by_session_id(&session_id).unwrap();
    assert_eq!(retrieved.session_id, session.session_id);
    assert_eq!(retrieved.client_id, session.client_id);
    assert_eq!(retrieved.scope, session.scope);
    assert_eq!(retrieved.dpop_jwk_thumbprint, session.dpop_jwk_thumbprint);
    assert_eq!(retrieved.refresh_token, session.refresh_token);
    assert_eq!(retrieved.auth_type, session.auth_type);
}

#[test]
fn oauth_session_delete_old() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_oauth_sessions().unwrap();
    
    let session_id = Uuid::new_v4().to_string();
    let session = OauthSession {
        session_id: session_id.clone(),
        client_id: "clientId".to_string(),
        scope: "scope".to_string(),
        dpop_jwk_thumbprint: "dpopJwkThumbprint".to_string(),
        refresh_token: "refreshToken".to_string(),
        refresh_token_expires_date: format_datetime_for_db(Utc::now() - Duration::minutes(5)),
        created_date: get_current_datetime_for_db(),
        ip_address: "ipaddr".to_string(),
        auth_type: "Legacy".to_string(),
    };
    
    pds_db.insert_oauth_session(&session).unwrap();
    pds_db.delete_old_oauth_sessions().unwrap();
    
    let result = pds_db.get_oauth_session_by_session_id(&session_id);
    assert!(matches!(result, Err(PdsDbError::OauthSessionNotFound(_))));
}

// =========================================================================
// LEGACY SESSION TESTS
// =========================================================================

#[test]
fn legacy_session_create() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_legacy_sessions().unwrap();
    
    let session = LegacySession {
        access_jwt: "accessjwt".to_string(),
        refresh_jwt: "refreshjwt".to_string(),
        created_date: get_current_datetime_for_db(),
        ip_address: "ipaddr".to_string(),
        user_agent: "useragent".to_string(),
    };
    
    pds_db.create_legacy_session(&session).unwrap();
    
    assert!(pds_db.legacy_session_exists_for_access_jwt(&session.access_jwt).unwrap());
    assert!(pds_db.legacy_session_exists_for_refresh_jwt(&session.refresh_jwt).unwrap());
    assert!(!pds_db.legacy_session_exists_for_access_jwt("nonexistent").unwrap());
    assert!(!pds_db.legacy_session_exists_for_refresh_jwt("nonexistent").unwrap());
}

#[test]
fn legacy_session_delete_for_refresh_jwt() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_legacy_sessions().unwrap();
    
    let session = LegacySession {
        access_jwt: "accessjwt".to_string(),
        refresh_jwt: "refreshjwt".to_string(),
        created_date: get_current_datetime_for_db(),
        ip_address: "ipaddr".to_string(),
        user_agent: "useragent".to_string(),
    };
    
    pds_db.create_legacy_session(&session).unwrap();
    
    assert!(pds_db.legacy_session_exists_for_access_jwt(&session.access_jwt).unwrap());
    assert!(pds_db.legacy_session_exists_for_refresh_jwt(&session.refresh_jwt).unwrap());
    
    pds_db.delete_legacy_session_for_refresh_jwt(&session.refresh_jwt).unwrap();
    
    assert!(!pds_db.legacy_session_exists_for_access_jwt(&session.access_jwt).unwrap());
    assert!(!pds_db.legacy_session_exists_for_refresh_jwt(&session.refresh_jwt).unwrap());
}

// =========================================================================
// ADMIN SESSION TESTS
// =========================================================================

#[test]
fn admin_session_create_and_delete() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_admin_sessions().unwrap();
    
    let session_id = Uuid::new_v4().to_string();
    let session = AdminSession {
        session_id: session_id.clone(),
        ip_address: "ipaddr".to_string(),
        created_date: get_current_datetime_for_db(),
        user_agent: "useragent".to_string(),
        auth_type: "authType".to_string(),
    };
    
    pds_db.insert_admin_session(&session).unwrap();
    
    let retrieved = pds_db.get_valid_admin_session(&session_id, "ipaddr", 60).unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    
    assert_eq!(retrieved.session_id, session.session_id);
    assert_eq!(retrieved.ip_address, session.ip_address);
    assert_eq!(retrieved.user_agent, session.user_agent);
    assert_eq!(retrieved.auth_type, session.auth_type);
    
    pds_db.delete_all_admin_sessions().unwrap();
    
    assert!(pds_db.get_valid_admin_session(&session_id, "ipaddr", 60).unwrap().is_none());
}

#[test]
fn admin_session_create_and_get_invalid() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_admin_sessions().unwrap();
    
    let session_id = Uuid::new_v4().to_string();
    let session = AdminSession {
        session_id: session_id.clone(),
        ip_address: "ipaddr".to_string(),
        created_date: format_datetime_for_db(Utc::now() - Duration::hours(2)),
        user_agent: "useragent".to_string(),
        auth_type: "authType".to_string(),
    };
    
    pds_db.insert_admin_session(&session).unwrap();
    
    let retrieved = pds_db.get_valid_admin_session(&session_id, "ipaddr", 60).unwrap();
    assert!(retrieved.is_none());
}

// =========================================================================
// PASSKEY TESTS
// =========================================================================

#[test]
fn passkey_insert_and_retrieve() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_passkeys().unwrap();
    
    let passkey = Passkey {
        name: Uuid::new_v4().to_string(),
        credential_id: "zCredentialIdExample".to_string(),
        public_key: "zPublicKeyExample".to_string(),
        created_date: get_current_datetime_for_db(),
    };
    
    pds_db.insert_passkey(&passkey).unwrap();
    
    let retrieved = pds_db.get_passkey_by_credential_id(&passkey.credential_id).unwrap();
    assert_eq!(retrieved.name, passkey.name);
    assert_eq!(retrieved.credential_id, passkey.credential_id);
    assert_eq!(retrieved.public_key, passkey.public_key);
    assert_eq!(retrieved.created_date, passkey.created_date);
}

// =========================================================================
// PASSKEY CHALLENGE TESTS
// =========================================================================

#[test]
fn passkey_challenge_insert_and_retrieve() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_passkey_challenges().unwrap();
    
    let challenge = PasskeyChallenge {
        challenge: "challenge_example".to_string(),
        created_date: get_current_datetime_for_db(),
    };
    
    pds_db.insert_passkey_challenge(&challenge).unwrap();
    
    let retrieved = pds_db.get_passkey_challenge(&challenge.challenge).unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.challenge, challenge.challenge);
    assert_eq!(retrieved.created_date, challenge.created_date);
}

// =========================================================================
// STATISTIC TESTS
// =========================================================================

#[test]
fn stats_insert_and_retrieve() {
    let (pds_db, _, _) = setup_test_db();
    pds_db.delete_all_statistics().unwrap();
    
    let key = StatisticKey {
        name: "active_users".to_string(),
        ip_address: "userip".to_string(),
        user_agent: "useragent".to_string(),
    };
    
    pds_db.increment_statistic(&key).unwrap();
    pds_db.increment_statistic(&key).unwrap();
    
    assert_eq!(pds_db.get_statistic_value(&key).unwrap(), 2);
    assert!(pds_db.statistic_exists(&key).unwrap());
    
    pds_db.increment_statistic(&key).unwrap();
    assert_eq!(pds_db.get_statistic_value(&key).unwrap(), 3);
    
    let stats = pds_db.get_all_statistics().unwrap();
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].name, "active_users");
    assert_eq!(stats[0].ip_address, "userip");
    assert_eq!(stats[0].user_agent, "useragent");
    assert_eq!(stats[0].value, 3);
}
