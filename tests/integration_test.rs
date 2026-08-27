//! Integration tests for rustproto repository reading and writing.
//!
//! These tests verify that:
//! 1. We can read a CAR file into memory
//! 2. We can write a CAR file from memory
//! 3. A round-trip (read -> write -> read) produces identical results

use rustproto::repo::{Repo, RepoHeader, RepoRecord, VarInt, CidV1, DagCborObject, DagCborType, DagCborMajorType, DagCborValue};
use std::collections::HashMap;

/// Helper to create a test CID
fn create_test_cid(seed: u8) -> CidV1 {
    CidV1 {
        version: VarInt::from_long(1),
        multicodec: VarInt::from_long(0x71), // dag-cbor
        hash_function: VarInt::from_long(0x12), // sha2-256
        digest_size: VarInt::from_long(32),
        digest_bytes: vec![seed; 32],
        all_bytes: Vec::new(),
        base32: String::new(),
    }
}

/// Helper to create a test data block (AT Protocol post)
fn create_test_post(text: &str) -> DagCborObject {
    let mut map = HashMap::new();
    map.insert(
        "$type".to_string(),
        DagCborObject {
            cbor_type: DagCborType {
                major_type: DagCborMajorType::Text,
                additional_info: 0,
                original_byte: 0,
            },
            value: DagCborValue::Text("app.bsky.feed.post".to_string()),
        },
    );
    map.insert(
        "text".to_string(),
        DagCborObject {
            cbor_type: DagCborType {
                major_type: DagCborMajorType::Text,
                additional_info: 0,
                original_byte: 0,
            },
            value: DagCborValue::Text(text.to_string()),
        },
    );
    map.insert(
        "createdAt".to_string(),
        DagCborObject {
            cbor_type: DagCborType {
                major_type: DagCborMajorType::Text,
                additional_info: 0,
                original_byte: 0,
            },
            value: DagCborValue::Text("2024-01-01T00:00:00.000Z".to_string()),
        },
    );

    DagCborObject {
        cbor_type: DagCborType {
            major_type: DagCborMajorType::Map,
            additional_info: 3,
            original_byte: 0,
        },
        value: DagCborValue::Map(map),
    }
}

#[test]
fn test_repo_read_write_roundtrip_in_memory() {
    // Create a synthetic repo in memory
    let header = RepoHeader {
        repo_commit_cid: create_test_cid(0xAA),
        version: 1,
    };

    let records = vec![
        RepoRecord {
            cid: create_test_cid(0xBB),
            data_block: create_test_post("First post! 🎉"),
            json_string: String::new(),
            at_proto_type: Some("app.bsky.feed.post".to_string()),
            created_at: Some("2024-01-01T00:00:00.000Z".to_string()),
            is_error: false,
            error_message: None,
        },
        RepoRecord {
            cid: create_test_cid(0xCC),
            data_block: create_test_post("Second post with emoji 🚀"),
            json_string: String::new(),
            at_proto_type: Some("app.bsky.feed.post".to_string()),
            created_at: Some("2024-01-01T00:00:00.000Z".to_string()),
            is_error: false,
            error_message: None,
        },
        RepoRecord {
            cid: create_test_cid(0xDD),
            data_block: create_test_post("Third post: こんにちは世界!"),
            json_string: String::new(),
            at_proto_type: Some("app.bsky.feed.post".to_string()),
            created_at: Some("2024-01-01T00:00:00.000Z".to_string()),
            is_error: false,
            error_message: None,
        },
    ];

    // Write to bytes
    let mut first_write = Vec::new();
    Repo::write_repo(&mut first_write, &header, &records).unwrap();

    // Read back
    let (read_header, read_records) = Repo::read_repo(std::io::Cursor::new(&first_write)).unwrap();

    // Verify header
    assert_eq!(read_header.version, header.version);
    assert_eq!(read_header.repo_commit_cid.digest_bytes, header.repo_commit_cid.digest_bytes);

    // Verify record count
    assert_eq!(read_records.len(), records.len());

    // Write again
    let mut second_write = Vec::new();
    Repo::write_repo(&mut second_write, &read_header, &read_records).unwrap();

    // Verify bytes are identical
    assert_eq!(first_write.len(), second_write.len(), 
        "Byte lengths differ: first={}, second={}", first_write.len(), second_write.len());
    assert_eq!(first_write, second_write, "Bytes are not identical after round-trip");
}


#[test]
fn test_empty_repo_roundtrip() {
    // Test with a repo that has only a header and no records
    let header = RepoHeader {
        repo_commit_cid: create_test_cid(0xFF),
        version: 1,
    };
    let records: Vec<RepoRecord> = vec![];

    let mut bytes = Vec::new();
    Repo::write_repo(&mut bytes, &header, &records).unwrap();

    let (read_header, read_records) = Repo::read_repo(std::io::Cursor::new(&bytes)).unwrap();

    assert_eq!(read_header.version, 1);
    assert_eq!(read_records.len(), 0);
}
