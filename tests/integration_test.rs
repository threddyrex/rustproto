//! Integration tests for rustproto repository reading and writing.
//!
//! These tests verify that:
//! 1. We can read a CAR file into memory
//! 2. We can write a CAR file from memory
//! 3. A round-trip (read -> write -> read) produces identical results

use std::fs::{self, File};
use std::path::PathBuf;

use rustproto::repo::{Repo, RepoHeader, RepoRecord, VarInt, CidV1, DagCborObject, DagCborType, DagCborMajorType, DagCborValue};
use std::collections::HashMap;

/// Helper to get the test data directory
fn test_data_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data");
    path.push("repos");
    path
}

/// Helper to get the scratch directory for test outputs
fn test_scratch_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data");
    path.push("scratch");
    path
}

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
fn test_real_repo_file_roundtrip() {
    let test_file = test_data_dir().join("did_web_threddyrex_org.car");
    
    // Skip test if file doesn't exist
    if !test_file.exists() {
        eprintln!("Skipping test: test file {:?} not found", test_file);
        return;
    }

    // Read original file bytes
    let original_bytes = fs::read(&test_file).unwrap();

    // Read repo into memory
    let input_file = File::open(&test_file).unwrap();
    let (header, records) = Repo::read_repo(input_file).unwrap();

    println!("Read {} records from repo", records.len());
    println!("Header version: {}", header.version);
    println!("Root CID: {}", header.repo_commit_cid.get_base32());

    // Write to output file
    let output_file = test_scratch_dir().join("roundtrip_output.car");
    
    // Ensure scratch directory exists
    fs::create_dir_all(test_scratch_dir()).unwrap();
    
    Repo::write_repo_file(&output_file, &header, &records).unwrap();

    // Read output file bytes
    let output_bytes = fs::read(&output_file).unwrap();

    // Compare byte lengths
    assert_eq!(original_bytes.len(), output_bytes.len(),
        "File sizes differ: original={}, output={}", original_bytes.len(), output_bytes.len());

    // Compare bytes - the files should be identical
    if original_bytes != output_bytes {
        // Find first difference for debugging
        for (i, (a, b)) in original_bytes.iter().zip(output_bytes.iter()).enumerate() {
            if a != b {
                println!("First difference at byte {}: original=0x{:02X}, output=0x{:02X}", i, a, b);
                
                // Show context
                let start = i.saturating_sub(10);
                let end = (i + 10).min(original_bytes.len());
                println!("Original context [{}-{}]: {:02X?}", start, end, &original_bytes[start..end]);
                println!("Output context [{}-{}]: {:02X?}", start, end, &output_bytes[start..end]);
                break;
            }
        }
        panic!("Bytes are not identical after round-trip");
    }

    // Clean up
    let _ = fs::remove_file(&output_file);

    println!("Round-trip test passed! {} bytes", original_bytes.len());
}

#[test]
fn test_repo_copy() {
    let test_file = test_data_dir().join("did_web_threddyrex_org.car");
    
    // Skip test if file doesn't exist
    if !test_file.exists() {
        eprintln!("Skipping test: test file {:?} not found", test_file);
        return;
    }

    // Read original bytes
    let original_bytes = fs::read(&test_file).unwrap();

    // Use copy_repo_file
    let output_file = test_scratch_dir().join("copy_output.car");
    fs::create_dir_all(test_scratch_dir()).unwrap();
    
    Repo::copy_repo_file(&test_file, &output_file).unwrap();

    // Read output bytes
    let output_bytes = fs::read(&output_file).unwrap();

    // Compare
    assert_eq!(original_bytes.len(), output_bytes.len());
    assert_eq!(original_bytes, output_bytes, "Copy produced different bytes");

    // Clean up
    let _ = fs::remove_file(&output_file);

    println!("Copy test passed!");
}

#[test]
fn test_walk_repo_counts_records() {
    let test_file = test_data_dir().join("did_web_threddyrex_org.car");
    
    // Skip test if file doesn't exist
    if !test_file.exists() {
        eprintln!("Skipping test: test file {:?} not found", test_file);
        return;
    }

    let mut record_count = 0;
    let mut atproto_record_count = 0;
    let mut commit_count = 0;
    let mut mst_count = 0;

    Repo::walk_repo_file(&test_file, |header| {
        println!("Header: version={}, root={}", header.version, header.repo_commit_cid.get_base32());
        true
    }, |record| {
        record_count += 1;
        
        if record.is_at_proto_record() {
            atproto_record_count += 1;
        }
        if record.is_repo_commit() {
            commit_count += 1;
        }
        if record.is_mst_node() {
            mst_count += 1;
        }
        
        true
    }).unwrap();

    println!("Total records: {}", record_count);
    println!("AT Protocol records: {}", atproto_record_count);
    println!("Repo commits: {}", commit_count);
    println!("MST nodes: {}", mst_count);

    assert!(record_count > 0, "Expected at least one record");
}

#[test]
fn test_iter_records() {
    let test_file = test_data_dir().join("did_web_threddyrex_org.car");
    
    // Skip test if file doesn't exist
    if !test_file.exists() {
        eprintln!("Skipping test: test file {:?} not found", test_file);
        return;
    }

    let file = File::open(&test_file).unwrap();
    let mut iter = Repo::iter_records(file).unwrap();

    println!("Header: version={}", iter.header().version);

    let mut count = 0;
    for record_result in &mut iter {
        let record = record_result.unwrap();
        count += 1;
        
        if count <= 3 {
            println!("Record {}: type={:?}, cid={}", 
                count, 
                record.at_proto_type,
                record.cid.get_base32()
            );
        }
    }

    println!("Total records via iterator: {}", count);
    assert!(count > 0, "Expected at least one record");
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
