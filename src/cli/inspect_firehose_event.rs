
use std::collections::HashMap;
use std::io::Cursor;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;
use crate::repo::{DagCborObject, DagCborValue, Repo};
use crate::pds::PdsDb;
use super::hex_encode;


/// Inspect a stored firehose event from the PDS database for debugging.
pub fn cmd_inspect_firehose_event(args: &HashMap<String, String>) {
    let log = logger();
    log.info("InspectFirehoseEvent command started");

    let data_dir = match get_arg(args, "dataDir") {
        Some(d) => d.to_string(),
        None => {
            log.error("Missing required argument: /dataDir");
            return;
        }
    };

    let seq_str = match get_arg(args, "seq") {
        Some(s) => s.to_string(),
        None => {
            log.error("Missing required argument: /seq (sequence number)");
            return;
        }
    };

    let sequence_number: i64 = match seq_str.parse() {
        Ok(n) => n,
        Err(_) => {
            log.error(&format!("Invalid sequence number: {}", seq_str));
            return;
        }
    };

    // Open PDS database
    let lfs = match LocalFileSystem::initialize(&data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };
    let db = match PdsDb::connect(&lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to open database: {}", e));
            return;
        }
    };

    // Get the firehose event
    let event = match db.get_firehose_event(sequence_number) {
        Ok(e) => e,
        Err(e) => {
            log.error(&format!("Failed to get firehose event {}: {}", sequence_number, e));
            return;
        }
    };

    log.info(&format!("=== FIREHOSE EVENT {} ===", sequence_number));
    log.info(&format!("Created: {}", event.created_date));
    log.info(&format!("Header op: {}", event.header_op));
    log.info(&format!("Header t: {:?}", event.header_t));
    log.info(&format!("Header bytes length: {}", event.header_dag_cbor_bytes.len()));
    log.info(&format!("Body bytes length: {}", event.body_dag_cbor_bytes.len()));

    // Parse and display header DAG-CBOR
    log.info("");
    log.info("=== HEADER DAG-CBOR ===");
    let mut header_cursor = Cursor::new(&event.header_dag_cbor_bytes);
    match DagCborObject::read_from_stream(&mut header_cursor) {
        Ok(header_obj) => {
            log.info(&format!("Header JSON:\n{}", header_obj.to_json_string()));
            log.info(&format!("Header debug:\n{}", header_obj.get_recursive_debug_string(0)));
        }
        Err(e) => {
            log.error(&format!("Failed to parse header DAG-CBOR: {}", e));
            log.info(&format!("Header hex: {}", hex_encode(&event.header_dag_cbor_bytes)));
        }
    }

    // Parse and display body DAG-CBOR
    log.info("");
    log.info("=== BODY DAG-CBOR ===");
    let mut body_cursor = Cursor::new(&event.body_dag_cbor_bytes);
    match DagCborObject::read_from_stream(&mut body_cursor) {
        Ok(body_obj) => {
            // Print JSON (may have binary data as base64)
            log.info(&format!("Body JSON:\n{}", body_obj.to_json_string()));
            
            // Print debug structure
            log.info(&format!("Body debug:\n{}", body_obj.get_recursive_debug_string(0)));

            // If this is a #commit or #sync, try to parse the blocks
            if let Some(blocks_obj) = body_obj.select_object(&["blocks"]) {
                if let DagCborValue::ByteString(blocks_bytes) = &blocks_obj.value {
                    log.info("");
                    log.info(&format!("=== BLOCKS ({} bytes) ===", blocks_bytes.len()));
                    
                    let mut blocks_cursor = Cursor::new(blocks_bytes);
                    let walk_result = Repo::walk_repo(
                        &mut blocks_cursor,
                        |repo_header| {
                            log.info("CAR HEADER:");
                            log.info(&format!("   roots: {}", repo_header.repo_commit_cid.get_base32()));
                            log.info(&format!("   version: {}", repo_header.version));
                            true
                        },
                        |repo_record| {
                            log.info(&format!("BLOCK CID: {}", repo_record.cid.get_base32()));
                            log.info(&format!("BLOCK JSON:\n{}", repo_record.json_string));
                            log.info(&format!("BLOCK debug:\n{}", repo_record.data_block.get_recursive_debug_string(0)));
                            true
                        },
                    );

                    if let Err(e) = walk_result {
                        log.error(&format!("Error walking blocks: {}", e));
                        log.info(&format!("Blocks hex (first 500 bytes): {}", hex_encode(&blocks_bytes[..std::cmp::min(500, blocks_bytes.len())])));
                    }
                } else {
                    log.info("blocks field is not a byte string");
                }
            }
        }
        Err(e) => {
            log.error(&format!("Failed to parse body DAG-CBOR: {}", e));
            log.info(&format!("Body hex (first 500 bytes): {}", hex_encode(&event.body_dag_cbor_bytes[..std::cmp::min(500, event.body_dag_cbor_bytes.len())])));
        }
    }
}


