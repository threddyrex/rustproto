

use std::collections::HashMap;
use std::io::Cursor;

use super::{build_commit_dag_cbor_local, hex_encode};
use crate::cli::get_arg;
use crate::log::{logger};
use crate::repo::{DagCborObject, DagCborValue};


pub fn cmd_test_apply_writes_and_log_firehose(args: &HashMap<String, String>) {
    let log = logger();
    log.info("TestApplyWritesAndLogFirehose command started");

    let data_dir = match get_arg(args, "dataDir") {
        Some(d) => d.to_string(),
        None => {
            log.error("Missing required argument: /dataDir");
            return;
        }
    };

    let text = get_arg(args, "text")
        .unwrap_or("Hello from TestApplyWritesAndLogFirehose")
        .to_string();

    // Open PDS database
    let lfs = match crate::fs::LocalFileSystem::initialize(&data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };
    let db = match crate::pds::PdsDb::connect(&lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to open database: {}", e));
            return;
        }
    };

    // Get sequence number before ApplyWrites
    let seq_before = match db.get_most_recently_used_sequence_number() {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to get sequence number: {}", e));
            return;
        }
    };
    log.info(&format!("Sequence number before ApplyWrites: {}", seq_before));

    // Get repo commit before ApplyWrites
    let commit_before = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit before ApplyWrites: {}", e));
            return;
        }
    };

    log.info("");
    log.info("=== REPO COMMIT BEFORE APPLYWRITES ===");
    print_repo_commit_details(log, &db, &commit_before);

    // Build ApplyWrites operation - create a post
    // Hardcoded rkey and createdAt for cross-implementation comparison
    let rkey = "3testapplywriteskey".to_string();
    let collection = "app.bsky.feed.post";

    let created_at = "2026-01-01T00:00:00.000Z".to_string();

    let mut record_map: HashMap<String, DagCborObject> = HashMap::new();
    record_map.insert("text".to_string(), DagCborObject::new_text(text.clone()));
    record_map.insert("createdAt".to_string(), DagCborObject::new_text(created_at));
    let record = DagCborObject::new_map(record_map);

    log.info("");
    log.info("=== APPLYWRITES INPUT ===");
    log.info(&format!("Collection: {}", collection));
    log.info(&format!("Rkey:       {}", rkey));
    log.info(&format!("Text:       {}", text));

    // Serialize the record to DAG-CBOR and print hex + debug
    log.info("");
    log.info("=== RECORD DAG-CBOR (before ApplyWrites) ===");
    match record.to_bytes() {
        Ok(record_bytes) => {
            log.info(&format!("Record DAG-CBOR hex ({} bytes):", record_bytes.len()));
            log.info(&hex_encode(&record_bytes));
            log.info("Record DAG-CBOR debug:");
            log.info(&record.get_recursive_debug_string(0));
        }
        Err(e) => {
            log.error(&format!("Failed to serialize record: {}", e));
        }
    }

    // Call ApplyWrites
    let user_repo = match crate::pds::UserRepo::new(&db) {
        Ok(ur) => ur,
        Err(e) => {
            log.error(&format!("Failed to create UserRepo: {}", e));
            return;
        }
    };

    let operation = crate::pds::ApplyWritesOperation {
        op_type: crate::pds::user_repo::write_type::CREATE.to_string(),
        collection: collection.to_string(),
        rkey: rkey.clone(),
        record: Some(record),
    };

    let results = match user_repo.apply_writes(
        vec![operation],
        "127.0.0.1",
        "TestApplyWritesAndLogFirehose",
    ) {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("ApplyWrites failed: {}", e));
            return;
        }
    };

    // Print ApplyWrites results
    log.info("");
    log.info("=== APPLYWRITES RESULTS ===");
    for result in &results {
        log.info(&format!("Type:             {}", result.result_type));
        log.info(&format!("Uri:              {}", result.uri.as_deref().unwrap_or("<null>")));
        log.info(&format!("Cid:              {}", result.cid.as_ref().map(|c| c.base32.as_str()).unwrap_or("<null>")));
        log.info(&format!("ValidationStatus: {}", result.validation_status.as_deref().unwrap_or("<null>")));
    }

    // Get repo commit after ApplyWrites
    let commit_after = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit after ApplyWrites: {}", e));
            return;
        }
    };

    log.info("");
    log.info("=== REPO COMMIT AFTER APPLYWRITES ===");
    print_repo_commit_details(log, &db, &commit_after);

    // Print the commit as DAG-CBOR
    log.info("");
    log.info("=== COMMIT DAG-CBOR ===");
    match build_commit_dag_cbor_local(&db, &commit_after) {
        Ok(commit_dag_cbor) => {
            match commit_dag_cbor.to_bytes() {
                Ok(commit_bytes) => {
                    log.info(&format!("Commit DAG-CBOR hex ({} bytes):", commit_bytes.len()));
                    log.info(&hex_encode(&commit_bytes));
                }
                Err(e) => log.error(&format!("Failed to serialize commit: {}", e)),
            }
            log.info("Commit DAG-CBOR debug:");
            log.info(&commit_dag_cbor.get_recursive_debug_string(0));
            log.info("Commit JSON:");
            log.info(&commit_dag_cbor.to_json_string());
        }
        Err(e) => log.error(&format!("Failed to build commit DAG-CBOR: {}", e)),
    }

    // Get the new firehose event
    let seq_after = match db.get_most_recently_used_sequence_number() {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to get sequence number after: {}", e));
            return;
        }
    };
    log.info("");
    log.info(&format!("Sequence number after ApplyWrites: {}", seq_after));

    for seq in (seq_before + 1)..=seq_after {
        let event = match db.get_firehose_event(seq) {
            Ok(e) => e,
            Err(_) => {
                log.info(&format!("No firehose event at sequence {}.", seq));
                continue;
            }
        };

        log.info("");
        log.info(&format!("=== FIREHOSE EVENT {} ===", seq));
        log.info(&format!("Created:          {}", event.created_date));
        log.info(&format!("Header op:        {}", event.header_op));
        log.info(&format!("Header t:         {:?}", event.header_t));

        // Header DAG-CBOR
        log.info("");
        log.info("=== FIREHOSE HEADER DAG-CBOR ===");
        log.info(&format!("Header DAG-CBOR hex ({} bytes):", event.header_dag_cbor_bytes.len()));
        log.info(&hex_encode(&event.header_dag_cbor_bytes));
        let mut header_cursor = Cursor::new(&event.header_dag_cbor_bytes);
        match DagCborObject::read_from_stream(&mut header_cursor) {
            Ok(header_obj) => {
                log.info("Header JSON:");
                log.info(&header_obj.to_json_string());
                log.info("Header DAG-CBOR debug:");
                log.info(&header_obj.get_recursive_debug_string(0));
            }
            Err(e) => {
                log.error(&format!("Failed to parse header DAG-CBOR: {}", e));
            }
        }

        // Body DAG-CBOR
        log.info("");
        log.info("=== FIREHOSE BODY DAG-CBOR ===");
        log.info(&format!("Body DAG-CBOR hex ({} bytes):", event.body_dag_cbor_bytes.len()));
        log.info(&hex_encode(&event.body_dag_cbor_bytes));
        let mut body_cursor = Cursor::new(&event.body_dag_cbor_bytes);
        match DagCborObject::read_from_stream(&mut body_cursor) {
            Ok(body_obj) => {
                log.info("Body JSON:");
                log.info(&body_obj.to_json_string());
                log.info("Body DAG-CBOR debug:");
                log.info(&body_obj.get_recursive_debug_string(0));

                // Walk blocks inside the firehose body
                if let Some(blocks_obj) = body_obj.select_object(&["blocks"]) {
                    if let DagCborValue::ByteString(blocks_bytes) = &blocks_obj.value {
                        log.info("");
                        log.info(&format!("=== FIREHOSE BLOCKS ({} bytes) ===", blocks_bytes.len()));

                        let mut blocks_cursor = Cursor::new(blocks_bytes);
                        let walk_result = crate::repo::Repo::walk_repo(
                            &mut blocks_cursor,
                            |repo_header| {
                                log.info("CAR HEADER:");
                                log.info(&format!("   roots:   {}", repo_header.repo_commit_cid.get_base32()));
                                log.info(&format!("   version: {}", repo_header.version));
                                true
                            },
                            |repo_record| {
                                log.info("");
                                log.info(&format!("BLOCK CID: {}", repo_record.cid.get_base32()));

                                match repo_record.data_block.to_bytes() {
                                    Ok(block_bytes) => {
                                        log.info(&format!("BLOCK DAG-CBOR hex ({} bytes):", block_bytes.len()));
                                        log.info(&hex_encode(&block_bytes));
                                    }
                                    Err(e) => log.error(&format!("Failed to serialize block: {}", e)),
                                }

                                log.info("BLOCK JSON:");
                                log.info(&repo_record.json_string);
                                log.info("BLOCK DAG-CBOR debug:");
                                log.info(&repo_record.data_block.get_recursive_debug_string(0));
                                true
                            },
                        );

                        if let Err(e) = walk_result {
                            log.error(&format!("Error walking blocks: {}", e));
                        }
                    } else {
                        log.info("blocks field is not a byte string");
                    }
                }
            }
            Err(e) => {
                log.error(&format!("Failed to parse body DAG-CBOR: {}", e));
            }
        }
    }

    log.info("");
    log.info("=== DONE ===");
}

fn print_repo_commit_details(
    log: &crate::log::Logger,
    db: &crate::pds::db::PdsDb,
    commit: &crate::pds::db::DbRepoCommit,
) {
    log.info(&format!("Commit CID:        {}", commit.cid));
    log.info(&format!("Root MST Node CID: {}", commit.root_mst_node_cid));
    log.info(&format!("Rev:               {}", commit.rev));
    log.info(&format!("Version:           {}", commit.version));
    match db.get_config_property("UserDid") {
        Ok(did) => log.info(&format!("Did:               {}", did)),
        Err(_) => log.info("Did:               <unknown>"),
    }
    log.info(&format!("Prev MST Node CID: {}", commit.prev_mst_node_cid.as_deref().unwrap_or("<null>")));
    log.info(&format!("Signature hex ({} bytes):", commit.signature.len()));
    log.info(&hex_encode(&commit.signature));
}
