

use std::collections::HashMap;
use std::io::Cursor;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;
use crate::firehose::Firehose;
use crate::repo::{DagCborValue, Repo};

pub async fn cmd_start_firehose_consumer(args: &HashMap<String, String>) {
    let log = logger();

    // Get actor argument
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command StartFirehoseConsumer /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let cursor = get_arg(args, "cursor");
    let show_dag_cbor_types = get_arg(args, "showdagcbortypes")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    // Resolve actor info to get PDS and DID
    log.info(&format!("Resolving actor: {}", actor));
    let actor_info = match client.resolve_actor_info(actor, None).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Failed to resolve actor info: {}", e));
            return;
        }
    };

    let pds = match &actor_info.pds {
        Some(p) => p.clone(),
        None => {
            log.error("Could not resolve PDS for actor");
            return;
        }
    };

    let target_did = match &actor_info.did {
        Some(d) => d.clone(),
        None => {
            log.error("Could not resolve DID for actor");
            return;
        }
    };

    // Build the firehose URL
    let mut url = format!("wss://{}/xrpc/com.atproto.sync.subscribeRepos", pds);
    if let Some(c) = cursor {
        url = format!("{}?cursor={}", url, c);
    }

    log.info(&format!("Connecting to firehose at: {}", url));

    // Listen on firehose
    let result = Firehose::listen(&url, |header, body| {
        // Filter to only our DID
        let did = body.select_string(&["repo"]);
        if did.as_ref() != Some(&target_did) {
            return true; // continue listening
        }

        log.info(" -----------------------------------------------------------------------------------------------------------");
        log.info(" NEW FIREHOSE FRAME");
        log.info(" -----------------------------------------------------------------------------------------------------------");

        log.info(&format!("DAG CBOR OBJECT 1 (HEADER):\n{}", header.to_json_string()));
        log.info(&format!("DAG CBOR OBJECT 2 (MESSAGE):\n{}", body.to_json_string()));

        if show_dag_cbor_types {
            log.trace(&format!("\nDAG CBOR OBJECT 1 TYPES (HEADER):\n{}", header.get_recursive_debug_string(0)));
            log.trace(&format!("\nDAG CBOR OBJECT 2 TYPES (MESSAGE):\n{}", body.get_recursive_debug_string(0)));
        }

        log.info(" PARSING BLOCKS");

        // Look for the "blocks" key in the message
        // "blocks" should be a byte array of records, in repo format
        if let Some(blocks_obj) = body.select_object(&["blocks"]) {
            if let DagCborValue::ByteString(blocks_bytes) = &blocks_obj.value {
                let mut cursor = Cursor::new(blocks_bytes);

                // Walk it like a repo
                let walk_result = Repo::walk_repo(
                    &mut cursor,
                    |repo_header| {
                        log.info("REPO HEADER:");
                        log.info(&format!("   roots: {}", repo_header.repo_commit_cid.get_base32()));
                        log.info(&format!("   version: {}", repo_header.version));
                        true
                    },
                    |repo_record| {
                        log.info(&format!("cid: {}", repo_record.cid.get_base32()));
                        log.info("BLOCK JSON:");
                        log.info(&format!("\n{}", repo_record.json_string));

                        if show_dag_cbor_types {
                            log.trace(&format!("\n{}", repo_record.data_block.get_recursive_debug_string(0)));
                        }

                        true
                    },
                );

                if let Err(e) = walk_result {
                    log.error(&format!("Error walking blocks: {}", e));
                }
            } else {
                log.info("No blocks found in message (blocks is not a byte string).");
            }
        } else {
            log.info("No blocks found in message.");
        }

        true // continue listening
    }).await;

    if let Err(e) = result {
        log.error(&format!("Firehose error: {}", e));
    }
}


