//! resolve a lexicon schema by NSID via DNS, DID, and PDS

use std::collections::HashMap;

use crate::cli::get_arg;
use crate::fs::LocalFileSystem;
use crate::log::logger;
use crate::ws::{BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};

/// Resolves a lexicon schema by NSID and prints the JSON response.
pub async fn cmd_resolve_lexicon_schema(args: &HashMap<String, String>) {
    let log = logger();

    let schema = match get_arg(args, "schema") {
        Some(s) => s,
        None => {
            log.error("missing /schema argument");
            log.error("Usage: rustproto /command ResolveLexiconSchema /schema <lexicon_id>");
            return;
        }
    };

    let response = if let Some(data_dir) = get_arg(args, "datadir") {
        let lfs = match LocalFileSystem::initialize_with_create(data_dir) {
            Ok(lfs) => lfs,
            Err(e) => {
                log.error(&format!("Error initializing data directory: {}", e));
                return;
            }
        };

        match lfs
            .resolve_lexicon_schema(schema, None, DEFAULT_APP_VIEW_HOST_NAME)
            .await
        {
            Ok(response) => response,
            Err(e) => {
                log.error(&format!("Error resolving lexicon schema: {}", e));
                return;
            }
        }
    } else {
        let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);
        match client.resolve_lexicon_schema(schema).await {
            Ok(response) => response,
            Err(e) => {
                log.error(&format!("Error resolving lexicon schema: {}", e));
                return;
            }
        }
    };

    match serde_json::to_string_pretty(&response) {
        Ok(response) => {
            log.info(&response);
        }
        Err(e) => {
            log.error(&format!("Error formatting lexicon schema JSON: {}", e));
        }
    }
}
