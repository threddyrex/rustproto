//! resolve actor info by handle or DID, print in human-readable format and as JSON, and at trace level also print the raw DID document pretty-printed if available.

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger, LogLevel};
use crate::ws::ActorQueryOptions;
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;


pub async fn cmd_resolve_actor(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command ResolveActorInfo /actor <handle_or_did>");
            return;
        }
    };

    let use_all = get_arg(args, "all")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let options = if use_all {
        ActorQueryOptions::all()
    } else {
        ActorQueryOptions::default()
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    log.info(&format!("Resolving actor: {}", actor));

    match client.resolve_actor_info(actor, Some(options)).await {
        Ok(info) => {
            log.info("");
            log.info("=== Actor Info ===");
            if let Some(ref handle) = info.handle {
                log.info(&format!("Handle: {}", handle));
            }
            if let Some(ref did) = info.did {
                log.info(&format!("DID: {}", did));
            }
            if let Some(ref pds) = info.pds {
                log.info(&format!("PDS: {}", pds));
            }
            if let Some(ref pubkey) = info.public_key_multibase {
                log.info(&format!("Public Key: {}", pubkey));
            }

            // Output as JSON
            if let Ok(json) = info.to_json_string() {
                log.info("");
                log.info("=== JSON ===");
                log.info(&json);
            }

            // At trace level, also dump the raw DID document pretty-printed
            if log.level() <= LogLevel::Trace {
                if let Some(ref did_doc) = info.did_doc {
                    log.trace("");
                    log.trace("=== DID Document (pretty) ===");
                    match serde_json::from_str::<serde_json::Value>(did_doc) {
                        Ok(value) => match serde_json::to_string_pretty(&value) {
                            Ok(pretty) => log.trace(&pretty),
                            Err(_) => log.trace(did_doc),
                        },
                        Err(_) => log.trace(did_doc),
                    }
                }
            }
        }
        Err(e) => {
            log.error(&format!("Error resolving actor: {}", e));
        }
    }
}


