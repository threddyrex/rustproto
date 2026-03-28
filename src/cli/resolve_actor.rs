

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
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
        }
        Err(e) => {
            log.error(&format!("Error resolving actor: {}", e));
        }
    }
}


