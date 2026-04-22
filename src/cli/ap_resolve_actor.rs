//! `ApResolveActor` CLI command.
//!
//! Resolves an ActivityPub actor via WebFinger followed by an actor object fetch.

use std::collections::HashMap;

use crate::ap::ApClient;
use crate::cli::get_arg;
use crate::log::logger;

pub async fn cmd_ap_resolve_actor(args: &HashMap<String, String>) {
    let log = logger();

    let actor_arg = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command ApResolveActor /actor <user@host|@user@host|acct:user@host|url>");
            return;
        }
    };

    let client = ApClient::new();

    log.info(&format!("Resolving ActivityPub actor: {}", actor_arg));

    match client.resolve_actor(actor_arg).await {
        Ok(result) => {
            // -----------------------------------------------------------------
            // Step 1: WebFinger GET
            // -----------------------------------------------------------------
            log.info("");
            log.info("============================================================");
            log.info("=== Step 1: WebFinger GET (response) ===");
            log.info("============================================================");
            if let Some(ref s) = result.webfinger_url {
                log.info(&format!("Request URL : {}", s));
            } else {
                log.info("Request URL : (skipped - input was a direct actor URL)");
            }
            if let Some(ref s) = result.webfinger_subject {
                log.info(&format!("Resource    : {}", s));
            }

            if let Some(ref wf) = result.webfinger {
                if let Some(ref s) = wf.subject {
                    log.info(&format!("subject     : {}", s));
                }
                if !wf.aliases.is_empty() {
                    log.info(&format!("aliases     : {}", wf.aliases.join(", ")));
                }
                log.info(&format!("links       : {} entr{}",
                    wf.links.len(),
                    if wf.links.len() == 1 { "y" } else { "ies" }));
                for (i, link) in wf.links.iter().enumerate() {
                    log.info(&format!(
                        "  [{}] rel={} type={} href={}",
                        i,
                        link.rel.as_deref().unwrap_or("-"),
                        link.link_type.as_deref().unwrap_or("-"),
                        link.href.as_deref().unwrap_or("-"),
                    ));
                }
                log.info(&format!(
                    "Picked self : {} (used as Step 2 URL)",
                    result.actor_url.as_deref().unwrap_or("-")
                ));

                log.info("");
                log.info("--- Step 1 JSON (WebFinger response) ---");
                match serde_json::to_string_pretty(wf) {
                    Ok(s) => log.info(&s),
                    Err(e) => log.error(&format!("Failed to format JSON: {}", e)),
                }
            } else {
                log.info("(WebFinger was skipped because the input was a direct actor URL)");
            }

            // -----------------------------------------------------------------
            // Step 2: Actor object GET
            // -----------------------------------------------------------------
            log.info("");
            log.info("============================================================");
            log.info("=== Step 2: Actor object GET (response) ===");
            log.info("============================================================");
            if let Some(ref s) = result.actor_url {
                log.info(&format!("Request URL : {}", s));
            }

            let a = &result.actor;
            if let Some(ref s) = a.id {
                log.info(&format!("id                : {}", s));
            }
            if let Some(ref s) = a.actor_type {
                log.info(&format!("type              : {}", s));
            }
            if let Some(ref s) = a.preferred_username {
                log.info(&format!("preferredUsername : {}", s));
            }
            if let Some(ref s) = a.name {
                log.info(&format!("name              : {}", s));
            }
            if let Some(ref s) = a.url {
                log.info(&format!("url               : {}", s));
            }
            if let Some(ref s) = a.inbox {
                log.info(&format!("inbox             : {}", s));
            }
            if let Some(ref s) = a.outbox {
                log.info(&format!("outbox            : {}", s));
            }
            if let Some(ref s) = a.followers {
                log.info(&format!("followers         : {}", s));
            }
            if let Some(ref s) = a.following {
                log.info(&format!("following         : {}", s));
            }
            if let Some(pk) = a.public_key.as_ref().and_then(|p| p.first()) {
                if let Some(ref s) = pk.id {
                    log.info(&format!("publicKey.id      : {}", s));
                }
            }

            log.info("");
            log.info("--- Step 2 JSON (Actor object) ---");
            match serde_json::to_string_pretty(&result.raw) {
                Ok(s) => log.info(&s),
                Err(e) => log.error(&format!("Failed to format JSON: {}", e)),
            }
        }
        Err(e) => {
            log.error(&format!("Error resolving ActivityPub actor: {}", e));
        }
    }
}
