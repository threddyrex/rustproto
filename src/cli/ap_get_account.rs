//! `ApGetAccount` CLI command.
//!
//! Takes an actor (e.g. `gargron@mastodon.social`) and:
//! 1. Calls `GET https://{host}/api/v1/accounts/lookup?acct={user}` to map
//!    the handle to a numeric Mastodon account id.
//! 2. Calls `GET https://{host}/api/v1/accounts/{id}` to fetch the account.

use std::collections::HashMap;

use crate::ap::{parse_actor_handle, ApClient};
use crate::cli::get_arg;
use crate::log::logger;

pub async fn cmd_ap_get_account(args: &HashMap<String, String>) {
    let log = logger();

    let actor_arg = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command ApGetAccount /actor <user@host|@user@host|acct:user@host>");
            return;
        }
    };

    let (user, host) = match parse_actor_handle(actor_arg) {
        Ok(pair) => pair,
        Err(e) => {
            log.error(&format!("Invalid actor '{}': {}", actor_arg, e));
            return;
        }
    };

    let client = ApClient::new();

    log.info(&format!("Getting Mastodon account for actor: {}@{}", user, host));

    // -----------------------------------------------------------------
    // Step 1: lookup -> numeric id
    // -----------------------------------------------------------------
    log.info("");
    log.info("============================================================");
    log.info("=== Step 1: GET /api/v1/accounts/lookup?acct={user} ===");
    log.info("============================================================");

    let (lookup_raw, lookup) = match client.lookup_mastodon_account(&host, &user).await {
        Ok(v) => v,
        Err(e) => {
            log.error(&format!("Error looking up account: {}", e));
            return;
        }
    };

    let id = match lookup.id.clone() {
        Some(s) => s,
        None => {
            log.error("Lookup response did not contain an 'id' field");
            log.error(&serde_json::to_string_pretty(&lookup_raw).unwrap_or_default());
            return;
        }
    };

    log.info(&format!("Resolved id: {}", id));

    // -----------------------------------------------------------------
    // Step 2: GET /api/v1/accounts/:id
    // -----------------------------------------------------------------
    log.info("");
    log.info("============================================================");
    log.info("=== Step 2: GET /api/v1/accounts/{id} ===");
    log.info("============================================================");

    let (raw, account) = match client.get_mastodon_account(&host, &id).await {
        Ok(v) => v,
        Err(e) => {
            log.error(&format!("Error fetching account: {}", e));
            return;
        }
    };

    if let Some(ref s) = account.id {
        log.info(&format!("id              : {}", s));
    }
    if let Some(ref s) = account.username {
        log.info(&format!("username        : {}", s));
    }
    if let Some(ref s) = account.acct {
        log.info(&format!("acct            : {}", s));
    }
    if let Some(ref s) = account.display_name {
        log.info(&format!("display_name    : {}", s));
    }
    if let Some(ref s) = account.url {
        log.info(&format!("url             : {}", s));
    }
    if let Some(ref s) = account.uri {
        log.info(&format!("uri             : {}", s));
    }
    if let Some(ref s) = account.created_at {
        log.info(&format!("created_at      : {}", s));
    }
    if let Some(n) = account.followers_count {
        log.info(&format!("followers_count : {}", n));
    }
    if let Some(n) = account.following_count {
        log.info(&format!("following_count : {}", n));
    }
    if let Some(n) = account.statuses_count {
        log.info(&format!("statuses_count  : {}", n));
    }

    log.info("");
    log.info("--- Account JSON ---");
    match serde_json::to_string_pretty(&raw) {
        Ok(s) => log.info(&s),
        Err(e) => log.error(&format!("Failed to format JSON: {}", e)),
    }
}
