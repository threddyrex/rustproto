

use std::collections::HashMap;

use super::get_arg;

use crate::fs::{LocalFileSystem};
use crate::log::{logger};
use crate::ws::{BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};


pub async fn cmd_get_repo(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command GetRepo /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command GetRepo /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    // Initialize the local file system
    let lfs = match LocalFileSystem::initialize_with_create(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Error initializing data directory: {}", e));
            return;
        }
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    log.info(&format!("Resolving actor: {}", actor));

    // First, resolve actor to get DID and PDS
    let info = match client.resolve_actor_info(actor, None).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Error resolving actor: {}", e));
            return;
        }
    };

    let did = match &info.did {
        Some(d) => d.clone(),
        None => {
            log.error("Could not resolve DID for actor");
            return;
        }
    };

    let pds = match &info.pds {
        Some(p) => p.clone(),
        None => {
            log.error("Could not resolve PDS for actor");
            return;
        }
    };

    log.info(&format!("DID: {}", did));
    log.info(&format!("PDS: {}", pds));

    // Get the repo file path
    let repo_file = match lfs.get_path_repo_file(&did) {
        Ok(path) => path,
        Err(e) => {
            log.error(&format!("Error getting repo file path: {}", e));
            return;
        }
    };

    log.info(&format!("Downloading repo to: {}", repo_file.display()));

    // Download the repo
    match client.get_repo(&pds, &did, &repo_file).await {
        Ok(bytes) => {
            log.info(&format!("Downloaded {} bytes", bytes));
            log.info(&format!("Repo saved to: {}", repo_file.display()));
        }
        Err(e) => {
            log.error(&format!("Error downloading repo: {}", e));
        }
    }
}



