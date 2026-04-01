

use std::collections::HashMap;

use super::get_arg;

use crate::fs::LocalFileSystem;
use crate::log::logger;
use crate::ws::{BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};


pub async fn cmd_get_blob(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command GetBlob /actor <handle_or_did> /blobCid <cid> /dataDir <path>");
            return;
        }
    };

    let blob_cid = match get_arg(args, "blobcid") {
        Some(c) => c,
        None => {
            log.error("missing /blobCid argument");
            log.error("Usage: rustproto /command GetBlob /actor <handle_or_did> /blobCid <cid> /dataDir <path>");
            return;
        }
    };

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command GetBlob /actor <handle_or_did> /blobCid <cid> /dataDir <path>");
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

    // Resolve actor to get DID and PDS
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

    // Build the blobs output directory: {dataDir}/blobs/{safe_did}/
    let blobs_dir = match lfs.get_path_blob_dir(&did) {
        Ok(path) => path,
        Err(e) => {
            log.error(&format!("Error getting blobs directory path: {}", e));
            return;
        }
    };

    if !blobs_dir.exists() {
        log.info(&format!("Creating blobs directory: {}", blobs_dir.display()));
        if let Err(e) = std::fs::create_dir_all(&blobs_dir) {
            log.error(&format!("Failed to create blobs directory: {}", e));
            return;
        }
    }

    let blob_path = blobs_dir.join(blob_cid);

    if blob_path.exists() {
        log.info(&format!("Blob file already exists: {}", blob_path.display()));
        return;
    }

    let url = format!(
        "https://{}/xrpc/com.atproto.sync.getBlob?did={}&cid={}",
        pds, did, blob_cid
    );
    log.info(&format!("URL: {}", url));
    log.info(&format!("Downloading blob {} to: {}", blob_cid, blob_path.display()));

    match client.get_blob(&pds, &did, blob_cid, &blob_path).await {
        Ok(bytes) => {
            log.info(&format!("Downloaded {} bytes", bytes));
            log.info(&format!("Blob saved to: {}", blob_path.display()));
        }
        Err(e) => {
            log.error(&format!("Error downloading blob: {}", e));
        }
    }
}
