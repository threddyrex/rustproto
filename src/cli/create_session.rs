

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;
use crate::fs::LocalFileSystem;



pub async fn cmd_create_session(args: &HashMap<String, String>) {
    let log = logger();

    //
    // Get arguments.
    //
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command CreateSession /actor <handle_or_did> /dataDir <path> /password <password>");
            return;
        }
    };

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command CreateSession /actor <handle_or_did> /dataDir <path> /password <password>");
            return;
        }
    };

    let password = match get_arg(args, "password") {
        Some(p) => p.to_string(),
        None => {
            // Prompt for password
            log.info(&format!("Actor is {}.", actor));
            eprint!("please enter password:");
            let mut password = String::new();
            match std::io::stdin().read_line(&mut password) {
                Ok(_) => password.trim().to_string(),
                Err(e) => {
                    log.error(&format!("Failed to read password: {}", e));
                    return;
                }
            }
        }
    };

    let auth_factor_token = get_arg(args, "authfactortoken").map(|s| s.to_string());

    //
    // Initialize local file system and resolve actor info.
    //
    let lfs = match LocalFileSystem::initialize_with_create(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize local file system: {}", e));
            return;
        }
    };

    let actor_info = match lfs.resolve_actor_info(actor, None, DEFAULT_APP_VIEW_HOST_NAME).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Failed to resolve actor info: {}", e));
            return;
        }
    };

    let did = match &actor_info.did {
        Some(d) => d.clone(),
        None => {
            log.error("Failed to resolve actor to DID.");
            return;
        }
    };

    let pds = match &actor_info.pds {
        Some(p) => p.clone(),
        None => {
            log.warning("Failed to resolve PDS, defaulting to bsky.social");
            "bsky.social".to_string()
        }
    };

    log.info(&format!("Resolving handle to get pds: {}", pds));

    //
    // Get session file path.
    //
    let session_file = match lfs.get_path_session_file(&did) {
        Ok(p) => p,
        Err(e) => {
            log.error(&format!("Failed to get session file path: {}", e));
            return;
        }
    };

    //
    // Create session.
    //
    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    let mut session = match client
        .create_session(
            &pds,
            actor,
            &password,
            auth_factor_token.as_deref(),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to create session: {}", e));
            return;
        }
    };

    // Add pds to the session JSON
    session["pds"] = serde_json::Value::String(pds);

    //
    // Write session to disk.
    //
    log.info(&format!("Writing session file: {}", session_file.display()));
    let session_string = serde_json::to_string_pretty(&session).unwrap_or_default();
    if let Err(e) = std::fs::write(&session_file, &session_string) {
        log.error(&format!("Failed to write session file: {}", e));
        return;
    }

    // Print the session response
    log.info("");
    log.info(&session_string);
}

