

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;
use crate::fs::LocalFileSystem;


pub async fn cmd_backup_account(args: &HashMap<String, String>) {
    let log = logger();

    //
    // Get arguments.
    //
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command BackupAccount /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command BackupAccount /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let get_prefs = get_arg(args, "getprefs")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let get_repo = get_arg(args, "getrepo")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let get_blobs = get_arg(args, "getblobs")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let blob_sleep_seconds: u64 = get_arg(args, "blobsleepseconds")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    log.trace(&format!("actor: {}", actor));
    log.trace(&format!("dataDir: {}", data_dir));
    log.trace(&format!("getPrefs: {}", get_prefs));
    log.trace(&format!("getRepo: {}", get_repo));
    log.trace(&format!("getBlobs: {}", get_blobs));
    log.trace(&format!("blobSleepSeconds: {}", blob_sleep_seconds));

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
            log.error(&format!("Failed to resolve actor info for actor: {} - {}", actor, e));
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
            log.error("Failed to resolve actor to PDS.");
            return;
        }
    };

    log.info(&format!("Resolved handle to did: {}", did));
    log.info(&format!("Resolved handle to pds: {}", pds));

    //
    // Load session (for authenticated requests like prefs).
    //
    let session = lfs.load_session(&did, None);
    let access_jwt = session
        .as_ref()
        .and_then(|s| s["accessJwt"].as_str())
        .map(|s| s.to_string());

    let has_session = access_jwt.is_some();

    if !has_session {
        log.warning(&format!(
            "Failed to load session for actor: {}. Will not be able to backup prefs.",
            actor
        ));
    }

    //
    // Verify session by calling getPreferences.
    //
    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    if has_session && get_prefs {
        log.info("Verifying session by calling getPreferences...");
        match client.get_preferences(&pds, access_jwt.as_ref().unwrap()).await {
            Ok(_) => {
                log.info("Session verified successfully.");
            }
            Err(e) => {
                log.error(&format!(
                    "Failed to verify session with getPreferences call. Is the session still valid? Error: {}",
                    e
                ));
                return;
            }
        }
    }

    //
    // Set up backup directory.
    //
    let backup_dir = match lfs.get_path_account_backup_dir(&did) {
        Ok(d) => d,
        Err(e) => {
            log.error(&format!("Failed to get backup directory: {}", e));
            return;
        }
    };

    if !backup_dir.exists() {
        log.trace(&format!("Creating backup directory: {}", backup_dir.display()));
        if let Err(e) = std::fs::create_dir_all(&backup_dir) {
            log.error(&format!("Failed to create backup directory: {}", e));
            return;
        }
    }

    //
    // Create README.txt
    //
    let readme_path = backup_dir.join("README.txt");
    let readme_contents = format!(
        "Account backup for Bluesky account. Created by rustproto.\n\n\
         actor: {}\n\
         backupDir: {}\n\
         getPrefs: {}\n\
         getRepo: {}\n\
         getBlobs: {}\n",
        actor,
        backup_dir.display(),
        get_prefs,
        get_repo,
        get_blobs
    );
    log.info(&format!("Creating readme file: {}", readme_path.display()));
    if let Err(e) = std::fs::write(&readme_path, &readme_contents) {
        log.error(&format!("Failed to create readme file: {}", e));
        return;
    }

    //
    // Get prefs.
    //
    if get_prefs && has_session {
        log.info("");
        log.info("----- PREFS -----");
        match client.get_preferences(&pds, access_jwt.as_ref().unwrap()).await {
            Ok(prefs) => {
                let prefs_file = backup_dir.join("prefs.json");
                log.info(&format!("Creating prefs file: {}", prefs_file.display()));
                let prefs_string = serde_json::to_string_pretty(&prefs).unwrap_or_default();
                if let Err(e) = std::fs::write(&prefs_file, prefs_string) {
                    log.error(&format!("Failed to write prefs file: {}", e));
                    return;
                }
            }
            Err(e) => {
                log.error(&format!("Failed to get preferences: {}", e));
                return;
            }
        }
    }

    //
    // Get repo.
    //
    if get_repo {
        let repo_file = backup_dir.join("repo.car");
        log.info("");
        log.info("----- REPO -----");
        log.info(&format!("Getting repo file: {}", repo_file.display()));
        match client.get_repo(&pds, &did, &repo_file).await {
            Ok(bytes) => {
                log.info(&format!("Downloaded {} bytes", bytes));
            }
            Err(e) => {
                log.error(&format!("Failed to download repo: {}", e));
            }
        }
    }

    //
    // Get blobs.
    //
    if get_blobs {
        log.info("");
        log.info("----- BLOBS -----");

        // List blobs
        let blobs = match client.list_blobs(&pds, &did).await {
            Ok(b) => b,
            Err(e) => {
                log.error(&format!("Failed to list blobs: {}", e));
                return;
            }
        };

        let blob_list_file = backup_dir.join("blobs.txt");
        log.info(&format!("Found {} blobs.", blobs.len()));
        log.info(&format!("Creating blob list file: {}", blob_list_file.display()));
        if let Err(e) = std::fs::write(&blob_list_file, blobs.join("\n")) {
            log.error(&format!("Failed to write blob list file: {}", e));
            return;
        }

        // Create blobs directory
        let blobs_directory = backup_dir.join("blobs");
        if !blobs_directory.exists() {
            log.info(&format!("Creating blobs directory: {}", blobs_directory.display()));
            if let Err(e) = std::fs::create_dir_all(&blobs_directory) {
                log.error(&format!("Failed to create blobs directory: {}", e));
                return;
            }
        }

        // Download all blobs
        let mut blob_count_downloaded = 0;
        let mut blob_count_skipped = 0;

        for blob in &blobs {
            let blob_path = blobs_directory.join(blob);

            if !blob_path.exists() {
                log.info(&format!("Downloading blob: {}", blob_path.display()));
                match client.get_blob(&pds, &did, blob, &blob_path).await {
                    Ok(_) => {}
                    Err(e) => {
                        log.error(&format!("Failed to download blob {}: {}", blob, e));
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(blob_sleep_seconds)).await;
                blob_count_downloaded += 1;
            } else {
                log.trace(&format!("Blob file already exists, skipping: {}", blob_path.display()));
                blob_count_skipped += 1;
            }
        }

        // Count blob files on disk
        let blob_file_count = std::fs::read_dir(&blobs_directory)
            .map(|rd| rd.count())
            .unwrap_or(0);

        log.info(&format!(
            "Downloaded {} blobs, skipped {} blobs. There are {} blob files on disk.",
            blob_count_downloaded, blob_count_skipped, blob_file_count
        ));

        log.info("");
    }
}

