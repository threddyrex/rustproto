//! (run on PDS) list the WebAuthn passkeys stored in the PDS database in a human-readable format.

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::logger;
use crate::fs::LocalFileSystem;

pub fn cmd_list_passkeys(args: &HashMap<String, String>) {
    use crate::pds::db::PdsDb;

    let log = logger();

    // Get arguments
    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command ListPasskeys /dataDir <path>");
            return;
        }
    };

    // Initialize file system
    let lfs = match LocalFileSystem::initialize(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };

    // Connect to database
    let db = match PdsDb::connect(&lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to connect to PDS database: {}", e));
            return;
        }
    };

    // Get all passkeys
    let passkeys = match db.get_all_passkeys() {
        Ok(p) => p,
        Err(e) => {
            log.error(&format!("Failed to get passkeys: {}", e));
            return;
        }
    };

    log.info("");
    log.info("=== LIST PASSKEYS ===");
    log.info("");
    log.info(&format!("Passkey count: {}", passkeys.len()));
    log.info("");

    for (index, passkey) in passkeys.iter().enumerate() {
        log.info(&format!("[{}] Name:          {}", index, passkey.name));
        log.info(&format!("    CreatedDate:   {}", passkey.created_date));
        log.info(&format!("    CredentialId:  {}", passkey.credential_id));

        // PublicKey is stored as a JWK JSON string; pretty-print it if possible.
        match serde_json::from_str::<serde_json::Value>(&passkey.public_key) {
            Ok(jwk) => {
                let pretty = serde_json::to_string_pretty(&jwk)
                    .unwrap_or_else(|_| passkey.public_key.clone());
                log.info("    PublicKey (JWK):");
                for line in pretty.lines() {
                    log.info(&format!("      {}", line));
                }
            }
            Err(_) => {
                log.info(&format!("    PublicKey:     {}", passkey.public_key));
            }
        }

        log.info("");
    }
}
