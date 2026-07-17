//! (run on PDS) runs the PDS

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;
use crate::pds::server::PdsRunner;

pub async fn cmd_run_pds(args: &HashMap<String, String>) {
    let log = logger();

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command RunPds /dataDir <path>");
            return;
        }
    };

    let lfs = match LocalFileSystem::initialize(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };

    let runner = match PdsRunner::initialize(lfs, log) {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to initialize PDS server: {}", e));
            return;
        }
    };

    if let Err(e) = runner.run().await {
        log.error(&format!("PDS server error: {}", e));
    }
}


