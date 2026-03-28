

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;
use crate::pds::Installer;


pub fn cmd_install_db(args: &HashMap<String, String>) {
    let log = logger();

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command InstallDb /dataDir <path> [/deleteExistingDb true]");
            return;
        }
    };

    let delete_existing_db = get_arg(args, "deleteexistingdb")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let lfs = match LocalFileSystem::initialize(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };

    if let Err(e) = Installer::install_db(&lfs, &log, delete_existing_db) {
        log.error(&format!("Failed to install database: {}", e));
    }
}


