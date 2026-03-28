

use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::fs::LocalFileSystem;
use crate::pds::Installer;

pub fn cmd_install_config(args: &HashMap<String, String>) {
    let log = logger();

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command InstallConfig /dataDir <path> /listenScheme <http|https> /listenHost <host> /listenPort <port>");
            return;
        }
    };

    let listen_scheme = match get_arg(args, "listenscheme") {
        Some(s) => s,
        None => {
            log.error("missing /listenScheme argument");
            return;
        }
    };

    let listen_host = match get_arg(args, "listenhost") {
        Some(h) => h,
        None => {
            log.error("missing /listenHost argument");
            return;
        }
    };

    let listen_port: i32 = match get_arg(args, "listenport") {
        Some(p) => match p.parse() {
            Ok(port) => port,
            Err(_) => {
                log.error("Invalid /listenPort value - must be an integer");
                return;
            }
        },
        None => {
            log.error("missing /listenPort argument");
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

    if let Err(e) = Installer::install_config(&lfs, &log, listen_scheme, listen_host, listen_port) {
        log.error(&format!("Failed to install config: {}", e));
    }
}

