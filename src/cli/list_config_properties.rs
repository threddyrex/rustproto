use std::collections::HashMap;
use std::io::{self, Write};

use crate::cli::get_arg;
use crate::pds::db::PdsDb;

pub fn cmd_list_config_properties(args: &HashMap<String, String>) -> Result<(), String> {
    let data_dir = get_arg(args, "datadir")
        .filter(|dir| !dir.is_empty())
        .ok_or("Usage: rustproto /command ListConfigProperties /dataDir <path>")?;
    let db = PdsDb::connect_from_data_dir(data_dir)
        .map_err(|e| format!("Failed to connect to PDS database: {e}"))?;
    let properties = db
        .get_all_config_properties()
        .map_err(|e| format!("Failed to read config properties: {e}"))?;

    let mut stdout = io::stdout().lock();
    for (key, value) in properties {
        writeln!(stdout, "{key}={value}")
            .map_err(|e| format!("Failed to write config properties: {e}"))?;
    }
    Ok(())
}
