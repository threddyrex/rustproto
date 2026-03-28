
pub mod repair_commit;
pub mod install_db;
pub mod install_config;
pub mod run_pds;
pub mod resolve_actor;
pub mod walk_mst;
pub mod print_db_mst;

use std::collections::HashMap;

/// Gets an argument value or returns None.
pub fn get_arg<'a>(args: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    args.get(&key.to_lowercase()).map(|s| s.as_str())
}


/// Parses command line arguments in the format `/name1 value1 /name2 value2`.
pub fn parse_arguments(args: &[String]) -> Result<HashMap<String, String>, String> {
    if args.len() % 2 != 0 {
        return Err("Arguments must be in the format '/name1 value1 /name2 value2'".to_string());
    }

    let mut arguments = HashMap::new();

    for chunk in args.chunks(2) {
        let key = &chunk[0];
        let value = &chunk[1];

        if !key.starts_with('/') {
            return Err(format!(
                "Argument name must start with '/': {}",
                key
            ));
        }

        let key_name = key[1..].to_lowercase();
        arguments.insert(key_name, value.clone());
    }

    Ok(arguments)
}

/// Convert bytes to hex string for debugging.
pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
