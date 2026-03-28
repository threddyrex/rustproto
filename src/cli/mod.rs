
pub mod repair_commit;

use std::collections::HashMap;

/// Gets an argument value or returns None.
pub fn get_arg<'a>(args: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    args.get(&key.to_lowercase()).map(|s| s.as_str())
}

