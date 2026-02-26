//! rstproto CLI - AT Protocol / Bluesky tools

use std::collections::HashMap;
use std::sync::Arc;
use rstproto::fs::LocalFileSystem;
use rstproto::log::{init_logger, logger, FileDestination, LogLevel};
use rstproto::ws::{ActorQueryOptions, BlueskyClient};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let arguments = match parse_arguments(&args) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("Error: {}", e);
            print_usage();
            return;
        }
    };

    // Initialize logger based on arguments
    let log_level = get_arg(&arguments, "loglevel")
        .map(|s| s.parse::<LogLevel>().unwrap_or_default())
        .unwrap_or(LogLevel::Info);

    let log = init_logger(log_level);

    // Add file destination if logToDataDir is true
    let log_to_data_dir = get_arg(&arguments, "logtodatadir")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if log_to_data_dir {
        if let Some(data_dir) = get_arg(&arguments, "datadir") {
            let command = get_arg(&arguments, "command").unwrap_or("unknown");
            if let Ok(file_dest) = FileDestination::from_data_dir(data_dir, command) {
                log.add_destination(Arc::new(file_dest));
            }
        }
    }

    let command = arguments
        .get("command")
        .map(|s| s.as_str())
        .unwrap_or("help");

    match command.to_lowercase().as_str() {
        "resolve" | "resolveactorinfo" => cmd_resolve_actor(&arguments).await,
        "getrepo" => cmd_get_repo(&arguments).await,
        "help" => print_usage(),
        _ => {
            logger().error(&format!("Unknown command: {}", command));
            print_usage();
        }
    }
}

/// Parses command line arguments in the format `/name1 value1 /name2 value2`.
fn parse_arguments(args: &[String]) -> Result<HashMap<String, String>, String> {
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

/// Gets an argument value or returns None.
fn get_arg<'a>(args: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    args.get(&key.to_lowercase()).map(|s| s.as_str())
}

fn print_usage() {
    println!("rstproto - AT Protocol / Bluesky CLI tools");
    println!();
    println!("Usage: rstproto /command <name> [/arg1 val1 /arg2 val2 ...]");
    println!();
    println!("Commands:");
    println!("  ResolveActorInfo   Resolve actor info (DID, PDS, etc.)");
    println!("  GetRepo            Download repository (CAR file) for an actor");
    println!("  Help               Show this help message");
    println!();
    println!("Arguments:");
    println!("  /command <name>       Command to run");
    println!("  /actor <handle>       Handle or DID to resolve");
    println!("  /all <true|false>     Use all resolution methods");
    println!("  /dataDir <path>       Path to data directory");
    println!("  /logLevel <level>     Log level: trace, info, warning, error");
    println!("  /logToDataDir <bool>  Write logs to data directory");
    println!();
    println!("Examples:");
    println!("  rstproto /command ResolveActorInfo /actor alice.bsky.social");
    println!("  rstproto /command ResolveActorInfo /actor did:plc:abc123 /all true");
    println!("  rstproto /command GetRepo /actor alice.bsky.social /dataDir ./data");
    println!("  rstproto /command GetRepo /actor alice.bsky.social /dataDir ./data /logLevel trace /logToDataDir true");
}

async fn cmd_resolve_actor(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rstproto /command ResolveActorInfo /actor <handle_or_did>");
            return;
        }
    };

    let use_all = get_arg(args, "all")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let options = if use_all {
        ActorQueryOptions::all()
    } else {
        ActorQueryOptions::default()
    };

    let client = BlueskyClient::new();

    log.info(&format!("Resolving actor: {}", actor));

    match client.resolve_actor_info(actor, Some(options)).await {
        Ok(info) => {
            log.info("");
            log.info("=== Actor Info ===");
            if let Some(ref handle) = info.handle {
                log.info(&format!("Handle: {}", handle));
            }
            if let Some(ref did) = info.did {
                log.info(&format!("DID: {}", did));
            }
            if let Some(ref pds) = info.pds {
                log.info(&format!("PDS: {}", pds));
            }
            if let Some(ref pubkey) = info.public_key_multibase {
                log.info(&format!("Public Key: {}", pubkey));
            }

            // Output as JSON
            if let Ok(json) = info.to_json_string() {
                log.info("");
                log.info("=== JSON ===");
                log.info(&json);
            }
        }
        Err(e) => {
            log.error(&format!("Error resolving actor: {}", e));
        }
    }
}

async fn cmd_get_repo(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rstproto /command GetRepo /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rstproto /command GetRepo /actor <handle_or_did> /dataDir <path>");
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

    let client = BlueskyClient::new();

    log.info(&format!("Resolving actor: {}", actor));

    // First, resolve actor to get DID and PDS
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

    // Get the repo file path
    let repo_file = match lfs.get_path_repo_file(&did) {
        Ok(path) => path,
        Err(e) => {
            log.error(&format!("Error getting repo file path: {}", e));
            return;
        }
    };

    log.info(&format!("Downloading repo to: {}", repo_file.display()));

    // Download the repo
    match client.get_repo(&pds, &did, &repo_file).await {
        Ok(bytes) => {
            log.info(&format!("Downloaded {} bytes", bytes));
            log.info(&format!("Repo saved to: {}", repo_file.display()));
        }
        Err(e) => {
            log.error(&format!("Error downloading repo: {}", e));
        }
    }
}
