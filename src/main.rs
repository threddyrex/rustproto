//! rstproto CLI - AT Protocol / Bluesky tools

use std::collections::HashMap;
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

    let command = arguments
        .get("command")
        .map(|s| s.as_str())
        .unwrap_or("help");

    match command.to_lowercase().as_str() {
        "resolve" | "resolveactorinfo" => cmd_resolve_actor(&arguments).await,
        "help" => print_usage(),
        _ => {
            eprintln!("Unknown command: {}", command);
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
    println!("  Help               Show this help message");
    println!();
    println!("Arguments:");
    println!("  /command <name>    Command to run");
    println!("  /actor <handle>    Handle or DID to resolve");
    println!("  /all <true|false>  Use all resolution methods");
    println!();
    println!("Examples:");
    println!("  rstproto /command ResolveActorInfo /actor alice.bsky.social");
    println!("  rstproto /command ResolveActorInfo /actor did:plc:abc123 /all true");
}

async fn cmd_resolve_actor(args: &HashMap<String, String>) {
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            eprintln!("Error: missing /actor argument");
            eprintln!("Usage: rstproto /command ResolveActorInfo /actor <handle_or_did>");
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

    println!("Resolving actor: {}", actor);

    match client.resolve_actor_info(actor, Some(options)).await {
        Ok(info) => {
            println!("\n=== Actor Info ===");
            if let Some(ref handle) = info.handle {
                println!("Handle: {}", handle);
            }
            if let Some(ref did) = info.did {
                println!("DID: {}", did);
            }
            if let Some(ref pds) = info.pds {
                println!("PDS: {}", pds);
            }
            if let Some(ref pubkey) = info.public_key_multibase {
                println!("Public Key: {}", pubkey);
            }

            // Output as JSON
            if let Ok(json) = info.to_json_string() {
                println!("\n=== JSON ===");
                println!("{}", json);
            }
        }
        Err(e) => {
            eprintln!("Error resolving actor: {}", e);
        }
    }
}
