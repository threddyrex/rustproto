//! rstproto CLI - AT Protocol / Bluesky tools

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use chrono::Datelike;
use rstproto::firehose::Firehose;
use rstproto::fs::LocalFileSystem;
use rstproto::log::{init_logger, logger, FileDestination, LogLevel};
use rstproto::repo::{DagCborValue, Repo, RepoRecord, AtProtoType};
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
        "printrepostats" => cmd_print_repo_stats(&arguments).await,
        "printreporecords" => cmd_print_repo_records(&arguments).await,
        "startfirehoseconsumer" => cmd_start_firehose_consumer(&arguments).await,
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
    println!("  ResolveActorInfo       Resolve actor info (DID, PDS, etc.)");
    println!("  GetRepo                Download repository (CAR file) for an actor");
    println!("  PrintRepoStats         Print statistics about a repository");
    println!("  PrintRepoRecords       Print records from a repository");
    println!("  StartFirehoseConsumer  Listen to a PDS firehose and print events");
    println!("  Help                   Show this help message");
    println!();
    println!("Arguments:");
    println!("  /command <name>       Command to run");
    println!("  /actor <handle>       Handle or DID to resolve");
    println!("  /all <true|false>     Use all resolution methods");
    println!("  /dataDir <path>       Path to data directory");
    println!("  /repoFile <path>      Path to CAR file (for repo commands)");
    println!("  /collection <type>    Filter by collection type (e.g., app.bsky.feed.post)");
    println!("  /month <yyyy-MM>      Filter by month (e.g., 2024-01)");
    println!("  /cursor <int>         Firehose cursor position");
    println!("  /showDagCborTypes     Show DAG-CBOR type debug info (true/false)");
    println!("  /logLevel <level>     Log level: trace, info, warning, error");
    println!("  /logToDataDir <bool>  Write logs to data directory");
    println!();
    println!("Examples:");
    println!("  rstproto /command ResolveActorInfo /actor alice.bsky.social");
    println!("  rstproto /command ResolveActorInfo /actor did:plc:abc123 /all true");
    println!("  rstproto /command GetRepo /actor alice.bsky.social /dataDir ./data");
    println!("  rstproto /command PrintRepoStats /repoFile ./data/repos/did_plc_xxx/repo.car");
    println!("  rstproto /command PrintRepoRecords /actor alice.bsky.social /dataDir ./data");
    println!("  rstproto /command PrintRepoRecords /repoFile ./repo.car /collection app.bsky.feed.post");
    println!("  rstproto /command StartFirehoseConsumer /actor alice.bsky.social /dataDir ./data");
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

/// Resolves the repo file path from arguments.
/// Supports either /repoFile directly or /actor + /dataDir combination.
/// If actor is not cached, resolves online via BlueskyClient.
async fn resolve_repo_file(args: &HashMap<String, String>) -> Option<std::path::PathBuf> {
    // Check for direct repoFile argument
    if let Some(repo_file) = get_arg(args, "repofile") {
        let path = std::path::PathBuf::from(repo_file);
        if path.exists() {
            return Some(path);
        } else {
            logger().error(&format!("Repo file does not exist: {}", repo_file));
            return None;
        }
    }

    // Try to resolve from actor + dataDir
    let actor = get_arg(args, "actor")?;
    let data_dir = get_arg(args, "datadir")?;

    let lfs = match LocalFileSystem::initialize(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            logger().error(&format!("Error initializing data directory: {}", e));
            return None;
        }
    };

    // Resolve actor to DID
    let did = if actor.starts_with("did:") {
        // Already a DID
        actor.to_string()
    } else {
        // Try to resolve handle from cached actor info
        match lfs.resolve_actor_info(actor, None) {
            Ok(info) => {
                match info.did {
                    Some(d) => d,
                    None => {
                        logger().error("Cached actor info does not contain a DID");
                        return None;
                    }
                }
            }
            Err(_) => {
                // Cache miss - resolve online
                logger().trace("Cache miss, resolving actor online...");
                let client = BlueskyClient::new();
                match client.resolve_actor_info(actor, None).await {
                    Ok(info) => {
                        // Save to cache for future use
                        if let Err(e) = lfs.save_actor_info(actor, &info) {
                            logger().warning(&format!("Failed to cache actor info: {}", e));
                        }
                        match info.did {
                            Some(d) => d,
                            None => {
                                logger().error("Resolved actor info does not contain a DID");
                                return None;
                            }
                        }
                    }
                    Err(e) => {
                        logger().error(&format!("Could not resolve actor: {}", e));
                        return None;
                    }
                }
            }
        }
    };
    
    match lfs.get_path_repo_file(&did) {
        Ok(path) => {
            if path.exists() {
                Some(path)
            } else {
                logger().error(&format!("Repo file does not exist: {}", path.display()));
                None
            }
        }
        Err(e) => {
            logger().error(&format!("Error getting repo file path: {}", e));
            None
        }
    }
}

async fn cmd_print_repo_stats(args: &HashMap<String, String>) {
    let log = logger();

    let repo_file = match resolve_repo_file(args).await {
        Some(path) => path,
        None => {
            log.error("Could not resolve repo file. Use /repoFile <path> or /actor <handle> /dataDir <path>");
            return;
        }
    };

    log.info(&format!("Reading repo file: {}", repo_file.display()));

    // Stats tracking
    let mut earliest_date: Option<chrono::NaiveDateTime> = None;
    let mut latest_date: Option<chrono::NaiveDateTime> = None;
    let mut record_type_counts: HashMap<String, usize> = HashMap::new();
    let mut record_counts_by_month: HashMap<String, usize> = HashMap::new();
    let mut record_type_counts_by_month: HashMap<String, HashMap<String, usize>> = HashMap::new();
    let mut error_count = 0;
    let mut total_record_count = 0;

    // Walk the repo
    let result = Repo::walk_repo_file(
        &repo_file,
        |_header| true,
        |record: &RepoRecord| {
            total_record_count += 1;

            if record.is_error {
                error_count += 1;
                log.trace(&format!("ERROR: {}", record.json_string));
                return true;
            }

            let record_type = record.at_proto_type.clone().unwrap_or_else(|| "<null>".to_string());

            // Total counts
            *record_type_counts.entry(record_type.clone()).or_insert(0) += 1;

            // Counts by month
            if let Some(created_at) = &record.created_at {
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(created_at, "%Y-%m-%dT%H:%M:%S%.fZ") {
                    // Convert UTC to local time to match .NET DateTime.TryParse behavior
                    let local_dt = dt.and_utc().with_timezone(&chrono::Local).naive_local();
                    
                    // Update earliest/latest dates
                    match &earliest_date {
                        None => earliest_date = Some(local_dt),
                        Some(e) if local_dt < *e => earliest_date = Some(local_dt),
                        _ => {}
                    }
                    match &latest_date {
                        None => latest_date = Some(local_dt),
                        Some(l) if local_dt > *l => latest_date = Some(local_dt),
                        _ => {}
                    }

                    let month = local_dt.format("%Y-%m").to_string();

                    // Initialize if needed
                    record_counts_by_month.entry(month.clone()).or_insert(0);
                    record_type_counts_by_month.entry(month.clone()).or_insert_with(HashMap::new);

                    *record_counts_by_month.get_mut(&month).unwrap() += 1;
                    *record_type_counts_by_month
                        .get_mut(&month)
                        .unwrap()
                        .entry(record_type)
                        .or_insert(0) += 1;
                }
            }

            true
        },
    );

    if let Err(e) = result {
        log.error(&format!("Error walking repo: {}", e));
        return;
    }

    // Print stats
    log.info("");
    log.info(&format!("repoFile: {}", repo_file.display()));
    log.info("");
    log.info(&format!(
        "earliestDate: {}",
        earliest_date.map(|d| d.to_string()).unwrap_or_else(|| "<none>".to_string())
    ));
    log.info(&format!(
        "latestDate: {}",
        latest_date.map(|d| d.to_string()).unwrap_or_else(|| "<none>".to_string())
    ));
    log.info("");
    log.info(&format!("errorCount: {}", error_count));
    log.info("");
    log.info(&format!("totalRecordCount: {}", total_record_count));
    log.info("");

    // Print monthly breakdown
    if let (Some(start), Some(end)) = (earliest_date, latest_date) {
        let mut current = start;
        while current <= end + chrono::Duration::days(31) {
            let month = current.format("%Y-%m").to_string();

            let record_count = record_counts_by_month.get(&month).copied().unwrap_or(0);
            let type_counts = record_type_counts_by_month.get(&month);

            let post_count = type_counts.and_then(|tc| tc.get(AtProtoType::BLUESKY_POST)).copied().unwrap_or(0);
            let like_count = type_counts.and_then(|tc| tc.get(AtProtoType::BLUESKY_LIKE)).copied().unwrap_or(0);
            let repost_count = type_counts.and_then(|tc| tc.get(AtProtoType::BLUESKY_REPOST)).copied().unwrap_or(0);
            let follow_count = type_counts.and_then(|tc| tc.get(AtProtoType::BLUESKY_FOLLOW)).copied().unwrap_or(0);
            let block_count = type_counts.and_then(|tc| tc.get(AtProtoType::BLUESKY_BLOCK)).copied().unwrap_or(0);

            log.info(&format!(
                "{}: records={}, follows={}, posts={}, likes={}, reposts={}, blocks={}",
                month, record_count, follow_count, post_count, like_count, repost_count, block_count
            ));

            // Move to next month
            let year = current.date().year();
            let month_num = current.date().month();
            if month_num == 12 {
                current = chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
            } else {
                current = chrono::NaiveDate::from_ymd_opt(year, month_num + 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap();
            }
        }
    }

    // Print record type counts, descending
    log.info("");
    log.info("");
    log.info("RECORD TYPE COUNTS:");
    log.info("");

    let mut sorted_types: Vec<_> = record_type_counts.iter().collect();
    sorted_types.sort_by(|a, b| b.1.cmp(a.1));

    for (record_type, count) in sorted_types {
        log.info(&format!("{}: {}", record_type, count));
    }
    log.info("");

    if error_count > 0 {
        log.info(&format!(
            "Note: {} records could not be parsed. Use log level 'trace' to see details.",
            error_count
        ));
        log.info("");
    }
}

async fn cmd_print_repo_records(args: &HashMap<String, String>) {
    let log = logger();

    let repo_file = match resolve_repo_file(args).await {
        Some(path) => path,
        None => {
            log.error("Could not resolve repo file. Use /repoFile <path> or /actor <handle> /dataDir <path>");
            return;
        }
    };

    let collection_filter = get_arg(args, "collection").map(|s| s.to_string());
    let month_filter = get_arg(args, "month").map(|s| s.to_string());

    log.info(&format!("Reading repo file: {}", repo_file.display()));
    if let Some(ref col) = collection_filter {
        log.info(&format!("Filtering by collection: {}", col));
    }
    if let Some(ref month) = month_filter {
        log.info(&format!("Filtering by month: {}", month));
    }

    // Stats tracking
    let mut total_records = 0;
    let mut dag_cbor_type_counts: HashMap<String, usize> = HashMap::new();
    let mut record_type_counts: HashMap<String, usize> = HashMap::new();

    // Walk the repo
    let result = Repo::walk_repo_file(
        &repo_file,
        |header| {
            log.trace("");
            log.trace("REPO HEADER:");
            log.trace(&format!("   roots: {}", header.repo_commit_cid.get_base32()));
            log.trace(&format!("   version: {}", header.version));
            true
        },
        |record: &RepoRecord| {
            let record_type = record.at_proto_type.clone().unwrap_or_else(|| "<null>".to_string());

            // Apply collection filter
            if let Some(ref col) = collection_filter {
                if record.at_proto_type.as_ref() != Some(col) {
                    return true;
                }
            }

            // Apply month filter
            if let Some(ref month) = month_filter {
                if let Some(created_at) = &record.created_at {
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(created_at, "%Y-%m-%dT%H:%M:%S%.fZ") {
                        // Convert UTC to local time to match .NET DateTime.TryParse behavior
                        let local_dt = dt.and_utc().with_timezone(&chrono::Local).naive_local();
                        let record_month = local_dt.format("%Y-%m").to_string();
                        if &record_month != month {
                            return true;
                        }
                    } else {
                        // Skip records without valid CreatedAt when month filter is active
                        return true;
                    }
                } else {
                    return true;
                }
            }

            let record_type_str = record.get_record_type_string();

            log.trace("");
            log.trace(&format!("{}:", record_type_str));
            log.trace(&format!("  cid: {}", record.cid.get_base32()));
            log.trace(&format!("  blockJson:\n {}", record.json_string));

            // For stats
            total_records += 1;
            let type_string = record.data_block.cbor_type.get_major_type_string().to_string();
            *dag_cbor_type_counts.entry(type_string).or_insert(0) += 1;
            *record_type_counts.entry(record_type).or_insert(0) += 1;

            true
        },
    );

    if let Err(e) = result {
        log.error(&format!("Error walking repo: {}", e));
        return;
    }

    // Print stats
    log.info("TOTAL RECORDS:");
    log.info(&format!("   {}", total_records));

    log.trace("DAG CBOR TYPE COUNTS:");
    for (type_name, count) in &dag_cbor_type_counts {
        log.trace(&format!("  {} - {}", type_name, count));
    }

    log.info("RECORD TYPE COUNTS:");
    let mut sorted_types: Vec<_> = record_type_counts.iter().collect();
    sorted_types.sort_by(|a, b| b.1.cmp(a.1));
    for (record_type, count) in sorted_types {
        log.info(&format!("  {} - {}", record_type, count));
    }
}

async fn cmd_start_firehose_consumer(args: &HashMap<String, String>) {
    let log = logger();

    // Get actor argument
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rstproto /command StartFirehoseConsumer /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let cursor = get_arg(args, "cursor");
    let show_dag_cbor_types = get_arg(args, "showdagcbortypes")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let client = BlueskyClient::new();

    // Resolve actor info to get PDS and DID
    log.info(&format!("Resolving actor: {}", actor));
    let actor_info = match client.resolve_actor_info(actor, None).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Failed to resolve actor info: {}", e));
            return;
        }
    };

    let pds = match &actor_info.pds {
        Some(p) => p.clone(),
        None => {
            log.error("Could not resolve PDS for actor");
            return;
        }
    };

    let target_did = match &actor_info.did {
        Some(d) => d.clone(),
        None => {
            log.error("Could not resolve DID for actor");
            return;
        }
    };

    // Build the firehose URL
    let mut url = format!("wss://{}/xrpc/com.atproto.sync.subscribeRepos", pds);
    if let Some(c) = cursor {
        url = format!("{}?cursor={}", url, c);
    }

    log.info(&format!("Connecting to firehose at: {}", url));

    // Listen on firehose
    let result = Firehose::listen(&url, |header, body| {
        // Filter to only our DID
        let did = body.select_string(&["repo"]);
        if did.as_ref() != Some(&target_did) {
            return true; // continue listening
        }

        log.info(" -----------------------------------------------------------------------------------------------------------");
        log.info(" NEW FIREHOSE FRAME");
        log.info(" -----------------------------------------------------------------------------------------------------------");

        log.info(&format!("DAG CBOR OBJECT 1 (HEADER):\n{}", header.to_json_string()));
        log.info(&format!("DAG CBOR OBJECT 2 (MESSAGE):\n{}", body.to_json_string()));

        if show_dag_cbor_types {
            log.trace(&format!("\nDAG CBOR OBJECT 1 TYPES (HEADER):\n{}", header.get_recursive_debug_string(0)));
            log.trace(&format!("\nDAG CBOR OBJECT 2 TYPES (MESSAGE):\n{}", body.get_recursive_debug_string(0)));
        }

        log.info(" PARSING BLOCKS");

        // Look for the "blocks" key in the message
        // "blocks" should be a byte array of records, in repo format
        if let Some(blocks_obj) = body.select_object(&["blocks"]) {
            if let DagCborValue::ByteString(blocks_bytes) = &blocks_obj.value {
                let mut cursor = Cursor::new(blocks_bytes);

                // Walk it like a repo
                let walk_result = Repo::walk_repo(
                    &mut cursor,
                    |repo_header| {
                        log.info("REPO HEADER:");
                        log.info(&format!("   roots: {}", repo_header.repo_commit_cid.get_base32()));
                        log.info(&format!("   version: {}", repo_header.version));
                        true
                    },
                    |repo_record| {
                        log.info(&format!("cid: {}", repo_record.cid.get_base32()));
                        log.info("BLOCK JSON:");
                        log.info(&format!("\n{}", repo_record.json_string));

                        if show_dag_cbor_types {
                            log.trace(&format!("\n{}", repo_record.data_block.get_recursive_debug_string(0)));
                        }

                        true
                    },
                );

                if let Err(e) = walk_result {
                    log.error(&format!("Error walking blocks: {}", e));
                }
            } else {
                log.info("No blocks found in message (blocks is not a byte string).");
            }
        } else {
            log.info("No blocks found in message.");
        }

        true // continue listening
    }).await;

    if let Err(e) = result {
        log.error(&format!("Firehose error: {}", e));
    }
}
