//! rustproto CLI - AT Protocol / Bluesky tools


use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use chrono::Datelike;
use rustproto::fs::LocalFileSystem;
use rustproto::log::{init_logger, logger, FileDestination, LogLevel};
use rustproto::mst::Mst;
use rustproto::pds::{PdsDb};
use rustproto::repo::{CidV1, DagCborObject, DagCborValue, Repo, RepoMst, RepoRecord, AtProtoType, MstNodeKey};
use rustproto::ws::{BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};
use rustproto::cli::get_arg;
use rustproto::cli::hex_encode;
use rustproto::cli::parse_arguments;
use rustproto::cli::repair_commit::cmd_repair_commit;
use rustproto::cli::install_db::cmd_install_db;
use rustproto::cli::install_config::cmd_install_config;
use rustproto::cli::run_pds::cmd_run_pds;
use rustproto::cli::resolve_actor::cmd_resolve_actor;
use rustproto::cli::walk_mst::cmd_walk_mst;
use rustproto::cli::print_db_mst::cmd_print_db_mst;
use rustproto::cli::start_firehose_consumer::cmd_start_firehose_consumer;
use rustproto::cli::inspect_firehose_event::cmd_inspect_firehose_event;


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
            let log_filename = get_arg(&arguments, "logfilename");
            if let Ok(file_dest) = FileDestination::from_data_dir(data_dir, command, log_filename) {
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
        "walkmst" => cmd_walk_mst(&arguments).await,
        "printdbmst" => cmd_print_db_mst(&arguments),
        "startfirehoseconsumer" => cmd_start_firehose_consumer(&arguments).await,
        "installdb" => cmd_install_db(&arguments),
        "installconfig" => cmd_install_config(&arguments),
        "repaircommit" => cmd_repair_commit(&arguments),
        "runpds" => cmd_run_pds(&arguments).await,
        "inspectfirehoseevent" => cmd_inspect_firehose_event(&arguments),
        "getplchistory" => cmd_get_plc_history(&arguments).await,
        "getpdsinfo" => cmd_get_pds_info(&arguments).await,
        "getpost" => cmd_get_post(&arguments).await,
        "syncgetrecordlocal" => cmd_sync_get_record_local(&arguments),
        "syncrepo" => cmd_sync_repo(&arguments),
        "testapplywritesandlogfirehose" => cmd_test_apply_writes_and_log_firehose(&arguments),
        "backupaccount" => cmd_backup_account(&arguments).await,
        "createsession" => cmd_create_session(&arguments).await,
        "help" => print_usage(),
        _ => {
            logger().error(&format!("Unknown command: {}", command));
            print_usage();
        }
    }
}



fn print_usage() {
    println!("rustproto - AT Protocol / Bluesky CLI tools");
    println!();
    println!("Usage: rustproto /command <name> [/arg1 val1 /arg2 val2 ...]");
    println!();
    println!("Commands:");
    println!("  ResolveActorInfo       Resolve actor info (DID, PDS, etc.)");
    println!("  GetRepo                Download repository (CAR file) for an actor");
    println!("  PrintRepoStats         Print statistics about a repository");
    println!("  PrintRepoRecords       Print records from a repository");
    println!("  WalkMst                Walk and print the MST structure of a repository");
    println!("  StartFirehoseConsumer  Listen to a PDS firehose and print events");
    println!("  InspectFirehoseEvent   Inspect a stored firehose event (for debugging)");
    println!("  GetPlcHistory          Get PLC history for an actor and check repo status");
    println!("  GetPdsInfo             Get PDS info (health, description, repos)");
    println!("  GetPost                Get a post and print all URIs found");
    println!("  SyncGetRecordLocal     Get a record from local pds.db and print details");
    println!("  BackupAccount          Backup an account (repo, blobs, prefs) to local directory");
    println!("  CreateSession          Create a session (log in) for an actor");
    println!("  SyncRepo               Sync user repo data from one PDS data dir to another");
    println!("  InstallDb              Create PDS database schema");
    println!("  InstallConfig          Configure PDS server settings");
    println!("  RepairCommit           Re-sign repo commit after migration or format change");
    println!("  RunPds                 Run the PDS HTTP server");
    println!("  Help                   Show this help message");
    println!();
    println!("Arguments:");
    println!("  /command <name>       Command to run");
    println!("  /actor <handle>       Handle or DID to resolve");
    println!("  /uri <at_uri>         AT URI or bsky.app URL (for GetPost)");
    println!("  /all <true|false>     Use all resolution methods");
    println!("  /dataDir <path>       Path to data directory");
    println!("  /repoFile <path>      Path to CAR file (for repo commands)");
    println!("  /collection <type>    Filter by collection type (e.g., app.bsky.feed.post)");
    println!("  /month <yyyy-MM>      Filter by month (e.g., 2024-01)");
    println!("  /cursor <int>         Firehose cursor position");
    println!("  /seq <int>            Sequence number (for InspectFirehoseEvent)");
    println!("  /showDagCborTypes     Show DAG-CBOR type debug info (true/false)");
    println!("  /rkey <string>        Record key (for SyncGetRecordLocal)");
    println!("  /format <type>        Output format: dagcbor (default), json, or raw");
    println!("  /getPrefs <bool>      Backup preferences (true/false, default: true)");
    println!("  /getRepo <bool>       Backup repository (true/false, default: true)");
    println!("  /getBlobs <bool>      Backup blobs (true/false, default: true)");
    println!("  /blobSleepSeconds <n> Seconds to sleep between blob downloads (default: 1)");
    println!("  /password <string>    Password for CreateSession");
    println!("  /authFactorToken <s>  Auth factor token for CreateSession (optional)");
    println!("  /logLevel <level>     Log level: trace, info, warning, error");
    println!("  /logToDataDir <bool>  Write logs to data directory");
    println!("  /deleteExistingDb     Delete existing database before install (true/false)");
    println!("  /listenScheme <str>   Server scheme (http/https)");
    println!("  /listenHost <str>     Server hostname");
    println!("  /listenPort <int>     Server port number");
    println!();
    println!("Examples:");
    println!("  rustproto /command ResolveActorInfo /actor alice.bsky.social");
    println!("  rustproto /command ResolveActorInfo /actor did:plc:abc123 /all true");
    println!("  rustproto /command GetRepo /actor alice.bsky.social /dataDir ./data");
    println!("  rustproto /command PrintRepoStats /repoFile ./data/repos/did_plc_xxx/repo.car");
    println!("  rustproto /command PrintRepoRecords /actor alice.bsky.social /dataDir ./data");
    println!("  rustproto /command PrintRepoRecords /repoFile ./repo.car /collection app.bsky.feed.post");
    println!("  rustproto /command WalkMst /actor alice.bsky.social /dataDir ./data");
    println!("  rustproto /command WalkMst /repoFile ./repo.car");
    println!("  rustproto /command StartFirehoseConsumer /actor alice.bsky.social /dataDir ./data");
    println!("  rustproto /command GetPlcHistory /actor alice.bsky.social");
    println!("  rustproto /command GetPdsInfo /actor alice.bsky.social");
    println!("  rustproto /command GetPost /uri at://did:plc:xxx/app.bsky.feed.post/abc123");
    println!("  rustproto /command InstallDb /dataDir ./data");
    println!("  rustproto /command InstallConfig /dataDir ./data /listenScheme https /listenHost example.com /listenPort 443");
    println!("  rustproto /command RunPds /dataDir ./data");
    println!("  rustproto /command SyncGetRecordLocal /dataDir ./data /collection app.bsky.feed.post /rkey 3abc123");
    println!("  rustproto /command SyncRepo /sourceDataDir ./source-data /destDataDir ./dest-data");
    println!("  rustproto /command BackupAccount /actor alice.bsky.social /dataDir ./data");
    println!("  rustproto /command CreateSession /actor alice.bsky.social /dataDir ./data /password mypass");
}


async fn cmd_get_repo(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command GetRepo /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command GetRepo /actor <handle_or_did> /dataDir <path>");
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

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

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
        // Try to resolve handle from cached actor info (falls back to online)
        match lfs.resolve_actor_info(actor, None, DEFAULT_APP_VIEW_HOST_NAME).await {
            Ok(info) => {
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

/// Validate and normalize a PLC-provided PDS endpoint into a safe hostname.
fn sanitize_pds_host_for_repo_status(endpoint: &str) -> Result<String, String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err("PDS endpoint is empty".to_string());
    }

    // Accept both full URLs and bare hostnames from PLC history.
    let endpoint_url = if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("https://{}", endpoint)
    };

    let parsed = reqwest::Url::parse(&endpoint_url)
        .map_err(|e| format!("Invalid PDS endpoint URL '{}': {}", endpoint, e))?;

    let scheme = parsed.scheme().to_ascii_lowercase();
    if scheme != "https" && scheme != "http" {
        return Err(format!("Unsupported URL scheme '{}'", parsed.scheme()));
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("PDS endpoint must not include user info".to_string());
    }

    // We only accept a bare authority (optional trailing slash) from PLC endpoint.
    let path = parsed.path();
    if path != "/" && !path.is_empty() {
        return Err(format!("PDS endpoint must not include path '{}'.", path));
    }

    if parsed.query().is_some() {
        return Err("PDS endpoint must not include query parameters".to_string());
    }

    if parsed.fragment().is_some() {
        return Err("PDS endpoint must not include a URL fragment".to_string());
    }

    if parsed.port().is_some() {
        return Err("PDS endpoint must not include an explicit port".to_string());
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "PDS endpoint is missing a hostname".to_string())?
        .to_ascii_lowercase();

    if host == "localhost" || host.ends_with(".localhost") {
        return Err("Localhost PDS endpoints are not allowed".to_string());
    }

    if host.parse::<std::net::IpAddr>().is_ok() {
        return Err("IP address PDS endpoints are not allowed".to_string());
    }

    if !is_valid_dns_hostname(&host) {
        return Err(format!("Invalid DNS hostname '{}'.", host));
    }

    Ok(host)
}

fn is_valid_dns_hostname(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }

    let labels: Vec<&str> = host.split('.').collect();
    if labels.iter().any(|label| label.is_empty() || label.len() > 63) {
        return false;
    }

    labels.iter().all(|label| {
        let bytes = label.as_bytes();

        let first = bytes.first().copied().unwrap_or_default();
        let last = bytes.last().copied().unwrap_or_default();
        if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
            return false;
        }

        bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
    })
}

/// Gets PLC history for an actor and checks repo status on each PDS.
async fn cmd_get_plc_history(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command GetPlcHistory /actor <handle_or_did>");
            return;
        }
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    // Resolve actor to get DID
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

    if did.starts_with("did:web") {
        log.error(&format!("'{}' is a did:web and does not contain plc info.", did));
        return;
    }

    // Get PLC history
    let history = match client.get_plc_history(&did).await {
        Ok(h) => h,
        Err(e) => {
            log.error(&format!("Error getting PLC history: {}", e));
            return;
        }
    };

    // Track PDS status
    let mut pds_status: HashMap<String, String> = HashMap::new();
    pds_status.insert("bsky.social".to_string(), "<na>".to_string());

    let mut console_output: Vec<String> = Vec::new();

    if let Some(entries) = history.as_array() {
        for entry in entries {
            let pds = entry["operation"]["services"]["atproto_pds"]["endpoint"]
                .as_str()
                .map(|s| s.to_string());
            let created_at = entry["createdAt"].as_str();
            let also_known_as = entry["operation"]["alsoKnownAs"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str());

            if let Some(pds_url) = &pds {
                let pds_host = match sanitize_pds_host_for_repo_status(pds_url) {
                    Ok(host) => host,
                    Err(e) => {
                        console_output.push(format!(
                            "{}  pds: {}, handle: {}, active: <invalid-endpoint>, reason: {}",
                            created_at.unwrap_or("<unknown>"),
                            pds_url,
                            also_known_as.unwrap_or("<unknown>"),
                            e
                        ));
                        continue;
                    }
                };

                let repo_status_url = format!(
                    "https://{}/xrpc/com.atproto.sync.getRepoStatus?did={}",
                    pds_host, did
                );

                let active = if !pds_status.contains_key(&pds_host) {
                    log.info(&format!("Repo status URL: {}", repo_status_url));
                    match client.get_repo_status(&pds_host, &did).await {
                        Ok(status) => {
                            let active_val = status["active"]
                                .as_bool()
                                .map(|b| b.to_string())
                                .unwrap_or_else(|| "<null>".to_string());
                            pds_status.insert(pds_host.clone(), active_val.clone());
                            active_val
                        }
                        Err(_) => {
                            pds_status.insert(pds_host.clone(), "<exception>".to_string());
                            "<exception>".to_string()
                        }
                    }
                } else {
                    pds_status.get(&pds_host).cloned().unwrap_or_default()
                };

                console_output.push(format!(
                    "{}  pds: {}, handle: {}, active: {}",
                    created_at.unwrap_or("<unknown>"),
                    pds_url,
                    also_known_as.unwrap_or("<unknown>"),
                    active
                ));
            }
        }
    }

    // Print results
    log.info("");
    log.info(&format!("PDS History for {}:", did));
    for line in &console_output {
        log.info(line);
    }
    log.info("");

    // Check if account is active on multiple PDSs
    let active_pds_count = pds_status.values().filter(|s| s.eq_ignore_ascii_case("true")).count();
    if active_pds_count > 1 {
        log.error(&format!(
            "Account is active on {} PDSs. Expected at most 1.",
            active_pds_count
        ));
        log.info("");
    }
}

/// Gets PDS info including health, description, and repo list.
async fn cmd_get_pds_info(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command GetPdsInfo /actor <handle_or_did>");
            return;
        }
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    // Resolve actor to get PDS
    let info = match client.resolve_actor_info(actor, None).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Error resolving actor: {}", e));
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

    log.info(&format!("PDS: {}", pds));

    // Health
    log.info("");
    log.info("HEALTH");
    match client.pds_health(&pds).await {
        Ok(health) => {
            log.info(&serde_json::to_string_pretty(&health).unwrap_or_default());
        }
        Err(e) => {
            log.error(&format!("Error getting health: {}", e));
        }
    }

    // Describe Server
    log.info("");
    log.info("DESCRIBE SERVER");
    match client.pds_describe_server(&pds).await {
        Ok(desc) => {
            log.info(&serde_json::to_string_pretty(&desc).unwrap_or_default());
        }
        Err(e) => {
            log.error(&format!("Error describing server: {}", e));
        }
    }

    // List Repos
    log.info("");
    log.info("LIST REPOS");
    match client.list_repos(&pds, 100).await {
        Ok(repos) => {
            log.info(&format!("repo count: {}", repos.len()));
            for repo in repos {
                log.info(&serde_json::to_string_pretty(&repo).unwrap_or_default());
            }
        }
        Err(e) => {
            log.error(&format!("Error listing repos: {}", e));
        }
    }
}

/// Gets a post and prints all URIs found in the response.
async fn cmd_get_post(args: &HashMap<String, String>) {
    let log = logger();

    let uri = match get_arg(args, "uri") {
        Some(u) => u,
        None => {
            log.error("missing /uri argument");
            log.error("Usage: rustproto /command GetPost /uri <at_uri_or_bsky_url>");
            return;
        }
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    // Parse URI - could be AT URI or bsky.app URL
    let at_uri = parse_to_at_uri(uri, &client).await;

    let at_uri = match at_uri {
        Some(u) => u,
        None => {
            log.error("Invalid URI format");
            return;
        }
    };

    log.trace(&format!("AT URI: {}", at_uri));

    // Get posts
    match client.get_posts(&[&at_uri]).await {
        Ok(response) => {
            log.trace(&serde_json::to_string_pretty(&response).unwrap_or_default());

            log.info("All URIs found in response:");
            log.info("");
            find_and_print_uris(&response, "", &log);
        }
        Err(e) => {
            log.error(&format!("Error getting post: {}", e));
        }
    }
}

/// Parse a bsky.app URL or AT URI to an AT URI.
async fn parse_to_at_uri(input: &str, client: &BlueskyClient) -> Option<String> {
    // If already an AT URI
    if input.starts_with("at://") {
        return Some(input.to_string());
    }

    // Try to parse as bsky.app URL
    // Format: https://bsky.app/profile/{handle}/post/{rkey}
    if input.contains("bsky.app/profile/") && input.contains("/post/") {
        let parts: Vec<&str> = input.split('/').collect();
        
        // Find profile and post indices
        let profile_idx = parts.iter().position(|&p| p == "profile")?;
        let post_idx = parts.iter().position(|&p| p == "post")?;
        
        if profile_idx + 1 < parts.len() && post_idx + 1 < parts.len() {
            let handle_or_did = parts[profile_idx + 1];
            let rkey = parts[post_idx + 1];
            
            // Resolve handle to DID if needed
            let did = if handle_or_did.starts_with("did:") {
                handle_or_did.to_string()
            } else {
                match client.resolve_actor_info(handle_or_did, None).await {
                    Ok(info) => info.did?,
                    Err(_) => return None,
                }
            };
            
            return Some(format!("at://{}/app.bsky.feed.post/{}", did, rkey));
        }
    }

    None
}

/// Recursively find and print all URIs in a JSON value.
fn find_and_print_uris(value: &serde_json::Value, path: &str, log: &rustproto::log::Logger) {
    match value {
        serde_json::Value::Object(obj) => {
            for (key, val) in obj {
                let current_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };

                if key.eq_ignore_ascii_case("uri") {
                    if let Some(uri_str) = val.as_str() {
                        if let Some(url) = at_uri_to_bsky_url(uri_str) {
                            log.info(&current_path);
                            log.info(&url);
                            log.info("");
                        }
                    }
                }

                find_and_print_uris(val, &current_path, log);
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                let current_path = format!("{}[{}]", path, i);
                find_and_print_uris(val, &current_path, log);
            }
        }
        _ => {}
    }
}

/// Convert an AT URI to a bsky.app URL.
fn at_uri_to_bsky_url(at_uri: &str) -> Option<String> {
    // Format: at://did:plc:xxx/app.bsky.feed.post/rkey
    if !at_uri.starts_with("at://") {
        return None;
    }

    let rest = at_uri.strip_prefix("at://")?;
    let parts: Vec<&str> = rest.split('/').collect();

    if parts.len() >= 3 && parts[1] == "app.bsky.feed.post" {
        let did = parts[0];
        let rkey = parts[2];
        Some(format!("https://bsky.app/profile/{}/post/{}", did, rkey))
    } else {
        None
    }
}


/// Get a record directly from the local pds.db and print its details.
fn cmd_sync_get_record_local(args: &HashMap<String, String>) {
    use rustproto::mst::MstItem;
    use rustproto::pds::db::PdsDb;

    let log = logger();

    // Get required arguments
    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command SyncGetRecordLocal /dataDir <path> /collection <nsid> /rkey <rkey> [/format dagcbor|json|raw]");
            return;
        }
    };

    let collection = match get_arg(args, "collection") {
        Some(c) => c,
        None => {
            log.error("missing /collection argument");
            log.error("Usage: rustproto /command SyncGetRecordLocal /dataDir <path> /collection <nsid> /rkey <rkey> [/format dagcbor|json|raw]");
            return;
        }
    };

    let rkey = match get_arg(args, "rkey") {
        Some(r) => r,
        None => {
            log.error("missing /rkey argument");
            log.error("Usage: rustproto /command SyncGetRecordLocal /dataDir <path> /collection <nsid> /rkey <rkey> [/format dagcbor|json|raw]");
            return;
        }
    };

    let format = get_arg(args, "format").unwrap_or("dagcbor");
    let full_key = format!("{}/{}", collection, rkey);

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

    // Get repo header and commit
    let repo_header = match db.get_repo_header() {
        Ok(h) => h,
        Err(e) => {
            log.error(&format!("Failed to get repo header: {}", e));
            return;
        }
    };

    let repo_commit = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit: {}", e));
            return;
        }
    };

    // Get record
    let record = match db.get_repo_record(collection, rkey) {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("Record not found: {}/{}", collection, rkey));
            log.trace(&format!("Error: {}", e));
            return;
        }
    };

    // Build MST from all records
    let all_records = match db.get_all_repo_records() {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("Failed to get all records: {}", e));
            return;
        }
    };

    let mst_items: Vec<MstItem> = all_records
        .iter()
        .map(|r| MstItem::new(&format!("{}/{}", r.collection, r.rkey), &r.cid))
        .collect();

    let mst = Mst::assemble_tree_from_items(&mst_items);

    // Find nodes on the path to the record (proof chain)
    let proof_nodes = mst.find_nodes_for_key(&full_key);

    // Convert ALL MST nodes to DAG-CBOR first (so CIDs are computed correctly)
    let mst_cache = match RepoMst::convert_mst_to_dag_cbor(&mst) {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to convert MST: {}", e));
            return;
        }
    };

    // Get user DID for AT URI
    let user_did = db.get_config_property("UserDid").unwrap_or_else(|_| "<unknown>".to_string());

    // Parse record DAG-CBOR
    let record_dag_cbor = match DagCborObject::from_bytes(&record.dag_cbor_bytes) {
        Ok(obj) => obj,
        Err(e) => {
            log.error(&format!("Failed to parse record DAG-CBOR: {}", e));
            return;
        }
    };

    let at_proto_type = record_dag_cbor.select_string(&["$type"]).unwrap_or_else(|| "<null>".to_string());

    // Print based on format
    match format.to_lowercase().as_str() {
        "dagcbor" => {
            log.info("");
            log.info("=== SYNC GET RECORD (DAG-CBOR FORMAT) ===");
            log.info(&format!("AT URI: at://{}/{}/{}", user_did, collection, rkey));
            log.info("");

            // CAR Header
            log.info("--- BLOCK 1: CAR HEADER ---");
            let mut header_map: std::collections::HashMap<String, DagCborObject> = std::collections::HashMap::new();
            header_map.insert("version".to_string(), DagCborObject::new_unsigned_int(1));
            if let Ok(root_cid) = CidV1::from_base32(&repo_header.repo_commit_cid) {
                header_map.insert("roots".to_string(), DagCborObject::new_array(vec![
                    DagCborObject::new_cid(root_cid),
                ]));
            }
            let header_dag_cbor = DagCborObject::new_map(header_map);
            if let Ok(header_bytes) = header_dag_cbor.to_bytes() {
                log.info(&format!("CID:    {} (root reference)", repo_header.repo_commit_cid));
                log.info(&format!("Length: {} bytes", header_bytes.len()));
                log.info(&format!("Hex:    {}", hex_encode(&header_bytes)));
            }
            log.info("");

            // Repo Commit
            log.info("--- BLOCK 2: REPO COMMIT ---");
            if let Ok(commit_dag_cbor) = build_commit_dag_cbor_local(&db, &repo_commit) {
                if let Ok(commit_bytes) = commit_dag_cbor.to_bytes() {
                    log.info(&format!("CID:    {}", repo_commit.cid));
                    log.info(&format!("Length: {} bytes", commit_bytes.len()));
                    log.info(&format!("Hex:    {}", hex_encode(&commit_bytes)));
                }
            }
            log.info("");

            // MST Nodes (proof chain)
            let block_start = 3;
            log.info(&format!("--- BLOCKS {}-{}: MST NODES (PROOF CHAIN) ---", block_start, block_start + proof_nodes.len() - 1));
            log.info(&format!("Total MST nodes in proof chain: {}", proof_nodes.len()));
            log.info("");

            let mut block_num = block_start;
            for node in &proof_nodes {
                let node_key = MstNodeKey::from_node(node);
                if let Some((cid, dag_cbor)) = mst_cache.get(&node_key) {
                    if let Ok(node_bytes) = dag_cbor.to_bytes() {
                        log.info(&format!("  BLOCK {}: MST NODE", block_num));
                        log.info(&format!("  CID:    {}", cid.base32));
                        log.info(&format!("  Length: {} bytes", node_bytes.len()));
                        log.info(&format!("  Hex:    {}", hex_encode(&node_bytes)));
                        log.info("");
                    }
                }
                block_num += 1;
            }

            // Record
            log.info(&format!("--- BLOCK {}: RECORD ---", block_num));
            log.info(&format!("CID:    {}", record.cid));
            log.info(&format!("$type:  {}", at_proto_type));
            log.info(&format!("Length: {} bytes", record.dag_cbor_bytes.len()));
            log.info(&format!("Hex:    {}", hex_encode(&record.dag_cbor_bytes)));
        }
        "json" => {
            log.info("");
            log.info("=== SYNC GET RECORD (JSON FORMAT) ===");
            log.info(&format!("AT URI: at://{}/{}/{}", user_did, collection, rkey));
            log.info("");

            // CAR Header
            log.info("--- CAR HEADER ---");
            let mut header_map: std::collections::HashMap<String, DagCborObject> = std::collections::HashMap::new();
            header_map.insert("version".to_string(), DagCborObject::new_unsigned_int(1));
            if let Ok(root_cid) = CidV1::from_base32(&repo_header.repo_commit_cid) {
                header_map.insert("roots".to_string(), DagCborObject::new_array(vec![
                    DagCborObject::new_cid(root_cid),
                ]));
            }
            let header_dag_cbor = DagCborObject::new_map(header_map);
            log.info(&header_dag_cbor.to_json_string());
            log.info("");

            // Repo Commit
            log.info("--- REPO COMMIT ---");
            if let Ok(commit_dag_cbor) = build_commit_dag_cbor_local(&db, &repo_commit) {
                log.info(&commit_dag_cbor.to_json_string());
            }
            log.info("");

            // MST Nodes
            log.info(&format!("--- MST NODES (PROOF CHAIN: {} nodes) ---", proof_nodes.len()));
            let mut node_num = 1;
            for node in &proof_nodes {
                let node_key = MstNodeKey::from_node(node);
                if let Some((cid, dag_cbor)) = mst_cache.get(&node_key) {
                    log.info(&format!("MST NODE {} (CID: {}):", node_num, cid.base32));
                    log.info(&dag_cbor.to_json_string());
                    log.info("");
                }
                node_num += 1;
            }

            // Record
            log.info("--- RECORD ---");
            log.info(&format!("CID:   {}", record.cid));
            log.info(&format!("$type: {}", at_proto_type));
            log.info(&record_dag_cbor.to_json_string());
        }
        "raw" => {
            log.info("");
            log.info("=== SYNC GET RECORD (RAW FORMAT) ===");
            log.info(&format!("AT URI: at://{}/{}/{}", user_did, collection, rkey));
            log.info("");

            log.info(&format!("Record CID:        {}", record.cid));
            log.info(&format!("$type:             {}", at_proto_type));
            log.info(&format!("Commit CID:        {}", repo_commit.cid));
            log.info(&format!("Root MST Node CID: {}", repo_commit.root_mst_node_cid));
            log.info(&format!("MST Proof Chain:   {} nodes", proof_nodes.len()));
            log.info("");

            log.info(&format!("Record Length: {} bytes", record.dag_cbor_bytes.len()));
            log.info(&format!("Record Hex:    {}", hex_encode(&record.dag_cbor_bytes)));
        }
        "tree" => {
            log.info("");
            log.info("=== SYNC GET RECORD (TREE FORMAT) ===");
            log.info(&format!("AT URI: at://{}/{}/{}", user_did, collection, rkey));
            log.info("");

            // Repo Commit
            log.info("--- REPO COMMIT ---");
            log.info(&format!("CID:              {}", repo_commit.cid));
            log.info(&format!("Root MST Node:    {}", repo_commit.root_mst_node_cid));
            log.info(&format!("Rev:              {}", repo_commit.rev));
            log.info(&format!("Version:          {}", repo_commit.version));
            log.info("");

            // MST Proof Chain
            log.info("--- MST PROOF CHAIN ---");
            log.info(&format!("Total nodes in proof: {}", proof_nodes.len()));
            log.info("");

            for (node_idx, node) in proof_nodes.iter().enumerate() {
                let node_key = MstNodeKey::from_node(node);
                if let Some((cid, dag_cbor)) = mst_cache.get(&node_key) {
                    if let Ok(node_bytes) = dag_cbor.to_bytes() {
                        log.info(&format!("NODE {} (depth={})", node_idx, node.key_depth));
                        log.info(&format!("  CID: {}", cid.base32));
                        log.info(&format!("  Hex: {}", hex_encode(&node_bytes)));
                        log.info("  DAG-CBOR:");
                        log.info(&dag_cbor.get_recursive_debug_string(2));
                        log.info("");
                    }
                } else {
                    log.info(&format!("[NODE {} NOT IN CACHE]", node_idx));
                }
            }

            // Target Record
            log.info("--- TARGET RECORD ---");
            log.info(&format!("Key:   {}/{}", collection, rkey));
            log.info(&format!("CID:   {}", record.cid));
            log.info(&format!("$type: {}", at_proto_type));
            log.info(&format!("Hex:   {}", hex_encode(&record.dag_cbor_bytes)));
        }
        _ => {
            log.error(&format!("Unknown format: {}. Use 'dagcbor', 'json', 'raw', or 'tree'.", format));
        }
    }
}

fn cmd_test_apply_writes_and_log_firehose(args: &HashMap<String, String>) {
    let log = logger();
    log.info("TestApplyWritesAndLogFirehose command started");

    let data_dir = match get_arg(args, "dataDir") {
        Some(d) => d.to_string(),
        None => {
            log.error("Missing required argument: /dataDir");
            return;
        }
    };

    let text = get_arg(args, "text")
        .unwrap_or("Hello from TestApplyWritesAndLogFirehose")
        .to_string();

    // Open PDS database
    let lfs = match rustproto::fs::LocalFileSystem::initialize(&data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };
    let db = match rustproto::pds::PdsDb::connect(&lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to open database: {}", e));
            return;
        }
    };

    // Get sequence number before ApplyWrites
    let seq_before = match db.get_most_recently_used_sequence_number() {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to get sequence number: {}", e));
            return;
        }
    };
    log.info(&format!("Sequence number before ApplyWrites: {}", seq_before));

    // Get repo commit before ApplyWrites
    let commit_before = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit before ApplyWrites: {}", e));
            return;
        }
    };

    log.info("");
    log.info("=== REPO COMMIT BEFORE APPLYWRITES ===");
    print_repo_commit_details(log, &db, &commit_before);

    // Build ApplyWrites operation - create a post
    // Hardcoded rkey and createdAt for cross-implementation comparison
    let rkey = "3testapplywriteskey".to_string();
    let collection = "app.bsky.feed.post";

    let created_at = "2026-01-01T00:00:00.000Z".to_string();

    let mut record_map: HashMap<String, DagCborObject> = HashMap::new();
    record_map.insert("text".to_string(), DagCborObject::new_text(text.clone()));
    record_map.insert("createdAt".to_string(), DagCborObject::new_text(created_at));
    let record = DagCborObject::new_map(record_map);

    log.info("");
    log.info("=== APPLYWRITES INPUT ===");
    log.info(&format!("Collection: {}", collection));
    log.info(&format!("Rkey:       {}", rkey));
    log.info(&format!("Text:       {}", text));

    // Serialize the record to DAG-CBOR and print hex + debug
    log.info("");
    log.info("=== RECORD DAG-CBOR (before ApplyWrites) ===");
    match record.to_bytes() {
        Ok(record_bytes) => {
            log.info(&format!("Record DAG-CBOR hex ({} bytes):", record_bytes.len()));
            log.info(&hex_encode(&record_bytes));
            log.info("Record DAG-CBOR debug:");
            log.info(&record.get_recursive_debug_string(0));
        }
        Err(e) => {
            log.error(&format!("Failed to serialize record: {}", e));
        }
    }

    // Call ApplyWrites
    let user_repo = match rustproto::pds::UserRepo::new(&db) {
        Ok(ur) => ur,
        Err(e) => {
            log.error(&format!("Failed to create UserRepo: {}", e));
            return;
        }
    };

    let operation = rustproto::pds::ApplyWritesOperation {
        op_type: rustproto::pds::user_repo::write_type::CREATE.to_string(),
        collection: collection.to_string(),
        rkey: rkey.clone(),
        record: Some(record),
    };

    let results = match user_repo.apply_writes(
        vec![operation],
        "127.0.0.1",
        "TestApplyWritesAndLogFirehose",
    ) {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("ApplyWrites failed: {}", e));
            return;
        }
    };

    // Print ApplyWrites results
    log.info("");
    log.info("=== APPLYWRITES RESULTS ===");
    for result in &results {
        log.info(&format!("Type:             {}", result.result_type));
        log.info(&format!("Uri:              {}", result.uri.as_deref().unwrap_or("<null>")));
        log.info(&format!("Cid:              {}", result.cid.as_ref().map(|c| c.base32.as_str()).unwrap_or("<null>")));
        log.info(&format!("ValidationStatus: {}", result.validation_status.as_deref().unwrap_or("<null>")));
    }

    // Get repo commit after ApplyWrites
    let commit_after = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit after ApplyWrites: {}", e));
            return;
        }
    };

    log.info("");
    log.info("=== REPO COMMIT AFTER APPLYWRITES ===");
    print_repo_commit_details(log, &db, &commit_after);

    // Print the commit as DAG-CBOR
    log.info("");
    log.info("=== COMMIT DAG-CBOR ===");
    match build_commit_dag_cbor_local(&db, &commit_after) {
        Ok(commit_dag_cbor) => {
            match commit_dag_cbor.to_bytes() {
                Ok(commit_bytes) => {
                    log.info(&format!("Commit DAG-CBOR hex ({} bytes):", commit_bytes.len()));
                    log.info(&hex_encode(&commit_bytes));
                }
                Err(e) => log.error(&format!("Failed to serialize commit: {}", e)),
            }
            log.info("Commit DAG-CBOR debug:");
            log.info(&commit_dag_cbor.get_recursive_debug_string(0));
            log.info("Commit JSON:");
            log.info(&commit_dag_cbor.to_json_string());
        }
        Err(e) => log.error(&format!("Failed to build commit DAG-CBOR: {}", e)),
    }

    // Get the new firehose event
    let seq_after = match db.get_most_recently_used_sequence_number() {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to get sequence number after: {}", e));
            return;
        }
    };
    log.info("");
    log.info(&format!("Sequence number after ApplyWrites: {}", seq_after));

    for seq in (seq_before + 1)..=seq_after {
        let event = match db.get_firehose_event(seq) {
            Ok(e) => e,
            Err(_) => {
                log.info(&format!("No firehose event at sequence {}.", seq));
                continue;
            }
        };

        log.info("");
        log.info(&format!("=== FIREHOSE EVENT {} ===", seq));
        log.info(&format!("Created:          {}", event.created_date));
        log.info(&format!("Header op:        {}", event.header_op));
        log.info(&format!("Header t:         {:?}", event.header_t));

        // Header DAG-CBOR
        log.info("");
        log.info("=== FIREHOSE HEADER DAG-CBOR ===");
        log.info(&format!("Header DAG-CBOR hex ({} bytes):", event.header_dag_cbor_bytes.len()));
        log.info(&hex_encode(&event.header_dag_cbor_bytes));
        let mut header_cursor = Cursor::new(&event.header_dag_cbor_bytes);
        match DagCborObject::read_from_stream(&mut header_cursor) {
            Ok(header_obj) => {
                log.info("Header JSON:");
                log.info(&header_obj.to_json_string());
                log.info("Header DAG-CBOR debug:");
                log.info(&header_obj.get_recursive_debug_string(0));
            }
            Err(e) => {
                log.error(&format!("Failed to parse header DAG-CBOR: {}", e));
            }
        }

        // Body DAG-CBOR
        log.info("");
        log.info("=== FIREHOSE BODY DAG-CBOR ===");
        log.info(&format!("Body DAG-CBOR hex ({} bytes):", event.body_dag_cbor_bytes.len()));
        log.info(&hex_encode(&event.body_dag_cbor_bytes));
        let mut body_cursor = Cursor::new(&event.body_dag_cbor_bytes);
        match DagCborObject::read_from_stream(&mut body_cursor) {
            Ok(body_obj) => {
                log.info("Body JSON:");
                log.info(&body_obj.to_json_string());
                log.info("Body DAG-CBOR debug:");
                log.info(&body_obj.get_recursive_debug_string(0));

                // Walk blocks inside the firehose body
                if let Some(blocks_obj) = body_obj.select_object(&["blocks"]) {
                    if let DagCborValue::ByteString(blocks_bytes) = &blocks_obj.value {
                        log.info("");
                        log.info(&format!("=== FIREHOSE BLOCKS ({} bytes) ===", blocks_bytes.len()));

                        let mut blocks_cursor = Cursor::new(blocks_bytes);
                        let walk_result = rustproto::repo::Repo::walk_repo(
                            &mut blocks_cursor,
                            |repo_header| {
                                log.info("CAR HEADER:");
                                log.info(&format!("   roots:   {}", repo_header.repo_commit_cid.get_base32()));
                                log.info(&format!("   version: {}", repo_header.version));
                                true
                            },
                            |repo_record| {
                                log.info("");
                                log.info(&format!("BLOCK CID: {}", repo_record.cid.get_base32()));

                                match repo_record.data_block.to_bytes() {
                                    Ok(block_bytes) => {
                                        log.info(&format!("BLOCK DAG-CBOR hex ({} bytes):", block_bytes.len()));
                                        log.info(&hex_encode(&block_bytes));
                                    }
                                    Err(e) => log.error(&format!("Failed to serialize block: {}", e)),
                                }

                                log.info("BLOCK JSON:");
                                log.info(&repo_record.json_string);
                                log.info("BLOCK DAG-CBOR debug:");
                                log.info(&repo_record.data_block.get_recursive_debug_string(0));
                                true
                            },
                        );

                        if let Err(e) = walk_result {
                            log.error(&format!("Error walking blocks: {}", e));
                        }
                    } else {
                        log.info("blocks field is not a byte string");
                    }
                }
            }
            Err(e) => {
                log.error(&format!("Failed to parse body DAG-CBOR: {}", e));
            }
        }
    }

    log.info("");
    log.info("=== DONE ===");
}

fn print_repo_commit_details(
    log: &rustproto::log::Logger,
    db: &rustproto::pds::db::PdsDb,
    commit: &rustproto::pds::db::DbRepoCommit,
) {
    log.info(&format!("Commit CID:        {}", commit.cid));
    log.info(&format!("Root MST Node CID: {}", commit.root_mst_node_cid));
    log.info(&format!("Rev:               {}", commit.rev));
    log.info(&format!("Version:           {}", commit.version));
    match db.get_config_property("UserDid") {
        Ok(did) => log.info(&format!("Did:               {}", did)),
        Err(_) => log.info("Did:               <unknown>"),
    }
    log.info(&format!("Prev MST Node CID: {}", commit.prev_mst_node_cid.as_deref().unwrap_or("<null>")));
    log.info(&format!("Signature hex ({} bytes):", commit.signature.len()));
    log.info(&hex_encode(&commit.signature));
}

/// Build commit DAG-CBOR object for local display.
fn build_commit_dag_cbor_local(db: &rustproto::pds::db::PdsDb, commit: &rustproto::pds::db::DbRepoCommit) -> Result<DagCborObject, String> {
    let user_did = db.get_config_property("UserDid")
        .map_err(|e| format!("Failed to get UserDid: {}", e))?;

    let root_cid = CidV1::from_base32(&commit.root_mst_node_cid)
        .map_err(|e| format!("Invalid root CID: {}", e))?;

    let mut commit_map: std::collections::HashMap<String, DagCborObject> = std::collections::HashMap::new();
    commit_map.insert("did".to_string(), DagCborObject::new_text(user_did));
    commit_map.insert("version".to_string(), DagCborObject::new_unsigned_int(commit.version as i64));
    commit_map.insert("data".to_string(), DagCborObject::new_cid(root_cid));
    commit_map.insert("rev".to_string(), DagCborObject::new_text(commit.rev.clone()));

    if let Some(ref prev_cid_str) = commit.prev_mst_node_cid {
        if let Ok(prev_cid) = CidV1::from_base32(prev_cid_str) {
            commit_map.insert("prev".to_string(), DagCborObject::new_cid(prev_cid));
        } else {
            commit_map.insert("prev".to_string(), DagCborObject::new_null());
        }
    } else {
        commit_map.insert("prev".to_string(), DagCborObject::new_null());
    }

    commit_map.insert("sig".to_string(), DagCborObject::new_byte_string(commit.signature.clone()));

    Ok(DagCborObject::new_map(commit_map))
}

fn cmd_sync_repo(args: &HashMap<String, String>) {
    let log = logger();
    log.info("SyncRepo command started");

    let source_data_dir = match get_arg(args, "sourcedatadir") {
        Some(d) => d,
        None => {
            log.error("missing /sourceDataDir argument");
            log.error("Usage: rustproto /command SyncRepo /sourceDataDir <path> /destDataDir <path>");
            return;
        }
    };

    let dest_data_dir = match get_arg(args, "destdatadir") {
        Some(d) => d,
        None => {
            log.error("missing /destDataDir argument");
            log.error("Usage: rustproto /command SyncRepo /sourceDataDir <path> /destDataDir <path>");
            return;
        }
    };

    // Initialize file systems
    let source_lfs = match LocalFileSystem::initialize(source_data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize source file system: {}", e));
            return;
        }
    };

    let dest_lfs = match LocalFileSystem::initialize(dest_data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize dest file system: {}", e));
            return;
        }
    };

    // Connect to databases
    let source_db = match PdsDb::connect(&source_lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to connect to source database: {}", e));
            return;
        }
    };

    let dest_db = match PdsDb::connect(&dest_lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to connect to dest database: {}", e));
            return;
        }
    };

    // =========================================================================
    // SYNC REPO HEADER
    // =========================================================================
    log.info("");
    log.info("=== SYNC REPO HEADER ===");
    let source_header = match source_db.get_repo_header() {
        Ok(h) => h,
        Err(e) => {
            log.error(&format!("Failed to get source repo header: {}", e));
            return;
        }
    };
    log.info(&format!("Source RepoHeader: commitCid={} version={}", source_header.repo_commit_cid, source_header.version));

    if let Err(e) = dest_db.delete_repo_header() {
        log.error(&format!("Failed to delete dest repo header: {}", e));
        return;
    }
    if let Err(e) = dest_db.insert_update_repo_header(&source_header) {
        log.error(&format!("Failed to insert dest repo header: {}", e));
        return;
    }
    log.info("RepoHeader synced.");

    // =========================================================================
    // SYNC REPO COMMIT
    // =========================================================================
    log.info("");
    log.info("=== SYNC REPO COMMIT ===");
    let source_commit = match source_db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get source repo commit: {}", e));
            return;
        }
    };
    log.info(&format!("Source RepoCommit: cid={} rev={} rootMst={}", source_commit.cid, source_commit.rev, source_commit.root_mst_node_cid));

    if let Err(e) = dest_db.delete_repo_commit() {
        log.error(&format!("Failed to delete dest repo commit: {}", e));
        return;
    }
    if let Err(e) = dest_db.insert_update_repo_commit(&source_commit) {
        log.error(&format!("Failed to insert dest repo commit: {}", e));
        return;
    }
    log.info("RepoCommit synced.");

    // =========================================================================
    // SYNC REPO RECORDS
    // =========================================================================
    log.info("");
    log.info("=== SYNC REPO RECORDS ===");
    let source_records = match source_db.get_all_repo_records() {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("Failed to get source repo records: {}", e));
            return;
        }
    };
    log.info(&format!("Source records: {}", source_records.len()));

    // Delete all existing dest records
    if let Err(e) = dest_db.delete_all_repo_records() {
        log.error(&format!("Failed to delete dest repo records: {}", e));
        return;
    }

    // Insert source records into dest
    let mut records_synced = 0;
    for record in &source_records {
        if let Err(e) = dest_db.insert_repo_record(
            &record.collection,
            &record.rkey,
            &record.cid,
            &record.dag_cbor_bytes,
        ) {
            log.error(&format!("Failed to insert record {}/{}: {}", record.collection, record.rkey, e));
            return;
        }
        records_synced += 1;
    }
    log.info(&format!("RepoRecords synced: {}", records_synced));

    // =========================================================================
    // SYNC BLOBS (database metadata)
    // =========================================================================
    log.info("");
    log.info("=== SYNC BLOBS ===");
    let source_blobs = match source_db.get_all_blobs() {
        Ok(b) => b,
        Err(e) => {
            log.error(&format!("Failed to get source blobs: {}", e));
            return;
        }
    };
    log.info(&format!("Source blobs: {}", source_blobs.len()));

    // Delete all existing dest blob metadata
    if let Err(e) = dest_db.delete_all_blobs() {
        log.error(&format!("Failed to delete dest blobs: {}", e));
        return;
    }

    // Insert source blob metadata into dest
    let mut blobs_synced = 0;
    for blob in &source_blobs {
        if let Err(e) = dest_db.insert_blob(blob) {
            log.error(&format!("Failed to insert blob {}: {}", blob.cid, e));
            return;
        }
        blobs_synced += 1;
    }
    log.info(&format!("Blob metadata synced: {}", blobs_synced));

    // =========================================================================
    // SYNC BLOB FILES (on disk)
    // =========================================================================
    log.info("");
    log.info("=== SYNC BLOB FILES ===");
    let source_blob_db = rustproto::pds::blob_db::BlobDb::new(&source_lfs, log);
    let dest_blob_db = rustproto::pds::blob_db::BlobDb::new(&dest_lfs, log);

    let mut blob_files_synced = 0;
    let mut blob_files_skipped = 0;
    for blob in &source_blobs {
        if !source_blob_db.has_blob_bytes(&blob.cid) {
            log.info(&format!("Source blob file missing, skipping: {}", blob.cid));
            blob_files_skipped += 1;
            continue;
        }

        let bytes = match source_blob_db.get_blob_bytes(&blob.cid) {
            Ok(b) => b,
            Err(e) => {
                log.error(&format!("Failed to read source blob file {}: {}", blob.cid, e));
                return;
            }
        };

        // Write blob to dest (overwrite if exists)
        if let Err(e) = dest_blob_db.insert_blob_bytes(&blob.cid, &bytes) {
            log.error(&format!("Failed to write dest blob file {}: {}", blob.cid, e));
            return;
        }
        blob_files_synced += 1;
    }
    log.info(&format!("Blob files synced: {}, skipped: {}", blob_files_synced, blob_files_skipped));

    // =========================================================================
    // SUMMARY
    // =========================================================================
    log.info("");
    log.info("=== SYNC COMPLETE ===");
    log.info(&format!("RepoHeader:  synced"));
    log.info(&format!("RepoCommit:  synced"));
    log.info(&format!("RepoRecords: {} synced", records_synced));
    log.info(&format!("Blobs:       {} metadata synced", blobs_synced));
    log.info(&format!("Blob files:  {} synced, {} skipped", blob_files_synced, blob_files_skipped));
}

async fn cmd_backup_account(args: &HashMap<String, String>) {
    let log = logger();

    //
    // Get arguments.
    //
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command BackupAccount /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command BackupAccount /actor <handle_or_did> /dataDir <path>");
            return;
        }
    };

    let get_prefs = get_arg(args, "getprefs")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let get_repo = get_arg(args, "getrepo")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let get_blobs = get_arg(args, "getblobs")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(true);

    let blob_sleep_seconds: u64 = get_arg(args, "blobsleepseconds")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    log.trace(&format!("actor: {}", actor));
    log.trace(&format!("dataDir: {}", data_dir));
    log.trace(&format!("getPrefs: {}", get_prefs));
    log.trace(&format!("getRepo: {}", get_repo));
    log.trace(&format!("getBlobs: {}", get_blobs));
    log.trace(&format!("blobSleepSeconds: {}", blob_sleep_seconds));

    //
    // Initialize local file system and resolve actor info.
    //
    let lfs = match LocalFileSystem::initialize_with_create(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize local file system: {}", e));
            return;
        }
    };

    let actor_info = match lfs.resolve_actor_info(actor, None, DEFAULT_APP_VIEW_HOST_NAME).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Failed to resolve actor info for actor: {} - {}", actor, e));
            return;
        }
    };

    let did = match &actor_info.did {
        Some(d) => d.clone(),
        None => {
            log.error("Failed to resolve actor to DID.");
            return;
        }
    };

    let pds = match &actor_info.pds {
        Some(p) => p.clone(),
        None => {
            log.error("Failed to resolve actor to PDS.");
            return;
        }
    };

    log.info(&format!("Resolved handle to did: {}", did));
    log.info(&format!("Resolved handle to pds: {}", pds));

    //
    // Load session (for authenticated requests like prefs).
    //
    let session = lfs.load_session(&did, None);
    let access_jwt = session
        .as_ref()
        .and_then(|s| s["accessJwt"].as_str())
        .map(|s| s.to_string());

    let has_session = access_jwt.is_some();

    if !has_session {
        log.warning(&format!(
            "Failed to load session for actor: {}. Will not be able to backup prefs.",
            actor
        ));
    }

    //
    // Verify session by calling getPreferences.
    //
    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    if has_session && get_prefs {
        log.info("Verifying session by calling getPreferences...");
        match client.get_preferences(&pds, access_jwt.as_ref().unwrap()).await {
            Ok(_) => {
                log.info("Session verified successfully.");
            }
            Err(e) => {
                log.error(&format!(
                    "Failed to verify session with getPreferences call. Is the session still valid? Error: {}",
                    e
                ));
                return;
            }
        }
    }

    //
    // Set up backup directory.
    //
    let backup_dir = match lfs.get_path_account_backup_dir(&did) {
        Ok(d) => d,
        Err(e) => {
            log.error(&format!("Failed to get backup directory: {}", e));
            return;
        }
    };

    if !backup_dir.exists() {
        log.trace(&format!("Creating backup directory: {}", backup_dir.display()));
        if let Err(e) = std::fs::create_dir_all(&backup_dir) {
            log.error(&format!("Failed to create backup directory: {}", e));
            return;
        }
    }

    //
    // Create README.txt
    //
    let readme_path = backup_dir.join("README.txt");
    let readme_contents = format!(
        "Account backup for Bluesky account. Created by rustproto.\n\n\
         actor: {}\n\
         backupDir: {}\n\
         getPrefs: {}\n\
         getRepo: {}\n\
         getBlobs: {}\n",
        actor,
        backup_dir.display(),
        get_prefs,
        get_repo,
        get_blobs
    );
    log.info(&format!("Creating readme file: {}", readme_path.display()));
    if let Err(e) = std::fs::write(&readme_path, &readme_contents) {
        log.error(&format!("Failed to create readme file: {}", e));
        return;
    }

    //
    // Get prefs.
    //
    if get_prefs && has_session {
        log.info("");
        log.info("----- PREFS -----");
        match client.get_preferences(&pds, access_jwt.as_ref().unwrap()).await {
            Ok(prefs) => {
                let prefs_file = backup_dir.join("prefs.json");
                log.info(&format!("Creating prefs file: {}", prefs_file.display()));
                let prefs_string = serde_json::to_string_pretty(&prefs).unwrap_or_default();
                if let Err(e) = std::fs::write(&prefs_file, prefs_string) {
                    log.error(&format!("Failed to write prefs file: {}", e));
                    return;
                }
            }
            Err(e) => {
                log.error(&format!("Failed to get preferences: {}", e));
                return;
            }
        }
    }

    //
    // Get repo.
    //
    if get_repo {
        let repo_file = backup_dir.join("repo.car");
        log.info("");
        log.info("----- REPO -----");
        log.info(&format!("Getting repo file: {}", repo_file.display()));
        match client.get_repo(&pds, &did, &repo_file).await {
            Ok(bytes) => {
                log.info(&format!("Downloaded {} bytes", bytes));
            }
            Err(e) => {
                log.error(&format!("Failed to download repo: {}", e));
            }
        }
    }

    //
    // Get blobs.
    //
    if get_blobs {
        log.info("");
        log.info("----- BLOBS -----");

        // List blobs
        let blobs = match client.list_blobs(&pds, &did).await {
            Ok(b) => b,
            Err(e) => {
                log.error(&format!("Failed to list blobs: {}", e));
                return;
            }
        };

        let blob_list_file = backup_dir.join("blobs.txt");
        log.info(&format!("Found {} blobs.", blobs.len()));
        log.info(&format!("Creating blob list file: {}", blob_list_file.display()));
        if let Err(e) = std::fs::write(&blob_list_file, blobs.join("\n")) {
            log.error(&format!("Failed to write blob list file: {}", e));
            return;
        }

        // Create blobs directory
        let blobs_directory = backup_dir.join("blobs");
        if !blobs_directory.exists() {
            log.info(&format!("Creating blobs directory: {}", blobs_directory.display()));
            if let Err(e) = std::fs::create_dir_all(&blobs_directory) {
                log.error(&format!("Failed to create blobs directory: {}", e));
                return;
            }
        }

        // Download all blobs
        let mut blob_count_downloaded = 0;
        let mut blob_count_skipped = 0;

        for blob in &blobs {
            let blob_path = blobs_directory.join(blob);

            if !blob_path.exists() {
                log.info(&format!("Downloading blob: {}", blob_path.display()));
                match client.get_blob(&pds, &did, blob, &blob_path).await {
                    Ok(_) => {}
                    Err(e) => {
                        log.error(&format!("Failed to download blob {}: {}", blob, e));
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(blob_sleep_seconds)).await;
                blob_count_downloaded += 1;
            } else {
                log.trace(&format!("Blob file already exists, skipping: {}", blob_path.display()));
                blob_count_skipped += 1;
            }
        }

        // Count blob files on disk
        let blob_file_count = std::fs::read_dir(&blobs_directory)
            .map(|rd| rd.count())
            .unwrap_or(0);

        log.info(&format!(
            "Downloaded {} blobs, skipped {} blobs. There are {} blob files on disk.",
            blob_count_downloaded, blob_count_skipped, blob_file_count
        ));

        log.info("");
    }
}

async fn cmd_create_session(args: &HashMap<String, String>) {
    let log = logger();

    //
    // Get arguments.
    //
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command CreateSession /actor <handle_or_did> /dataDir <path> /password <password>");
            return;
        }
    };

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command CreateSession /actor <handle_or_did> /dataDir <path> /password <password>");
            return;
        }
    };

    let password = match get_arg(args, "password") {
        Some(p) => p.to_string(),
        None => {
            // Prompt for password
            log.info(&format!("Actor is {}.", actor));
            eprint!("please enter password:");
            let mut password = String::new();
            match std::io::stdin().read_line(&mut password) {
                Ok(_) => password.trim().to_string(),
                Err(e) => {
                    log.error(&format!("Failed to read password: {}", e));
                    return;
                }
            }
        }
    };

    let auth_factor_token = get_arg(args, "authfactortoken").map(|s| s.to_string());

    //
    // Initialize local file system and resolve actor info.
    //
    let lfs = match LocalFileSystem::initialize_with_create(data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize local file system: {}", e));
            return;
        }
    };

    let actor_info = match lfs.resolve_actor_info(actor, None, DEFAULT_APP_VIEW_HOST_NAME).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Failed to resolve actor info: {}", e));
            return;
        }
    };

    let did = match &actor_info.did {
        Some(d) => d.clone(),
        None => {
            log.error("Failed to resolve actor to DID.");
            return;
        }
    };

    let pds = match &actor_info.pds {
        Some(p) => p.clone(),
        None => {
            log.warning("Failed to resolve PDS, defaulting to bsky.social");
            "bsky.social".to_string()
        }
    };

    log.info(&format!("Resolving handle to get pds: {}", pds));

    //
    // Get session file path.
    //
    let session_file = match lfs.get_path_session_file(&did) {
        Ok(p) => p,
        Err(e) => {
            log.error(&format!("Failed to get session file path: {}", e));
            return;
        }
    };

    //
    // Create session.
    //
    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    let mut session = match client
        .create_session(
            &pds,
            actor,
            &password,
            auth_factor_token.as_deref(),
        )
        .await
    {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to create session: {}", e));
            return;
        }
    };

    // Add pds to the session JSON
    session["pds"] = serde_json::Value::String(pds);

    //
    // Write session to disk.
    //
    log.info(&format!("Writing session file: {}", session_file.display()));
    let session_string = serde_json::to_string_pretty(&session).unwrap_or_default();
    if let Err(e) = std::fs::write(&session_file, &session_string) {
        log.error(&format!("Failed to write session file: {}", e));
        return;
    }

    // Print the session response
    log.info("");
    log.info(&session_string);
}
