//! rustproto CLI - AT Protocol / Bluesky tools

use std::sync::Arc;

use rustproto::log::{
    init_logger, 
    logger, 
    FileDestination, 
    LogLevel
};

use rustproto::cli::{
    get_arg,
    parse_arguments,
    backup_account::cmd_backup_account,
    ap_resolve_actor::cmd_ap_resolve_actor,
    create_session::cmd_create_session,
    get_blob::cmd_get_blob,
    get_pds_info::cmd_get_pds_info,
    get_plc_history::cmd_get_plc_history,
    get_post::cmd_get_post,
    get_repo::cmd_get_repo,
    inspect_firehose_event::cmd_inspect_firehose_event,
    install_config::cmd_install_config,
    install_db::cmd_install_db,
    print_db_mst::cmd_print_db_mst,
    print_repo_stats::cmd_print_repo_stats,
    print_repo_records::cmd_print_repo_records,
    repair_commit::cmd_repair_commit,
    resolve_actor::cmd_resolve_actor,
    run_pds::cmd_run_pds,
    start_firehose_consumer::cmd_start_firehose_consumer,
    sync_get_record::cmd_sync_get_record,
    sync_get_record_local::cmd_sync_get_record_local,
    sync_repo::cmd_sync_repo,
    test_apply_writes_and_log_firehose::cmd_test_apply_writes_and_log_firehose,
    walk_mst::cmd_walk_mst
};


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
        "backupaccount" => cmd_backup_account(&arguments).await,
        "apresolveactor" => cmd_ap_resolve_actor(&arguments).await,
        "createsession" => cmd_create_session(&arguments).await,
        "getblob" => cmd_get_blob(&arguments).await,
        "getpdsinfo" => cmd_get_pds_info(&arguments).await,
        "getplchistory" => cmd_get_plc_history(&arguments).await,
        "getpost" => cmd_get_post(&arguments).await,
        "getrepo" => cmd_get_repo(&arguments).await,
        "help" => print_usage(),
        "inspectfirehoseevent" => cmd_inspect_firehose_event(&arguments),
        "installconfig" => cmd_install_config(&arguments),
        "installdb" => cmd_install_db(&arguments),
        "printdbmst" => cmd_print_db_mst(&arguments),
        "printreporecords" => cmd_print_repo_records(&arguments).await,
        "printrepostats" => cmd_print_repo_stats(&arguments).await,
        "repaircommit" => cmd_repair_commit(&arguments),
        "resolveactorinfo" => cmd_resolve_actor(&arguments).await,
        "runpds" => cmd_run_pds(&arguments).await,
        "startfirehoseconsumer" => cmd_start_firehose_consumer(&arguments).await,
        "syncgetrecord" => cmd_sync_get_record(&arguments).await,
        "syncgetrecordlocal" => cmd_sync_get_record_local(&arguments),
        "syncrepo" => cmd_sync_repo(&arguments),
        "testapplywritesandlogfirehose" => cmd_test_apply_writes_and_log_firehose(&arguments),
        "walkmst" => cmd_walk_mst(&arguments).await,
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
    println!("  GetBlob                Download a blob by CID for an actor");
    println!("  GetPost                Get a post and print all URIs found");
    println!("  SyncGetRecord         Get a record from a remote PDS and verify the proof chain");
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
    println!("  /blobCid <string>     CID of a blob to download (for GetBlob)");
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
    println!("  rustproto /command SyncGetRecord /actor alice.bsky.social /collection app.bsky.feed.post /rkey 3abc123");
    println!("  rustproto /command SyncGetRecordLocal /dataDir ./data /collection app.bsky.feed.post /rkey 3abc123");
    println!("  rustproto /command SyncRepo /sourceDataDir ./source-data /destDataDir ./dest-data");
    println!("  rustproto /command BackupAccount /actor alice.bsky.social /dataDir ./data");
    println!("  rustproto /command GetBlob /actor alice.bsky.social /blobCid bafkrei... /dataDir ./data");
    println!("  rustproto /command CreateSession /actor alice.bsky.social /dataDir ./data /password mypass");
}



