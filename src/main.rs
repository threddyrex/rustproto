//! rustproto CLI - AT Protocol / Bluesky tools

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use chrono::Datelike;
use rustproto::firehose::Firehose;
use rustproto::fs::LocalFileSystem;
use rustproto::log::{init_logger, logger, FileDestination, LogLevel};
use rustproto::mst::Mst;
use rustproto::pds::{Installer, PdsDb};
use rustproto::repo::{CidV1, DagCborObject, DagCborValue, Repo, RepoMst, RepoRecord, AtProtoType, MstNodeKey};
use rustproto::ws::{ActorQueryOptions, BlueskyClient};

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
        "startfirehoseconsumer" => cmd_start_firehose_consumer(&arguments).await,
        "installdb" => cmd_install_db(&arguments),
        "installconfig" => cmd_install_config(&arguments),
        "repaircommit" => cmd_repair_commit(&arguments),
        "runpds" => cmd_run_pds(&arguments).await,
        "inspectfirehoseevent" => cmd_inspect_firehose_event(&arguments),
        "getplchistory" => cmd_get_plc_history(&arguments).await,
        "getpdsinfo" => cmd_get_pds_info(&arguments).await,
        "getpost" => cmd_get_post(&arguments).await,
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
}

fn cmd_install_db(args: &HashMap<String, String>) {
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

fn cmd_install_config(args: &HashMap<String, String>) {
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

fn cmd_repair_commit(args: &HashMap<String, String>) {
    use rustproto::mst::{Mst, MstItem};
    use rustproto::pds::db::{DbRepoCommit, PdsDb};
    use rustproto::repo::{CidV1, DagCborObject, RepoMst, MstNodeKey};
    use sha2::{Digest, Sha256};

    let log = logger();

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command RepairCommit /dataDir <path>");
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

    let db = match PdsDb::connect(&lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to connect to database: {}", e));
            return;
        }
    };

    // Get existing commit
    let old_commit = match db.get_repo_commit() {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to get repo commit: {}", e));
            return;
        }
    };

    log.info(&format!("Current commit CID: {}", old_commit.cid));
    log.info(&format!("Current root MST CID: {}", old_commit.root_mst_node_cid));
    log.info(&format!("Current rev: {}", old_commit.rev));

    // Get all repo records and rebuild MST
    let all_records = match db.get_all_repo_records() {
        Ok(r) => r,
        Err(e) => {
            log.error(&format!("Failed to get repo records: {}", e));
            return;
        }
    };

    log.info(&format!("Found {} repo records", all_records.len()));

    let mst_items: Vec<MstItem> = all_records
        .iter()
        .map(|r| MstItem::new(&format!("{}/{}", r.collection, r.rkey), &r.cid))
        .collect();

    let mst = Mst::assemble_tree_from_items(&mst_items);

    // Convert entire MST to DAG-CBOR
    let mst_cache = match RepoMst::convert_mst_to_dag_cbor(&mst) {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to convert MST to DAG-CBOR: {}", e));
            return;
        }
    };

    // Get root node CID
    let root_key = MstNodeKey::from_node(&mst.root);
    let root_cid = match mst_cache.get(&root_key) {
        Some((cid, _)) => cid.clone(),
        None => {
            log.error("Root MST node not found in cache");
            return;
        }
    };

    log.info(&format!("Computed new root MST CID: {}", root_cid.base32));

    if root_cid.base32 == old_commit.root_mst_node_cid {
        log.info("Root MST CID matches existing commit - no repair needed.");
        return;
    }

    log.info("Root MST CID differs - re-signing commit...");

    // Get signing keys
    let private_key_multibase = match db.get_config_property("UserPrivateKeyMultibase") {
        Ok(k) => k,
        Err(e) => {
            log.error(&format!("Failed to get private key: {}", e));
            return;
        }
    };

    let user_did = match db.get_config_property("UserDid") {
        Ok(d) => d,
        Err(e) => {
            log.error(&format!("Failed to get user DID: {}", e));
            return;
        }
    };

    // Create unsigned commit
    let mut commit_map: std::collections::HashMap<String, DagCborObject> = std::collections::HashMap::new();
    commit_map.insert("did".to_string(), DagCborObject::new_text(user_did.clone()));
    commit_map.insert("version".to_string(), DagCborObject::new_unsigned_int(3));
    commit_map.insert("data".to_string(), DagCborObject::new_cid(root_cid.clone()));
    commit_map.insert("rev".to_string(), DagCborObject::new_text(old_commit.rev.clone()));
    commit_map.insert("prev".to_string(), DagCborObject::new_null());

    let unsigned_commit = DagCborObject::new_map(commit_map.clone());

    // Hash the unsigned commit
    let unsigned_bytes = match unsigned_commit.to_bytes() {
        Ok(b) => b,
        Err(e) => {
            log.error(&format!("Failed to serialize unsigned commit: {}", e));
            return;
        }
    };

    let mut hasher = Sha256::new();
    hasher.update(&unsigned_bytes);
    let hash: [u8; 32] = hasher.finalize().into();

    // Sign the hash
    let signature = match sign_commit_hash(&hash, &private_key_multibase) {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to sign commit: {}", e));
            return;
        }
    };

    // Create signed commit
    commit_map.insert("sig".to_string(), DagCborObject::new_byte_string(signature.clone()));
    let signed_commit = DagCborObject::new_map(commit_map);

    // Compute CID of signed commit
    let commit_cid = match CidV1::compute_cid_for_dag_cbor(&signed_commit) {
        Ok(c) => c,
        Err(e) => {
            log.error(&format!("Failed to compute commit CID: {}", e));
            return;
        }
    };

    log.info(&format!("New commit CID: {}", commit_cid.base32));

    // Update database
    let new_commit = DbRepoCommit {
        version: 3,
        cid: commit_cid.base32.clone(),
        root_mst_node_cid: root_cid.base32.clone(),
        rev: old_commit.rev.clone(),
        prev_mst_node_cid: None,
        signature: signature.clone(),
    };

    if let Err(e) = db.update_repo_commit(&new_commit) {
        log.error(&format!("Failed to update repo commit: {}", e));
        return;
    }

    // Update repo header
    let header = rustproto::pds::db::DbRepoHeader {
        repo_commit_cid: commit_cid.base32.clone(),
        version: 1,
    };

    if let Err(e) = db.insert_update_repo_header(&header) {
        log.error(&format!("Failed to update repo header: {}", e));
        return;
    }

    log.info("Commit repaired successfully!");
    log.info(&format!("Old root MST CID: {}", old_commit.root_mst_node_cid));
    log.info(&format!("New root MST CID: {}", root_cid.base32));
    log.info(&format!("New commit CID: {}", commit_cid.base32));
}

/// Sign a commit hash using the private key (helper for cmd_repair_commit).
fn sign_commit_hash(hash: &[u8; 32], private_key_multibase: &str) -> Result<Vec<u8>, String> {
    use p256::ecdsa::{signature::hazmat::PrehashSigner, Signature, SigningKey};

    // Decode the multibase private key (z prefix = base58btc)
    if !private_key_multibase.starts_with('z') {
        return Err("Private key must be multibase (base58btc, z prefix)".to_string());
    }

    let private_key_with_prefix = bs58::decode(&private_key_multibase[1..])
        .into_vec()
        .map_err(|e| format!("Invalid base58: {}", e))?;

    // Check for P-256 private key prefix (0x86 0x26)
    if private_key_with_prefix.len() < 34 {
        return Err("Private key too short".to_string());
    }

    if private_key_with_prefix[0] != 0x86 || private_key_with_prefix[1] != 0x26 {
        return Err(format!(
            "Expected P-256 private key prefix (0x86 0x26), got 0x{:02X} 0x{:02X}",
            private_key_with_prefix[0], private_key_with_prefix[1]
        ));
    }

    let private_key_bytes = &private_key_with_prefix[2..];
    if private_key_bytes.len() != 32 {
        return Err(format!(
            "Expected 32-byte private key, got {} bytes",
            private_key_bytes.len()
        ));
    }

    // Create signing key
    let signing_key = SigningKey::from_slice(private_key_bytes)
        .map_err(|e| format!("Invalid P-256 key: {}", e))?;

    // Sign the hash (prehashed)
    let signature: Signature = signing_key
        .sign_prehash(hash)
        .map_err(|e| format!("Signing failed: {}", e))?;

    // Get r and s values (IEEE P1363 format: r || s)
    let signature_bytes = signature.to_bytes();

    // Normalize to low-S form
    Ok(normalize_low_s(&signature_bytes))
}

/// Normalize ECDSA signature to low-S form (BIP-62 compliance).
fn normalize_low_s(signature: &[u8]) -> Vec<u8> {
    if signature.len() != 64 {
        return signature.to_vec();
    }

    let r = &signature[0..32];
    let s = &signature[32..64];

    // P-256 curve order
    let order: [u8; 32] = [
        0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00,
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xBC, 0xE6, 0xFA, 0xAD, 0xA7, 0x17, 0x9E, 0x84,
        0xF3, 0xB9, 0xCA, 0xC2, 0xFC, 0x63, 0x25, 0x51,
    ];

    // half_order = order / 2
    let half_order: [u8; 32] = [
        0x7F, 0xFF, 0xFF, 0xFF, 0x80, 0x00, 0x00, 0x00,
        0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        0xDE, 0x73, 0x7D, 0x56, 0xD3, 0x8B, 0xCF, 0x42,
        0x79, 0xDC, 0xE5, 0x61, 0x7E, 0x31, 0x92, 0xA8,
    ];

    // Check if s > half_order
    let s_high = compare_bytes(s, &half_order) > 0;

    if s_high {
        // s = order - s
        let new_s = subtract_bytes(&order, s);
        let mut result = Vec::with_capacity(64);
        result.extend_from_slice(r);
        result.extend_from_slice(&new_s);
        result
    } else {
        signature.to_vec()
    }
}

fn compare_bytes(a: &[u8], b: &[u8]) -> i32 {
    for (x, y) in a.iter().zip(b.iter()) {
        if x > y {
            return 1;
        }
        if x < y {
            return -1;
        }
    }
    0
}

fn subtract_bytes(a: &[u8; 32], b: &[u8]) -> [u8; 32] {
    let mut result = [0u8; 32];
    let mut borrow: i16 = 0;

    for i in (0..32).rev() {
        let diff = (a[i] as i16) - (b[i] as i16) - borrow;
        if diff < 0 {
            result[i] = (diff + 256) as u8;
            borrow = 1;
        } else {
            result[i] = diff as u8;
            borrow = 0;
        }
    }

    result
}

async fn cmd_run_pds(args: &HashMap<String, String>) {
    let log = logger();

    let data_dir = match get_arg(args, "datadir") {
        Some(d) => d,
        None => {
            log.error("missing /dataDir argument");
            log.error("Usage: rustproto /command RunPds /dataDir <path>");
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

    let server = match rustproto::pds::PdsServer::initialize(lfs, log) {
        Ok(s) => s,
        Err(e) => {
            log.error(&format!("Failed to initialize PDS server: {}", e));
            return;
        }
    };

    if let Err(e) = server.run().await {
        log.error(&format!("PDS server error: {}", e));
    }
}

async fn cmd_resolve_actor(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command ResolveActorInfo /actor <handle_or_did>");
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

async fn cmd_walk_mst(args: &HashMap<String, String>) {
    use rustproto::mst::MstNode;

    let log = logger();

    // Get arguments
    let actor = get_arg(args, "actor");
    let repo_file_arg = get_arg(args, "repofile");

    // Determine repo file path
    let repo_file: String = if let Some(rf) = repo_file_arg {
        rf.to_string()
    } else if let Some(act) = actor {
        let data_dir = match get_arg(args, "datadir") {
            Some(d) => d,
            None => {
                log.error("missing /dataDir argument when using /actor");
                log.error("Usage: rustproto /command WalkMst /actor <handle_or_did> /dataDir <path>");
                log.error("   or: rustproto /command WalkMst /repoFile <path>");
                return;
            }
        };

        let lfs = match LocalFileSystem::initialize(data_dir) {
            Ok(lfs) => lfs,
            Err(e) => {
                log.error(&format!("Error initializing data directory: {}", e));
                return;
            }
        };

        // Resolve actor to get DID
        let client = BlueskyClient::new();
        let info = match client.resolve_actor_info(act, None).await {
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

        match lfs.get_path_repo_file(&did) {
            Ok(path) => path.to_string_lossy().to_string(),
            Err(e) => {
                log.error(&format!("Error getting repo path for actor: {}", e));
                return;
            }
        }
    } else {
        log.error("missing /actor or /repoFile argument");
        log.error("Usage: rustproto /command WalkMst /actor <handle_or_did> /dataDir <path>");
        log.error("   or: rustproto /command WalkMst /repoFile <path>");
        return;
    };

    // Check if file exists
    if !std::path::Path::new(&repo_file).exists() {
        log.error(&format!("Repo file does not exist: {}", repo_file));
        return;
    }

    // Load MST items from repo
    log.info(&format!("Loading MST from: {}", repo_file));
    let mst_items = match RepoMst::load_mst_items_from_repo_file(&repo_file, log) {
        Ok(items) => items,
        Err(e) => {
            log.error(&format!("Error loading MST items: {}", e));
            return;
        }
    };

    // Assemble tree
    let mst = Mst::assemble_tree_from_items(&mst_items);
    let all_mst_nodes = mst.find_all_nodes();

    // Convert to DAG-CBOR and cache CIDs
    let mst_node_cache = match RepoMst::convert_mst_to_dag_cbor(&mst) {
        Ok(cache) => cache,
        Err(e) => {
            log.error(&format!("Error converting MST to DAG-CBOR: {}", e));
            return;
        }
    };

    // Compute stats
    let mut mst_entry_count = 0;
    for node in &all_mst_nodes {
        mst_entry_count += node.entries.len();
    }

    // Print stats
    log.info("");
    log.info(&format!("mst_items.len(): {}", mst_items.len()));
    log.info(&format!("all_mst_nodes.len(): {}", all_mst_nodes.len()));
    log.info(&format!("mst_node_cache.len(): {}", mst_node_cache.len()));
    log.info(&format!("mst_entry_count: {}", mst_entry_count));
    log.info(&format!("root depth: {}", mst.root.key_depth));
    log.info("");

    // Walk and print tree structure
    fn visit_node(
        log: &rustproto::log::Logger,
        mst_node_cache: &HashMap<MstNodeKey, (CidV1, DagCborObject)>,
        node: &MstNode,
        indent: usize,
        direction: &str,
    ) {
        let indent_str = " ".repeat(indent);
        let node_key = MstNodeKey::from_node(node);
        
        let cid_str = mst_node_cache
            .get(&node_key)
            .map(|(cid, _)| cid.get_base32().to_string())
            .unwrap_or_else(|| "<unknown>".to_string());

        log.trace(&format!(
            "{} [{}] [{}] {}",
            indent_str, direction, node.key_depth, cid_str
        ));

        for entry in &node.entries {
            log.trace(&format!("{} {}: {}", indent_str, entry.key, entry.value));
        }

        log.trace("");

        if let Some(ref left) = node.left_tree {
            visit_node(log, mst_node_cache, left, indent + 2, "left");
            log.trace("");
        }

        for entry in &node.entries {
            if let Some(ref right) = entry.right_tree {
                visit_node(log, mst_node_cache, right, indent + 2, "right");
            }
        }
    }

    log.trace("");
    visit_node(log, &mst_node_cache, &mst.root, 0, "root");
    log.trace("");
}

async fn cmd_start_firehose_consumer(args: &HashMap<String, String>) {
    let log = logger();

    // Get actor argument
    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command StartFirehoseConsumer /actor <handle_or_did> /dataDir <path>");
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

/// Inspect a stored firehose event from the PDS database for debugging.
fn cmd_inspect_firehose_event(args: &HashMap<String, String>) {
    let log = logger();
    log.info("InspectFirehoseEvent command started");

    let data_dir = match get_arg(args, "dataDir") {
        Some(d) => d.to_string(),
        None => {
            log.error("Missing required argument: /dataDir");
            return;
        }
    };

    let seq_str = match get_arg(args, "seq") {
        Some(s) => s.to_string(),
        None => {
            log.error("Missing required argument: /seq (sequence number)");
            return;
        }
    };

    let sequence_number: i64 = match seq_str.parse() {
        Ok(n) => n,
        Err(_) => {
            log.error(&format!("Invalid sequence number: {}", seq_str));
            return;
        }
    };

    // Open PDS database
    let lfs = match LocalFileSystem::initialize(&data_dir) {
        Ok(lfs) => lfs,
        Err(e) => {
            log.error(&format!("Failed to initialize file system: {}", e));
            return;
        }
    };
    let db = match PdsDb::connect(&lfs) {
        Ok(db) => db,
        Err(e) => {
            log.error(&format!("Failed to open database: {}", e));
            return;
        }
    };

    // Get the firehose event
    let event = match db.get_firehose_event(sequence_number) {
        Ok(e) => e,
        Err(e) => {
            log.error(&format!("Failed to get firehose event {}: {}", sequence_number, e));
            return;
        }
    };

    log.info(&format!("=== FIREHOSE EVENT {} ===", sequence_number));
    log.info(&format!("Created: {}", event.created_date));
    log.info(&format!("Header op: {}", event.header_op));
    log.info(&format!("Header t: {:?}", event.header_t));
    log.info(&format!("Header bytes length: {}", event.header_dag_cbor_bytes.len()));
    log.info(&format!("Body bytes length: {}", event.body_dag_cbor_bytes.len()));

    // Parse and display header DAG-CBOR
    log.info("");
    log.info("=== HEADER DAG-CBOR ===");
    let mut header_cursor = Cursor::new(&event.header_dag_cbor_bytes);
    match DagCborObject::read_from_stream(&mut header_cursor) {
        Ok(header_obj) => {
            log.info(&format!("Header JSON:\n{}", header_obj.to_json_string()));
            log.info(&format!("Header debug:\n{}", header_obj.get_recursive_debug_string(0)));
        }
        Err(e) => {
            log.error(&format!("Failed to parse header DAG-CBOR: {}", e));
            log.info(&format!("Header hex: {}", hex_encode(&event.header_dag_cbor_bytes)));
        }
    }

    // Parse and display body DAG-CBOR
    log.info("");
    log.info("=== BODY DAG-CBOR ===");
    let mut body_cursor = Cursor::new(&event.body_dag_cbor_bytes);
    match DagCborObject::read_from_stream(&mut body_cursor) {
        Ok(body_obj) => {
            // Print JSON (may have binary data as base64)
            log.info(&format!("Body JSON:\n{}", body_obj.to_json_string()));
            
            // Print debug structure
            log.info(&format!("Body debug:\n{}", body_obj.get_recursive_debug_string(0)));

            // If this is a #commit or #sync, try to parse the blocks
            if let Some(blocks_obj) = body_obj.select_object(&["blocks"]) {
                if let DagCborValue::ByteString(blocks_bytes) = &blocks_obj.value {
                    log.info("");
                    log.info(&format!("=== BLOCKS ({} bytes) ===", blocks_bytes.len()));
                    
                    let mut blocks_cursor = Cursor::new(blocks_bytes);
                    let walk_result = Repo::walk_repo(
                        &mut blocks_cursor,
                        |repo_header| {
                            log.info("CAR HEADER:");
                            log.info(&format!("   roots: {}", repo_header.repo_commit_cid.get_base32()));
                            log.info(&format!("   version: {}", repo_header.version));
                            true
                        },
                        |repo_record| {
                            log.info(&format!("BLOCK CID: {}", repo_record.cid.get_base32()));
                            log.info(&format!("BLOCK JSON:\n{}", repo_record.json_string));
                            log.info(&format!("BLOCK debug:\n{}", repo_record.data_block.get_recursive_debug_string(0)));
                            true
                        },
                    );

                    if let Err(e) = walk_result {
                        log.error(&format!("Error walking blocks: {}", e));
                        log.info(&format!("Blocks hex (first 500 bytes): {}", hex_encode(&blocks_bytes[..std::cmp::min(500, blocks_bytes.len())])));
                    }
                } else {
                    log.info("blocks field is not a byte string");
                }
            }
        }
        Err(e) => {
            log.error(&format!("Failed to parse body DAG-CBOR: {}", e));
            log.info(&format!("Body hex (first 500 bytes): {}", hex_encode(&event.body_dag_cbor_bytes[..std::cmp::min(500, event.body_dag_cbor_bytes.len())])));
        }
    }
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

    let client = BlueskyClient::new();

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
    pds_status.insert("https://bsky.social".to_string(), "<na>".to_string());

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
                // Extract hostname from PDS URL
                let pds_host = pds_url
                    .trim_start_matches("https://")
                    .trim_start_matches("http://");

                let active = if !pds_status.contains_key(pds_url) {
                    match client.get_repo_status(pds_host, &did).await {
                        Ok(status) => {
                            let active_val = status["active"]
                                .as_bool()
                                .map(|b| b.to_string())
                                .unwrap_or_else(|| "<null>".to_string());
                            pds_status.insert(pds_url.clone(), active_val.clone());
                            active_val
                        }
                        Err(_) => {
                            pds_status.insert(pds_url.clone(), "<exception>".to_string());
                            "<exception>".to_string()
                        }
                    }
                } else {
                    pds_status.get(pds_url).cloned().unwrap_or_default()
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

    let client = BlueskyClient::new();

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

    let client = BlueskyClient::new();

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

/// Convert bytes to hex string for debugging.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
