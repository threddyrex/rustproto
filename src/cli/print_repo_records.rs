

use std::collections::HashMap;

use super::{resolve_repo_file};

use crate::cli::get_arg;
use crate::log::{logger};
use crate::repo::{Repo, RepoRecord};


pub async fn cmd_print_repo_records(args: &HashMap<String, String>) {
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


