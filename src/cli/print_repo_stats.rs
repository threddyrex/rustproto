
use std::collections::HashMap;
use chrono::Datelike;

use super::{resolve_repo_file};

use crate::log::{logger};
use crate::repo::{AtProtoType, Repo, RepoRecord};


pub async fn cmd_print_repo_stats(args: &HashMap<String, String>) {
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

