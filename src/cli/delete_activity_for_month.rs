//! Preview and delete monthly activity, preserving bookmarked posts.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Local, NaiveDate, NaiveDateTime};

use crate::cli::get_arg;
use crate::fs::LocalFileSystem;
use crate::log::logger;
use crate::repo::{AtProtoType, Repo, RepoMst};
use crate::uri::AtUri;
use crate::ws::{BlueskyClient, DEFAULT_APP_VIEW_HOST_NAME};

#[derive(Default)]
struct DeleteOptions {
    likes: bool,
    reposts: bool,
    posts: bool,
    blocks: bool,
    sleep_seconds: u64,
}

impl DeleteOptions {
    fn from_args(args: &HashMap<String, String>) -> Result<Self, String> {
        let flag = |name| match get_arg(args, name) {
            None => Ok(false),
            Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
            Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
            _ => Err(format!("/{} must be true or false", name)),
        };
        Ok(Self {
            likes: flag("deleteLikes")?,
            reposts: flag("deleteReposts")?,
            posts: flag("deletePosts")?,
            blocks: flag("deleteBlocks")?,
            sleep_seconds: match get_arg(args, "sleepSeconds") {
                None => 2,
                Some(value) => value
                    .parse()
                    .map_err(|_| "/sleepSeconds must be a non-negative integer".to_string())?,
            },
        })
    }
}

fn record_month(created_at: &str) -> Option<String> {
    // Match dnproto's DateTime.TryParse conversion to the local calendar month.
    if let Ok(date) = DateTime::parse_from_rfc3339(created_at) {
        Some(date.with_timezone(&Local).format("%Y-%m").to_string())
    } else {
        NaiveDateTime::parse_from_str(created_at, "%Y-%m-%dT%H:%M:%S%.f")
            .ok()
            .map(|date| date.format("%Y-%m").to_string())
    }
}

fn determination(
    collection: &str,
    rkey: Option<&str>,
    bookmarks: &HashSet<String>,
    options: &DeleteOptions,
) -> (String, bool) {
    let Some(rkey) = rkey.filter(|key| !key.is_empty()) else {
        return ("no rkey".to_string(), false);
    };
    match collection {
        AtProtoType::BLUESKY_LIKE => (format!("deleteLikes:{}", options.likes), options.likes),
        AtProtoType::BLUESKY_REPOST => (
            format!("deleteReposts:{}", options.reposts),
            options.reposts,
        ),
        AtProtoType::BLUESKY_BLOCK => (format!("deleteBlocks:{}", options.blocks), options.blocks),
        AtProtoType::BLUESKY_POST | AtProtoType::FLASHES_POST => {
            if bookmarks.contains(rkey) {
                ("bookmarked!".to_string(), false)
            } else {
                (format!("deletePosts:{}", options.posts), options.posts)
            }
        }
        _ => ("unknown collection".to_string(), false),
    }
}

pub async fn cmd_delete_activity_for_month(args: &HashMap<String, String>) {
    if let Err(error) = delete_activity_for_month(args).await {
        logger().error(&error);
    }
}

async fn delete_activity_for_month(args: &HashMap<String, String>) -> Result<(), String> {
    let required = |name| {
        get_arg(args, name)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| format!("missing /{} argument", name))
    };
    let actor = required("actor")?;
    let data_dir = required("dataDir")?;
    let month = required("month")?;
    if month.len() != 7 || NaiveDate::parse_from_str(&format!("{}-01", month), "%Y-%m-%d").is_err()
    {
        return Err("/month must be a valid yyyy-MM value".to_string());
    }
    let options = DeleteOptions::from_args(args)?;
    let log = logger();
    let lfs = LocalFileSystem::initialize(data_dir).map_err(|error| error.to_string())?;
    let actor_info = lfs
        .resolve_actor_info(actor, None, DEFAULT_APP_VIEW_HOST_NAME)
        .await
        .map_err(|error| error.to_string())?;
    let did = actor_info
        .did
        .as_deref()
        .ok_or("Failed to resolve actor to DID")?;
    let session = lfs.load_session(did, None).ok_or_else(|| {
        format!(
            "Failed to load session for actor: {}. Run CreateSession first.",
            actor
        )
    })?;
    let session_value = |name| {
        session[name]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("Session is missing {}", name))
    };
    if session_value("did")? != did {
        return Err("Session DID does not match the actor".to_string());
    }
    let pds = session_value("pds")?;
    let access_jwt = session_value("accessJwt")?;
    let repo_file = lfs
        .get_path_repo_file(did)
        .map_err(|error| error.to_string())?;
    if !repo_file.is_file() {
        return Err(format!(
            "Repo file does not exist: {}. Run GetRepo first.",
            repo_file.display()
        ));
    }

    let client = BlueskyClient::default();
    let bookmarks = client
        .get_bookmarks(pds, access_jwt)
        .await
        .map_err(|error| {
            format!(
                "Failed to retrieve bookmarks; no records deleted: {}",
                error
            )
        })?;
    // Keep dnproto's conservative rkey-based bookmark protection.
    let bookmark_rkeys: HashSet<String> = bookmarks
        .iter()
        .filter_map(|uri| AtUri::from_at_uri(uri).map(|uri| uri.rkey))
        .collect();
    log.info(&format!("bookmarks.Count: {}", bookmarks.len()));
    if bookmarks.is_empty() {
        return Err("The bookmarks don't look right (empty). No records deleted.".to_string());
    }

    let items = RepoMst::load_mst_items_from_repo_file(&repo_file, log)
        .map_err(|error| format!("Failed to read repo MST: {}", error))?;
    let keys: HashMap<_, _> = items
        .iter()
        .filter_map(|item| {
            let (collection, rkey) = item.key.split_once('/')?;
            Some(((item.value.as_str(), collection), rkey))
        })
        .collect();
    let mut records = Vec::new();
    let mut repo_did = None;
    Repo::walk_repo_file(
        &repo_file,
        |_| true,
        |record| {
            if record.is_repo_commit() {
                repo_did = record.data_block.select_string(&["did"]);
            }
            let Some(collection) = record.at_proto_type.as_deref() else {
                return true;
            };
            if !matches!(
                collection,
                AtProtoType::BLUESKY_POST
                    | AtProtoType::FLASHES_POST
                    | AtProtoType::BLUESKY_LIKE
                    | AtProtoType::BLUESKY_REPOST
                    | AtProtoType::BLUESKY_BLOCK
            ) {
                return true;
            }
            let Some(created_at) = record.created_at.as_deref() else {
                return true;
            };
            if record_month(created_at).as_deref() != Some(month) {
                return true;
            }
            let cid = record.cid.get_base32();
            let rkey = keys.get(&(cid, collection)).copied().map(str::to_string);
            let (reason, delete) =
                determination(collection, rkey.as_deref(), &bookmark_rkeys, &options);
            records.push((
                created_at.to_string(),
                collection.to_string(),
                rkey,
                reason,
                delete,
            ));
            true
        },
    )
    .map_err(|error| format!("Failed to walk repo: {}", error))?;
    if repo_did.as_deref() != Some(did) {
        return Err("Repo DID does not match the actor; no records deleted".to_string());
    }
    records.sort_by(|a, b| a.0.cmp(&b.0));

    let mut delete_counts = BTreeMap::new();
    let mut keep_counts = BTreeMap::new();
    for (created_at, collection, _, reason, delete) in &records {
        log.info(&format!(
            "[{}] [{}] {} {}",
            created_at,
            collection,
            reason,
            if *delete { "<----------- DELETE!" } else { "" }
        ));
        let counts = if *delete {
            &mut delete_counts
        } else {
            &mut keep_counts
        };
        *counts.entry(collection).or_insert(0usize) += 1;
    }
    for (title, counts) in [
        ("Summary of records to be DELETED:", &delete_counts),
        ("Summary of records to be KEPT:", &keep_counts),
    ] {
        log.info("");
        log.info(title);
        for (collection, count) in counts {
            log.info(&format!("[{}]: {} items", collection, count));
        }
    }
    if delete_counts.is_empty() {
        log.info("No records selected for deletion.");
        return Ok(());
    }

    println!(
        "Please review the printed records. Anything marked for deletion will be deleted. Type 'yes' to continue..."
    );
    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .map_err(|error| error.to_string())?;
    if !input.trim().eq_ignore_ascii_case("yes") {
        log.info("exiting...");
        return Ok(());
    }
    for (created_at, collection, rkey, reason, delete) in &records {
        if !delete {
            continue;
        }
        let rkey = rkey.as_deref().ok_or("Missing rkey for deletion")?;
        log.info(&format!(
            "[{}] [{}] {} <----------- DELETE!",
            created_at, collection, reason
        ));
        client
            .delete_record(pds, did, access_jwt, collection, rkey)
            .await
            .map_err(|error| {
                format!(
                    "Failed to delete {}/{}; stopping: {}",
                    collection, rkey, error
                )
            })?;
        tokio::time::sleep(Duration::from_secs(options.sleep_seconds)).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_are_opt_in_and_independent() {
        let args = HashMap::from([("deletelikes".to_string(), "True".to_string())]);
        let options = DeleteOptions::from_args(&args).unwrap();
        for collection in [
            AtProtoType::BLUESKY_LIKE,
            AtProtoType::BLUESKY_REPOST,
            AtProtoType::BLUESKY_POST,
            AtProtoType::FLASHES_POST,
            AtProtoType::BLUESKY_BLOCK,
        ] {
            assert!(
                !determination(
                    collection,
                    Some("key"),
                    &HashSet::new(),
                    &DeleteOptions::from_args(&HashMap::new()).unwrap()
                )
                .1
            );
            assert_eq!(
                determination(collection, Some("key"), &HashSet::new(), &options).1,
                collection == AtProtoType::BLUESKY_LIKE
            );
        }
        assert!(
            DeleteOptions::from_args(&HashMap::from([(
                "deleteposts".to_string(),
                "yes".to_string()
            )]))
            .is_err()
        );
        let blocks = DeleteOptions::from_args(&HashMap::from([(
            "deleteblocks".to_string(),
            "True".to_string(),
        )]))
        .unwrap();
        assert!(
            determination(
                AtProtoType::BLUESKY_BLOCK,
                Some("key"),
                &HashSet::new(),
                &blocks
            )
            .1
        );
        assert!(
            !determination(
                AtProtoType::BLUESKY_LIKE,
                Some("key"),
                &HashSet::new(),
                &blocks
            )
            .1
        );
        assert!(
            DeleteOptions::from_args(&HashMap::from([(
                "deleteblocks".to_string(),
                "yes".to_string(),
            )]))
            .is_err()
        );
        assert!(
            DeleteOptions::from_args(&HashMap::from([(
                "sleepseconds".to_string(),
                "-1".to_string()
            )]))
            .is_err()
        );
    }

    #[test]
    fn bookmarks_and_missing_keys_are_kept() {
        let options = DeleteOptions {
            likes: true,
            reposts: true,
            posts: true,
            blocks: true,
            sleep_seconds: 0,
        };
        let bookmarks = HashSet::from(["saved".to_string()]);
        for collection in [AtProtoType::BLUESKY_POST, AtProtoType::FLASHES_POST] {
            assert!(!determination(collection, Some("saved"), &bookmarks, &options).1);
            assert!(determination(collection, Some("other"), &bookmarks, &options).1);
        }
        for collection in [
            AtProtoType::BLUESKY_LIKE,
            AtProtoType::BLUESKY_REPOST,
            AtProtoType::BLUESKY_POST,
            AtProtoType::FLASHES_POST,
            AtProtoType::BLUESKY_BLOCK,
        ] {
            assert!(!determination(collection, None, &bookmarks, &options).1);
            assert!(!determination(collection, Some(""), &bookmarks, &options).1);
        }
        assert!(
            determination(
                AtProtoType::BLUESKY_LIKE,
                Some("saved"),
                &bookmarks,
                &options
            )
            .1
        );
        assert!(
            determination(
                AtProtoType::BLUESKY_REPOST,
                Some("saved"),
                &bookmarks,
                &options
            )
            .1
        );
        assert!(
            determination(
                AtProtoType::BLUESKY_BLOCK,
                Some("saved"),
                &bookmarks,
                &options
            )
            .1
        );
        assert!(
            !determination(
                AtProtoType::BLUESKY_FOLLOW,
                Some("other"),
                &bookmarks,
                &options
            )
            .1
        );
    }

    #[test]
    fn filters_valid_timestamps_by_local_month() {
        for timestamp in [
            "2026-01-15T12:00:00Z",
            "2026-01-15T12:00:00.123Z",
            "2026-01-15T12:00:00+02:00",
            "2026-01-15T12:00:00",
        ] {
            assert_eq!(record_month(timestamp).as_deref(), Some("2026-01"));
        }
        let boundary = "2026-02-01T00:00:00Z";
        let expected = DateTime::parse_from_rfc3339(boundary)
            .unwrap()
            .with_timezone(&Local)
            .format("%Y-%m")
            .to_string();
        assert_eq!(record_month(boundary), Some(expected));
        assert_eq!(record_month("not a date"), None);
    }
}
