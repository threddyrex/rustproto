use std::collections::HashMap;
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::State,
    http::HeaderMap,
    routing::{get, post},
};
use rustproto::fs::LocalFileSystem;
use rustproto::repo::{AtProtoType, CidV1, DagCborObject, Repo, RepoHeader, RepoRecord};
use rustproto::ws::ActorInfo;
use serde_json::{Value, json};

const DID: &str = "did:plc:monthlytest";

#[derive(Clone)]
struct MockPds {
    bookmarks: Arc<Mutex<Value>>,
    deletes: Arc<Mutex<Vec<Value>>>,
}

async fn get_bookmarks(State(state): State<MockPds>, headers: HeaderMap) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer test-token");
    Json(state.bookmarks.lock().unwrap().clone())
}

async fn delete_record(
    State(state): State<MockPds>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Json<Value> {
    assert_eq!(headers["authorization"], "Bearer test-token");
    assert_eq!(body["repo"], DID);
    state.deletes.lock().unwrap().push(body);
    Json(json!({}))
}

fn record(data: DagCborObject) -> RepoRecord {
    RepoRecord {
        cid: CidV1::compute_cid_for_dag_cbor(&data).unwrap(),
        at_proto_type: data.select_string(&["$type"]),
        created_at: data.select_string(&["createdAt"]),
        json_string: data.to_json_string(),
        data_block: data,
        is_error: false,
        error_message: None,
    }
}

fn create_repo(lfs: &LocalFileSystem) {
    let mut records = Vec::new();
    let mut entries = Vec::new();
    for (collection, rkey, created_at) in [
        (AtProtoType::BLUESKY_POST, "saved", "2026-01-15T12:00:00Z"),
        (AtProtoType::BLUESKY_POST, "post", "2026-01-15T12:01:00Z"),
        (AtProtoType::FLASHES_POST, "flashes", "2026-01-15T12:02:00Z"),
        (AtProtoType::BLUESKY_LIKE, "like", "2026-01-15T12:03:00Z"),
        (
            AtProtoType::BLUESKY_REPOST,
            "repost",
            "2026-01-15T12:04:00Z",
        ),
        (AtProtoType::BLUESKY_BLOCK, "block", "2026-01-15T12:05:00Z"),
        (
            AtProtoType::BLUESKY_BLOCK,
            "block-other-month",
            "2026-02-15T12:00:00Z",
        ),
        (AtProtoType::BLUESKY_BLOCK, "block-bad-date", "invalid"),
        (
            AtProtoType::BLUESKY_LIKE,
            "other-month",
            "2026-02-15T12:00:00Z",
        ),
        (AtProtoType::BLUESKY_LIKE, "bad-date", "invalid"),
        (
            AtProtoType::BLUESKY_FOLLOW,
            "follow",
            "2026-01-15T12:00:00Z",
        ),
    ] {
        let activity = record(DagCborObject::new_map(HashMap::from([
            (
                "$type".to_string(),
                DagCborObject::new_text(collection.to_string()),
            ),
            (
                "createdAt".to_string(),
                DagCborObject::new_text(created_at.to_string()),
            ),
            (
                "text".to_string(),
                DagCborObject::new_text(rkey.to_string()),
            ),
        ])));
        entries.push(DagCborObject::new_map(HashMap::from([
            ("p".to_string(), DagCborObject::new_unsigned_int(0)),
            (
                "k".to_string(),
                DagCborObject::new_byte_string(format!("{}/{}", collection, rkey).into_bytes()),
            ),
            (
                "v".to_string(),
                DagCborObject::new_cid(activity.cid.clone()),
            ),
        ])));
        records.push(activity);
    }
    let mst = record(DagCborObject::new_map(HashMap::from([(
        "e".to_string(),
        DagCborObject::new_array(entries),
    )])));
    let commit = record(DagCborObject::new_map(HashMap::from([
        ("did".to_string(), DagCborObject::new_text(DID.to_string())),
        ("data".to_string(), DagCborObject::new_cid(mst.cid.clone())),
        (
            "rev".to_string(),
            DagCborObject::new_text("test-rev".to_string()),
        ),
        ("version".to_string(), DagCborObject::new_unsigned_int(3)),
    ])));
    let header = RepoHeader {
        repo_commit_cid: commit.cid.clone(),
        version: 1,
    };
    records.push(mst);
    records.push(commit);
    Repo::write_repo_file(lfs.get_path_repo_file(DID).unwrap(), &header, &records).unwrap();
}

fn invoke_command(data_dir: &std::path::Path, flags: &[&str], confirmation: &str) -> String {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rustproto"))
        .args([
            "/command",
            "DeleteActivityForMonth",
            "/actor",
            DID,
            "/dataDir",
        ])
        .arg(data_dir)
        .args(["/month", "2026-01", "/sleepSeconds", "0"])
        .args(flags)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(confirmation.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn monthly_deletion_cli_preserves_bookmarks_and_requires_confirmation() {
    let state = MockPds {
        bookmarks: Arc::new(Mutex::new(json!({
            "bookmarks": [{"item": {"uri": format!("at://{}/app.bsky.feed.post/saved", DID)}}]
        }))),
        deletes: Arc::new(Mutex::new(Vec::new())),
    };
    let router = Router::new()
        .route("/xrpc/app.bsky.bookmark.getBookmarks", get(get_bookmarks))
        .route("/xrpc/com.atproto.repo.deleteRecord", post(delete_record))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pds = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let directory = tempfile::tempdir().unwrap();
    let lfs = LocalFileSystem::initialize(directory.path()).unwrap();
    lfs.save_actor_info(
        DID,
        &ActorInfo {
            actor: Some(DID.to_string()),
            did: Some(DID.to_string()),
            pds: Some(pds.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    std::fs::write(
        lfs.get_path_session_file(DID).unwrap(),
        json!({"did": DID, "pds": pds, "accessJwt": "test-token"}).to_string(),
    )
    .unwrap();
    create_repo(&lfs);

    let output = invoke_command(directory.path(), &[], "");
    assert!(output.contains("No records selected for deletion."));
    assert!(state.deletes.lock().unwrap().is_empty());

    let output = invoke_command(directory.path(), &["/deleteLikes", "true"], "no\n");
    assert!(output.contains("Type 'yes'"));
    assert!(state.deletes.lock().unwrap().is_empty());

    invoke_command(directory.path(), &["/deleteLikes", "true"], "YES\n");
    assert_eq!(
        *state.deletes.lock().unwrap(),
        [json!({
            "repo": DID, "collection": AtProtoType::BLUESKY_LIKE, "rkey": "like"
        })]
    );
    state.deletes.lock().unwrap().clear();

    let output = invoke_command(
        directory.path(),
        &[
            "/deleteLikes",
            "true",
            "/deleteReposts",
            "true",
            "/deletePosts",
            "true",
        ],
        "yes\n",
    );
    assert!(output.contains("bookmarked!"));
    let deleted_keys: Vec<String> = state
        .deletes
        .lock()
        .unwrap()
        .iter()
        .map(|body| body["rkey"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(deleted_keys, ["post", "flashes", "like", "repost"]);
    state.deletes.lock().unwrap().clear();

    let output = invoke_command(directory.path(), &["/deleteBlocks", "true"], "no\n");
    assert!(output.contains("deleteBlocks:true"));
    assert!(output.contains("Type 'yes'"));
    assert!(state.deletes.lock().unwrap().is_empty());

    invoke_command(directory.path(), &["/deleteBlocks", "true"], "yes\n");
    assert_eq!(
        *state.deletes.lock().unwrap(),
        [json!({
            "repo": DID, "collection": AtProtoType::BLUESKY_BLOCK, "rkey": "block"
        })]
    );
    state.deletes.lock().unwrap().clear();

    for bookmarks in [json!({"bookmarks": []}), json!({"error": "InvalidToken"})] {
        *state.bookmarks.lock().unwrap() = bookmarks;
        let output = invoke_command(directory.path(), &["/deleteLikes", "true"], "yes\n");
        assert!(!output.contains("Type 'yes'"));
        assert!(state.deletes.lock().unwrap().is_empty());
    }
    server.abort();
}
