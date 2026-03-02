//! com.atproto.sync.subscribeRepos endpoint.
//!
//! WebSocket firehose for streaming repository events to subscribers.

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use axum::{
    extract::{Query, State, WebSocketUpgrade, ws::{Message, WebSocket}},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::time::{Duration, interval};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

/// Query parameters for subscribeRepos.
#[derive(Deserialize)]
pub struct SubscribeReposQuery {
    /// Optional cursor (sequence number) to start from.
    cursor: Option<String>,
}

/// Error response for subscribeRepos.
#[allow(dead_code)]
#[derive(Serialize)]
pub struct SubscribeReposError {
    error: String,
    message: String,
}

/// Global subscriber count for logging.
static SUBSCRIBER_COUNT: AtomicI32 = AtomicI32::new(0);

/// GET /xrpc/com.atproto.sync.subscribeRepos - WebSocket firehose.
///
/// Establishes a WebSocket connection and streams repository events
/// to the client. Each message is a binary frame containing two
/// DAG-CBOR objects concatenated together:
/// 1. Header - indicates message type (op and t fields)
/// 2. Body - the actual message content
///
/// Reference: https://atproto.com/specs/event-stream
///
/// # Query Parameters
///
/// * `cursor` - Optional sequence number to start streaming from (exclusive)
///
/// # Returns
///
/// * `101 Switching Protocols` on successful WebSocket upgrade
/// * `400 Bad Request` if not a WebSocket request
pub async fn subscribe_repos(
    State(state): State<Arc<PdsState>>,
    Query(query): Query<SubscribeReposQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.subscribeRepos".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Parse cursor from query
    let cursor = query.cursor
        .as_ref()
        .and_then(|c| c.parse::<i64>().ok())
        .unwrap_or_else(|| {
            state.db.get_most_recently_used_sequence_number().unwrap_or(0)
        });

    // Upgrade to WebSocket
    ws.on_upgrade(move |socket| handle_websocket(socket, state, cursor))
}

/// Handle the WebSocket connection.
async fn handle_websocket(socket: WebSocket, state: Arc<PdsState>, mut cursor: i64) {
    // Increment subscriber count
    let sub_count = SUBSCRIBER_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
    state.log.info(&format!(
        "[FIREHOSE] [START] subCount={} cursor={}",
        sub_count, cursor
    ));

    let (mut sender, mut receiver) = socket.split();

    // Create an interval for polling events
    let mut poll_interval = interval(Duration::from_secs(1));

    // Track connection start time
    let start_time = std::time::Instant::now();

    loop {
        tokio::select! {
            // Poll for new events
            _ = poll_interval.tick() => {
                // Get new events since cursor
                let events = match state.db.get_firehose_events_for_subscribe_repos(cursor, 100, 1) {
                    Ok(e) => e,
                    Err(e) => {
                        state.log.error(&format!("[FIREHOSE] Failed to get events: {}", e));
                        continue;
                    }
                };

                for event in events {
                    let lifetime_mins = start_time.elapsed().as_secs_f64() / 60.0;
                    state.log.info(&format!(
                        "[FIREHOSE] [SEND] subCount:{} seq={} cursor={} lifetime={:.1}min",
                        SUBSCRIBER_COUNT.load(Ordering::SeqCst),
                        event.sequence_number,
                        cursor,
                        lifetime_mins
                    ));

                    // Combine header and body bytes
                    let mut combined = Vec::with_capacity(
                        event.header_dag_cbor_bytes.len() + event.body_dag_cbor_bytes.len()
                    );
                    combined.extend_from_slice(&event.header_dag_cbor_bytes);
                    combined.extend_from_slice(&event.body_dag_cbor_bytes);

                    // Send as binary message
                    if let Err(e) = sender.send(Message::Binary(combined.into())).await {
                        state.log.info(&format!("[FIREHOSE] Send error, closing: {}", e));
                        break;
                    }

                    // Update cursor
                    cursor = event.sequence_number;
                }
            }

            // Handle incoming messages (mostly for close/ping)
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) => {
                        state.log.info("[FIREHOSE] Client closed connection");
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        if let Err(e) = sender.send(Message::Pong(data)).await {
                            state.log.info(&format!("[FIREHOSE] Pong error, closing: {}", e));
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        state.log.info(&format!("[FIREHOSE] Receive error: {}", e));
                        break;
                    }
                    None => {
                        state.log.info("[FIREHOSE] Connection closed");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    // Decrement subscriber count
    SUBSCRIBER_COUNT.fetch_sub(1, Ordering::SeqCst);
    state.log.trace("[FIREHOSE] WebSocket client disconnected");
}

/// Handler for when WebSocket upgrade fails (non-WebSocket request).
#[allow(dead_code)]
pub async fn subscribe_repos_non_ws(
    State(state): State<Arc<PdsState>>,
) -> Response {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.subscribeRepos".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    (
        StatusCode::BAD_REQUEST,
        Json(SubscribeReposError {
            error: "InvalidRequest".to_string(),
            message: "WebSocket connection required".to_string(),
        }),
    )
        .into_response()
}
