//! com.atproto.sync.subscribeRepos endpoint.
//!
//! WebSocket firehose for streaming repository events to subscribers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use axum::{
    extract::{ConnectInfo, Query, State, WebSocketUpgrade, ws::{Message, WebSocket}},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::time::{Duration, interval};

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;
use crate::pds::xrpc::auth_helpers::get_caller_info;

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

/// Tracks sequence numbers that cause rapid disconnects.
/// Key: sequence_number, Value: (disconnect_count, last_disconnect_time)
static TOXIC_EVENT_TRACKER: std::sync::OnceLock<Mutex<HashMap<i64, (u32, std::time::Instant)>>> = std::sync::OnceLock::new();

/// Number of rapid disconnects before an event is auto-retired.
const TOXIC_DISCONNECT_THRESHOLD: u32 = 3;

/// Time window for counting rapid disconnects (seconds).
const TOXIC_TIME_WINDOW_SECS: u64 = 60;

/// Maximum time after receiving an event to consider the disconnect "rapid".
const RAPID_DISCONNECT_SECS: u64 = 5;

fn get_toxic_tracker() -> &'static Mutex<HashMap<i64, (u32, std::time::Instant)>> {
    TOXIC_EVENT_TRACKER.get_or_init(|| Mutex::new(HashMap::new()))
}

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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<SubscribeReposQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.subscribeRepos".to_string(),
        ip_address,
        user_agent,
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

    // Track last sent event for toxic detection
    let mut last_sent_seq: Option<i64> = None;
    let mut last_send_time: Option<std::time::Instant> = None;

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

                    // Track this event for toxic detection
                    last_sent_seq = Some(event.sequence_number);
                    last_send_time = Some(std::time::Instant::now());

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

    // Check if this was a rapid disconnect after receiving an event
    if let (Some(seq), Some(send_time)) = (last_sent_seq, last_send_time) {
        let elapsed = send_time.elapsed().as_secs();
        if elapsed < RAPID_DISCONNECT_SECS {
            state.log.warning(&format!(
                "[FIREHOSE] [RAPID_DISCONNECT] seq={} elapsed={}s - potential toxic event",
                seq, elapsed
            ));
            
            // Track this as a potentially toxic event
            check_and_retire_toxic_event(&state, seq).await;
        }
    }

    // Decrement subscriber count
    SUBSCRIBER_COUNT.fetch_sub(1, Ordering::SeqCst);
    state.log.trace("[FIREHOSE] WebSocket client disconnected");
}

/// Check if an event has become toxic (too many rapid disconnects) and retire it.
async fn check_and_retire_toxic_event(state: &Arc<PdsState>, sequence_number: i64) {
    let tracker = get_toxic_tracker();
    let mut map = tracker.lock().await;
    
    let now = std::time::Instant::now();
    
    // Clean up old entries outside the time window
    map.retain(|_, (_, last_time)| {
        now.duration_since(*last_time).as_secs() < TOXIC_TIME_WINDOW_SECS
    });
    
    // Increment counter for this sequence number
    let entry = map.entry(sequence_number).or_insert((0, now));
    entry.0 += 1;
    entry.1 = now;
    
    let count = entry.0;
    
    if count >= TOXIC_DISCONNECT_THRESHOLD {
        state.log.error(&format!(
            "[FIREHOSE] [TOXIC_EVENT_RETIRED] seq={} disconnect_count={} - auto-hiding event",
            sequence_number, count
        ));
        
        // Hide the toxic event so it won't be sent again
        if let Err(e) = state.db.hide_firehose_event(sequence_number) {
            state.log.error(&format!(
                "[FIREHOSE] Failed to hide toxic event {}: {}",
                sequence_number, e
            ));
        } else {
            state.log.info(&format!(
                "[FIREHOSE] Successfully retired toxic event seq={}",
                sequence_number
            ));

            // Increment statistic for toxic events hidden
            let stat_key = StatisticKey {
                name: "firehose/toxic_event_hidden".to_string(),
                ip_address: "".to_string(),
                user_agent: "".to_string(),
            };
            let _ = state.db.increment_statistic(&stat_key);
        }
        
        // Remove from tracker since it's now hidden
        map.remove(&sequence_number);
    } else {
        state.log.warning(&format!(
            "[FIREHOSE] [TOXIC_TRACKING] seq={} disconnect_count={}/{}",
            sequence_number, count, TOXIC_DISCONNECT_THRESHOLD
        ));
    }
}

/// Handler for when WebSocket upgrade fails (non-WebSocket request).
#[allow(dead_code)]
pub async fn subscribe_repos_non_ws(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    // Get caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&headers, Some(addr));

    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/com.atproto.sync.subscribeRepos".to_string(),
        ip_address,
        user_agent,
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
