//! Firehose module for consuming AT Protocol event streams.
//!
//! This module provides functionality to connect to a PDS firehose
//! and process repo events in real-time.
//!
//! Reference: https://atproto.com/specs/event-stream
//!

use std::io::Cursor;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use thiserror::Error;

use crate::repo::DagCborObject;

/// Errors that can occur during firehose operations.
#[derive(Error, Debug)]
pub enum FirehoseError {
    #[error("WebSocket connection failed: {0}")]
    ConnectionFailed(String),

    #[error("WebSocket error: {0}")]
    WebSocketError(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("DAG-CBOR parsing error: {0}")]
    CborParseError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Firehose consumer for AT Protocol event streams.
///
/// Connects to a PDS firehose via WebSocket and processes incoming
/// event frames. Each frame consists of two DAG-CBOR objects:
/// 1. A header indicating the message type
/// 2. The actual message payload
pub struct Firehose;

impl Firehose {
    /// Listens to a firehose and sends messages back to caller.
    ///
    /// Reference: https://atproto.com/specs/event-stream#streaming-wire-protocol-v0
    ///
    /// > "Every WebSocket frame contains two DAG-CBOR objects,
    /// > with bytes concatenated together: a header (indicating message type),
    /// > and the actual message."
    ///
    /// In the second object (message), there is a property called "blocks"
    /// that contains a byte array of records, in repo format.
    /// You can walk this byte array like a repo.
    ///
    /// # Arguments
    ///
    /// * `url` - The WebSocket URL to connect to (e.g., "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos")
    /// * `message_callback` - A callback function that receives the header and body DAG-CBOR objects.
    ///                        Return `true` to continue listening, `false` to stop.
    ///
    pub async fn listen<F>(url: &str, mut message_callback: F) -> Result<(), FirehoseError>
    where
        F: FnMut(&DagCborObject, &DagCborObject) -> bool,
    {
        // Connect to WebSocket
        let (ws_stream, _response) = connect_async(url)
            .await
            .map_err(|e| FirehoseError::ConnectionFailed(e.to_string()))?;

        let (mut _write, mut read) = ws_stream.split();

        // Listen for messages
        while let Some(message_result) = read.next().await {
            let message = message_result?;

            match message {
                Message::Binary(data) => {
                    // Parse the two DAG-CBOR objects from the message
                    let mut cursor = Cursor::new(&data);

                    // First DAG-CBOR object: the header
                    let header = DagCborObject::read_from_stream(&mut cursor).map_err(|e| {
                        FirehoseError::CborParseError(format!("Failed to parse header: {}", e))
                    })?;

                    // Second DAG-CBOR object: the message body
                    let body = DagCborObject::read_from_stream(&mut cursor).map_err(|e| {
                        FirehoseError::CborParseError(format!("Failed to parse body: {}", e))
                    })?;

                    // Call the user's callback
                    let keep_going = message_callback(&header, &body);
                    if !keep_going {
                        break;
                    }
                }
                Message::Close(_) => {
                    // Server closing connection
                    break;
                }
                Message::Ping(data) => {
                    // Respond to ping with pong (handled automatically by tungstenite)
                    let _ = _write.send(Message::Pong(data)).await;
                }
                _ => {
                    // Ignore other message types (Text, Pong, Frame)
                }
            }
        }

        Ok(())
    }
}
