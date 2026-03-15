//! Bluesky client for resolving actor information.
//!
//! This module provides functionality to resolve handles to DIDs,
//! fetch DID documents, and extract PDS endpoints.

use std::time::Instant;

use crate::log::logger;
use crate::ws::{ActorInfo, ActorQueryOptions};
use reqwest::Client;
use serde_json::Value;
use thiserror::Error;

/// Errors that can occur during actor resolution.
#[derive(Error, Debug)]
pub enum BlueskyClientError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("JSON parsing failed: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Invalid actor: {0}")]
    InvalidActor(String),

    #[error("Resolution failed: {0}")]
    ResolutionFailed(String),
}

/// Client for interacting with Bluesky/AT Protocol services.
pub struct BlueskyClient {
    client: Client,
}

impl Default for BlueskyClient {
    fn default() -> Self {
        Self::new()
    }
}

impl BlueskyClient {

    /// Creates a new BlueskyClient with default settings.
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .user_agent("rustproto")
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    /// Creates a new BlueskyClient with a custom reqwest Client.
    pub fn with_client(client: Client) -> Self {
        Self { client }
    }

    /// Resolves actor information for a handle or DID.
    ///
    /// Attempts the following steps:
    /// 1. Resolve handle to DID (dns, http, or bluesky api)
    /// 2. Resolve DID to DID document (did:plc or did:web)
    /// 3. Extract PDS endpoint from DID document
    /// 4. Extract handle from DID document (if not already known)
    /// 5. Extract public key from DID document
    ///
    /// # Arguments
    ///
    /// * `actor` - A handle (e.g., "alice.bsky.social") or DID (e.g., "did:plc:abc123")
    /// * `options` - Optional query options to control resolution behavior
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustproto::ws::BlueskyClient;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = BlueskyClient::new();
    ///     let info = client.resolve_actor_info("alice.bsky.social", None).await.unwrap();
    ///     println!("DID: {:?}", info.did);
    ///     println!("PDS: {:?}", info.pds);
    /// }
    /// ```
    pub async fn resolve_actor_info(
        &self,
        actor: &str,
        options: Option<ActorQueryOptions>,
    ) -> Result<ActorInfo, BlueskyClientError> {
        let start_time = Instant::now();
        let options = options.unwrap_or_default();
        let mut info = ActorInfo::with_actor(actor);

        // Empty actor check
        if actor.is_empty() {
            return Err(BlueskyClientError::InvalidActor(
                "Actor is null or empty".to_string(),
            ));
        }

        // Step 1: Resolve handle to DID
        if actor.starts_with("did:") {
            info.did = Some(actor.to_string());
        } else {
            let normalized_handle = actor.to_ascii_lowercase();
            if !Self::is_valid_handle(&normalized_handle) {
                logger().warning(&format!(
                    "[SECURITY] Rejected invalid handle during actor resolution: {}",
                    actor
                ));
                return Err(BlueskyClientError::InvalidActor(format!(
                    "Invalid handle: {}",
                    actor
                )));
            }

            info.handle = Some(normalized_handle.clone());

            // Try different resolution methods
            if options.should_resolve_via_bluesky() {
                info.did_bsky = self
                    .resolve_handle_to_did_via_bluesky(&normalized_handle)
                    .await
                    .ok();
            }

            if options.should_resolve_via_dns() {
                info.did_dns = self
                    .resolve_handle_to_did_via_dns(&normalized_handle)
                    .await
                    .ok();
            }

            if options.should_resolve_via_http() {
                info.did_http = self
                    .resolve_handle_to_did_via_http(&normalized_handle)
                    .await
                    .ok();
            }

            // Use first successful resolution
            info.did = info
                .did_bsky
                .clone()
                .or_else(|| info.did_dns.clone())
                .or_else(|| info.did_http.clone());
        }

        // Early exit if no DID resolved
        let did = match &info.did {
            Some(d) if d.starts_with("did:") => d.clone(),
            _ => {
                let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
                logger().info(&format!(
                    "[ACTOR] [BSKY] actor={} all={} bsky={} dns={} http={} didDoc={} did=None [{:.2}ms]",
                    actor, options.all, options.resolve_handle_via_bluesky,
                    options.resolve_handle_via_dns, options.resolve_handle_via_http,
                    options.resolve_did_doc, elapsed_ms
                ));
                return Ok(info);
            }
        };

        // Step 2: Resolve DID to DID document
        if options.should_resolve_did_doc() {
            if let Ok(did_doc) = self.resolve_did_to_did_doc(&did).await {
                info.did_doc = Some(did_doc);
            }
        }

        // Step 3: Extract PDS from DID document
        if let Some(ref did_doc) = info.did_doc {
            if let Ok(pds) = Self::extract_pds_from_did_doc(did_doc) {
                info.pds = Some(pds);
            }

            // Step 4: Extract handle from DID document if not known
            if info.handle.is_none() {
                if let Ok(handle) = Self::extract_handle_from_did_doc(did_doc) {
                    info.handle = Some(handle);
                }
            }

            // Step 5: Extract public key from DID document
            if let Ok(pubkey) = Self::extract_public_key_from_did_doc(did_doc) {
                info.public_key_multibase = Some(pubkey);
            }
        }

        // Log the resolution result
        let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;
        let did_doc_length = info.did_doc.as_ref().map(|d| d.len()).unwrap_or(0);
        logger().info(&format!(
            "[ACTOR] [BSKY] actor={} all={} bsky={} dns={} http={} didDoc={} did={} didDocLength={} pds={} [{:.2}ms]",
            actor, options.all, options.resolve_handle_via_bluesky,
            options.resolve_handle_via_dns, options.resolve_handle_via_http,
            options.resolve_did_doc,
            info.did.as_deref().unwrap_or("None"),
            did_doc_length,
            info.pds.as_deref().unwrap_or("None"),
            elapsed_ms
        ));

        Ok(info)
    }

    /// Resolves a handle to a DID using the Bluesky public API.
    ///
    /// Calls `com.atproto.identity.resolveHandle` on the public API.
    pub async fn resolve_handle_to_did_via_bluesky(
        &self,
        handle: &str,
    ) -> Result<String, BlueskyClientError> {
        let url = format!(
            "https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle?handle={}",
            handle
        );

        let response = self.client.get(&url).send().await?;
        let json: Value = response.json().await?;

        json["did"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| BlueskyClientError::ResolutionFailed("No DID in response".to_string()))
    }

    /// Resolves a handle to a DID using DNS TXT records.
    ///
    /// Queries `_atproto.{handle}` TXT record via Cloudflare DNS-over-HTTPS.
    pub async fn resolve_handle_to_did_via_dns(
        &self,
        handle: &str,
    ) -> Result<String, BlueskyClientError> {
        let url = format!(
            "https://cloudflare-dns.com/dns-query?name=_atproto.{}&type=TXT",
            handle
        );

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/dns-json")
            .send()
            .await?;

        let json: Value = response.json().await?;

        // Parse DNS response and look for did= in TXT records
        if let Some(answers) = json["Answer"].as_array() {
            for answer in answers {
                if let Some(data) = answer["data"].as_str() {
                    let data = data.trim_matches('"');
                    if let Some(did) = data.strip_prefix("did=") {
                        return Ok(did.to_string());
                    }
                }
            }
        }

        Err(BlueskyClientError::ResolutionFailed(
            "No DID found in DNS TXT records".to_string(),
        ))
    }

    /// Resolves a handle to a DID using HTTP well-known endpoint.
    ///
    /// Fetches `https://{handle}/.well-known/atproto-did`.
    pub async fn resolve_handle_to_did_via_http(
        &self,
        handle: &str,
    ) -> Result<String, BlueskyClientError> {
        let url = format!("https://{}/.well-known/atproto-did", handle);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        let did = text.trim();
        if did.starts_with("did:") {
            Ok(did.to_string())
        } else {
            Err(BlueskyClientError::ResolutionFailed(
                "Invalid DID in HTTP response".to_string(),
            ))
        }
    }

    /// Resolves a DID to its DID document.
    ///
    /// Supports both did:plc (via plc.directory) and did:web (via .well-known/did.json).
    pub async fn resolve_did_to_did_doc(&self, did: &str) -> Result<String, BlueskyClientError> {
        if did.starts_with("did:plc:") {
            self.resolve_did_to_did_doc_plc(did).await
        } else if did.starts_with("did:web:") {
            self.resolve_did_to_did_doc_web(did).await
        } else {
            Err(BlueskyClientError::InvalidActor(format!(
                "Unsupported DID method: {}",
                did
            )))
        }
    }

    /// Resolves a did:plc to its DID document via plc.directory.
    async fn resolve_did_to_did_doc_plc(&self, did: &str) -> Result<String, BlueskyClientError> {
        let url = format!("https://plc.directory/{}", did);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        Ok(text)
    }

    /// Resolves a did:web to its DID document via .well-known/did.json.
    async fn resolve_did_to_did_doc_web(&self, did: &str) -> Result<String, BlueskyClientError> {
        let hostname = did
            .strip_prefix("did:web:")
            .ok_or_else(|| BlueskyClientError::InvalidActor("Invalid did:web format".to_string()))?;

        let url = format!("https://{}/.well-known/did.json", hostname);

        let response = self.client.get(&url).send().await?;
        let text = response.text().await?;

        Ok(text)
    }

    /// Extracts the PDS endpoint from a DID document.
    ///
    /// Looks for a service entry with type "AtprotoPersonalDataServer".
    pub fn extract_pds_from_did_doc(did_doc: &str) -> Result<String, BlueskyClientError> {
        let doc: Value = serde_json::from_str(did_doc)?;

        if let Some(services) = doc["service"].as_array() {
            for service in services {
                if service["type"].as_str() == Some("AtprotoPersonalDataServer") {
                    if let Some(endpoint) = service["serviceEndpoint"].as_str() {
                        let pds = endpoint
                            .trim_start_matches("https://")
                            .trim_start_matches("http://");
                        return Ok(pds.to_string());
                    }
                }
            }
        }

        Err(BlueskyClientError::ResolutionFailed(
            "No PDS found in DID document".to_string(),
        ))
    }

    /// Extracts the handle from a DID document.
    ///
    /// Looks for the first entry in "alsoKnownAs" with at:// prefix.
    pub fn extract_handle_from_did_doc(did_doc: &str) -> Result<String, BlueskyClientError> {
        let doc: Value = serde_json::from_str(did_doc)?;

        if let Some(aliases) = doc["alsoKnownAs"].as_array() {
            if let Some(first) = aliases.first() {
                if let Some(uri) = first.as_str() {
                    let handle = uri.trim_start_matches("at://").split('/').next();
                    if let Some(h) = handle {
                        return Ok(h.to_string());
                    }
                }
            }
        }

        Err(BlueskyClientError::ResolutionFailed(
            "No handle found in DID document".to_string(),
        ))
    }

    /// Extracts the public key (multibase) from a DID document.
    ///
    /// Looks for a verification method with id ending in "#atproto".
    pub fn extract_public_key_from_did_doc(did_doc: &str) -> Result<String, BlueskyClientError> {
        let doc: Value = serde_json::from_str(did_doc)?;

        if let Some(methods) = doc["verificationMethod"].as_array() {
            for method in methods {
                if let Some(id) = method["id"].as_str() {
                    if id.ends_with("#atproto") {
                        if let Some(pubkey) = method["publicKeyMultibase"].as_str() {
                            return Ok(pubkey.to_string());
                        }
                    }
                }
            }
        }

        Err(BlueskyClientError::ResolutionFailed(
            "No public key found in DID document".to_string(),
        ))
    }


    /// Validates whether a string is a syntactically valid ATProto handle.
    ///
    /// This follows the handle syntax rules from the ATProto specification:
    /// ASCII only, dot-separated labels, 2+ labels, per-label charset/length
    /// constraints, and top-level label must not start with a digit.
    pub fn is_valid_handle(handle: &str) -> bool {
        if handle.is_empty() || !handle.is_ascii() || handle.len() > 253 {
            return false;
        }

        if handle.starts_with('.') || handle.ends_with('.') {
            return false;
        }

        let labels: Vec<&str> = handle.split('.').collect();
        if labels.len() < 2 {
            return false;
        }

        for label in &labels {
            if label.is_empty() || label.len() > 63 {
                return false;
            }

            if label.starts_with('-') || label.ends_with('-') {
                return false;
            }

            if !label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            {
                return false;
            }
        }

        if labels
            .last()
            .and_then(|tld| tld.as_bytes().first())
            .is_some_and(u8::is_ascii_digit)
        {
            return false;
        }

        true
    }


    /// Gets the PLC audit log (history) for a DID.
    ///
    /// Calls `https://plc.directory/{did}/log/audit`.
    pub async fn get_plc_history(&self, did: &str) -> Result<Value, BlueskyClientError> {
        if !did.starts_with("did:plc:") {
            return Err(BlueskyClientError::InvalidActor(format!(
                "'{}' is not a did:plc",
                did
            )));
        }

        let url = format!("https://plc.directory/{}/log/audit", did);
        let response = self.client.get(&url).send().await?;
        let json: Value = response.json().await?;
        Ok(json)
    }

    /// Gets the repo status for a DID from a PDS.
    ///
    /// Calls `com.atproto.sync.getRepoStatus` on the PDS.
    pub async fn get_repo_status(
        &self,
        pds: &str,
        did: &str,
    ) -> Result<Value, BlueskyClientError> {
        let url = format!(
            "https://{}/xrpc/com.atproto.sync.getRepoStatus?did={}",
            pds, did
        );
        let response = self.client.get(&url).send().await?;
        let json: Value = response.json().await?;
        Ok(json)
    }

    /// Gets health status for a PDS.
    ///
    /// Calls `_health` on the PDS.
    pub async fn pds_health(&self, pds: &str) -> Result<Value, BlueskyClientError> {
        let url = format!("https://{}/xrpc/_health", pds);
        let response = self.client.get(&url).send().await?;
        let json: Value = response.json().await?;
        Ok(json)
    }

    /// Gets server description for a PDS.
    ///
    /// Calls `com.atproto.server.describeServer` on the PDS.
    pub async fn pds_describe_server(&self, pds: &str) -> Result<Value, BlueskyClientError> {
        let url = format!("https://{}/xrpc/com.atproto.server.describeServer", pds);
        let response = self.client.get(&url).send().await?;
        let json: Value = response.json().await?;
        Ok(json)
    }

    /// Lists repos on a PDS.
    ///
    /// Calls `com.atproto.sync.listRepos` on the PDS.
    pub async fn list_repos(&self, pds: &str, limit: u32) -> Result<Vec<Value>, BlueskyClientError> {
        let mut repos = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let url = match &cursor {
                Some(c) => format!(
                    "https://{}/xrpc/com.atproto.sync.listRepos?limit={}&cursor={}",
                    pds, limit, c
                ),
                None => format!(
                    "https://{}/xrpc/com.atproto.sync.listRepos?limit={}",
                    pds, limit
                ),
            };

            let response = self.client.get(&url).send().await?;
            let json: Value = response.json().await?;

            if let Some(repos_array) = json["repos"].as_array() {
                for repo in repos_array {
                    repos.push(repo.clone());
                }
            }

            cursor = json["cursor"].as_str().map(|s| s.to_string());
            if cursor.is_none() {
                break;
            }

            // Small delay between requests
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(repos)
    }

    /// Gets posts by URI.
    ///
    /// Calls `app.bsky.feed.getPosts` on the public API.
    pub async fn get_posts(&self, uris: &[&str]) -> Result<Value, BlueskyClientError> {
        let uris_param = uris.join(",");
        let url = format!(
            "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts?uris={}",
            uris_param
        );
        let response = self.client.get(&url).send().await?;
        let json: Value = response.json().await?;
        Ok(json)
    }

    /// Downloads a repository (CAR file) for the given DID from a PDS.
    ///
    /// Calls `com.atproto.sync.getRepo` on the PDS.
    ///
    /// # Arguments
    ///
    /// * `pds` - The PDS hostname (e.g., "bsky.social")
    /// * `did` - The DID to fetch the repo for
    /// * `output_path` - Path to write the CAR file to
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use rustproto::ws::BlueskyClient;
    /// use std::path::Path;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = BlueskyClient::new();
    ///     client.get_repo(
    ///         "bsky.social",
    ///         "did:plc:abc123",
    ///         Path::new("./repo.car")
    ///     ).await.unwrap();
    /// }
    /// ```
    pub async fn get_repo(
        &self,
        pds: &str,
        did: &str,
        output_path: &std::path::Path,
    ) -> Result<u64, BlueskyClientError> {
        use tokio::io::AsyncWriteExt;

        if pds.is_empty() || did.is_empty() {
            return Err(BlueskyClientError::InvalidActor(
                "PDS and DID are required".to_string(),
            ));
        }

        let url = format!(
            "https://{}/xrpc/com.atproto.sync.getRepo?did={}",
            pds, did
        );

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(BlueskyClientError::ResolutionFailed(format!(
                "HTTP {} from PDS",
                response.status()
            )));
        }

        // Stream the response to a file
        let bytes = response.bytes().await?;
        let bytes_written = bytes.len() as u64;

        let mut file = tokio::fs::File::create(output_path).await.map_err(|e| {
            BlueskyClientError::ResolutionFailed(format!("Failed to create output file: {}", e))
        })?;

        file.write_all(&bytes).await.map_err(|e| {
            BlueskyClientError::ResolutionFailed(format!("Failed to write to file: {}", e))
        })?;

        Ok(bytes_written)
    }

    /// Lists all blob CIDs for the given DID from a PDS.
    ///
    /// Calls `com.atproto.sync.listBlobs` on the PDS, paging through results.
    pub async fn list_blobs(
        &self,
        pds: &str,
        did: &str,
    ) -> Result<Vec<String>, BlueskyClientError> {
        let log = logger();
        let mut blobs = Vec::new();
        let mut cursor: Option<String> = None;
        let limit = 100;

        loop {
            let url = match &cursor {
                Some(c) => format!(
                    "https://{}/xrpc/com.atproto.sync.listBlobs?did={}&limit={}&cursor={}",
                    pds, did, limit, c
                ),
                None => format!(
                    "https://{}/xrpc/com.atproto.sync.listBlobs?did={}&limit={}",
                    pds, did, limit
                ),
            };

            log.trace(&format!("ListBlobs: url: {}", url));

            let response = self.client.get(&url).send().await?;
            let json: Value = response.json().await?;

            let cids = json["cids"].as_array();
            let cid_count = cids.map(|c| c.len()).unwrap_or(0);

            if let Some(cids) = cids {
                for cid in cids {
                    if let Some(s) = cid.as_str() {
                        blobs.push(s.to_string());
                    }
                }
            }

            cursor = json["cursor"].as_str().map(|s| s.to_string());
            log.trace(&format!("ListBlobs: count={}, cursor={:?}", cid_count, cursor));

            if cid_count < limit || cursor.is_none() {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        Ok(blobs)
    }

    /// Downloads a blob from a PDS and saves it to a file.
    /// Also writes a `.metadata.json` file alongside the blob containing
    /// the HTTP status code, content type, and content length.
    ///
    /// Calls `com.atproto.sync.getBlob` on the PDS.
    pub async fn get_blob(
        &self,
        pds: &str,
        did: &str,
        cid: &str,
        output_path: &std::path::Path,
    ) -> Result<u64, BlueskyClientError> {
        use tokio::io::AsyncWriteExt;

        let url = format!(
            "https://{}/xrpc/com.atproto.sync.getBlob?did={}&cid={}",
            pds, did, cid
        );

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(BlueskyClientError::ResolutionFailed(format!(
                "HTTP {} getting blob {}",
                response.status(),
                cid
            )));
        }

        let status_code = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let content_length = response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        let bytes = response.bytes().await?;
        let bytes_written = bytes.len() as u64;

        let mut file = tokio::fs::File::create(output_path).await.map_err(|e| {
            BlueskyClientError::ResolutionFailed(format!("Failed to create blob file: {}", e))
        })?;

        file.write_all(&bytes).await.map_err(|e| {
            BlueskyClientError::ResolutionFailed(format!("Failed to write blob file: {}", e))
        })?;

        // Write metadata file alongside the blob
        let metadata_path = {
            let mut p = output_path.as_os_str().to_os_string();
            p.push(".metadata.json");
            std::path::PathBuf::from(p)
        };

        let metadata = serde_json::json!({
            "statusCode": status_code,
            "contentType": content_type,
            "contentLength": content_length,
        });

        let metadata_str = serde_json::to_string_pretty(&metadata).map_err(|e| {
            BlueskyClientError::ResolutionFailed(format!("Failed to serialize metadata: {}", e))
        })?;

        tokio::fs::write(&metadata_path, metadata_str).await.map_err(|e| {
            BlueskyClientError::ResolutionFailed(format!("Failed to write metadata file: {}", e))
        })?;

        Ok(bytes_written)
    }

    /// Gets user preferences from a PDS (requires authentication).
    ///
    /// Calls `app.bsky.actor.getPreferences` on the PDS.
    pub async fn get_preferences(
        &self,
        pds: &str,
        access_jwt: &str,
    ) -> Result<Value, BlueskyClientError> {
        let url = format!(
            "https://{}/xrpc/app.bsky.actor.getPreferences",
            pds
        );

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", access_jwt))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(BlueskyClientError::ResolutionFailed(format!(
                "HTTP {} getting preferences",
                response.status()
            )));
        }

        let json: Value = response.json().await?;
        Ok(json)
    }

    /// Creates a session (logs in) on a PDS.
    ///
    /// Calls `com.atproto.server.createSession` on the PDS.
    pub async fn create_session(
        &self,
        pds: &str,
        identifier: &str,
        password: &str,
        auth_factor_token: Option<&str>,
    ) -> Result<Value, BlueskyClientError> {
        let url = format!(
            "https://{}/xrpc/com.atproto.server.createSession",
            pds
        );

        let body = if let Some(token) = auth_factor_token {
            serde_json::json!({
                "identifier": identifier,
                "password": password,
                "authFactorToken": token
            })
        } else {
            serde_json::json!({
                "identifier": identifier,
                "password": password
            })
        };

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(BlueskyClientError::ResolutionFailed(format!(
                "HTTP {} creating session",
                response.status()
            )));
        }

        let json: Value = response.json().await?;
        Ok(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_did_doc() -> &'static str {
        r##"{"id":"did:plc:abc123","alsoKnownAs":["at://alice.bsky.social"],"verificationMethod":[{"id":"did:plc:abc123#atproto","type":"Multikey","publicKeyMultibase":"zDnaekGxj2Fz4Cdf"}],"service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":"https://bsky.social"}]}"##
    }

    #[test]
    fn test_extract_pds() {
        let pds = BlueskyClient::extract_pds_from_did_doc(sample_did_doc()).unwrap();
        assert_eq!(pds, "bsky.social");
    }

    #[test]
    fn test_extract_handle() {
        let handle = BlueskyClient::extract_handle_from_did_doc(sample_did_doc()).unwrap();
        assert_eq!(handle, "alice.bsky.social");
    }

    #[test]
    fn test_extract_public_key() {
        let pubkey = BlueskyClient::extract_public_key_from_did_doc(sample_did_doc()).unwrap();
        assert_eq!(pubkey, "zDnaekGxj2Fz4Cdf");
    }

    #[test]
    fn test_valid_handles() {
        assert!(BlueskyClient::is_valid_handle("alice.bsky.social"));
        assert!(BlueskyClient::is_valid_handle("Alice.BSKY.Social"));
        assert!(BlueskyClient::is_valid_handle("foo-bar.example"));
    }

    #[test]
    fn test_invalid_handles() {
        assert!(!BlueskyClient::is_valid_handle(""));
        assert!(!BlueskyClient::is_valid_handle("localhost"));
        assert!(!BlueskyClient::is_valid_handle(".example.com"));
        assert!(!BlueskyClient::is_valid_handle("example.com."));
        assert!(!BlueskyClient::is_valid_handle("-foo.example"));
        assert!(!BlueskyClient::is_valid_handle("foo-.example"));
        assert!(!BlueskyClient::is_valid_handle("foo._example.com"));
        assert!(!BlueskyClient::is_valid_handle("foo.123"));
        assert!(!BlueskyClient::is_valid_handle("foo.exa mple.com"));
    }
}
