//! Bluesky client for resolving actor information.
//!
//! This module provides functionality to resolve handles to DIDs,
//! fetch DID documents, and extract PDS endpoints.

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
            info.handle = Some(actor.to_string());

            // Try different resolution methods
            if options.should_resolve_via_bluesky() {
                info.did_bsky = self.resolve_handle_to_did_via_bluesky(actor).await.ok();
            }

            if options.should_resolve_via_dns() {
                info.did_dns = self.resolve_handle_to_did_via_dns(actor).await.ok();
            }

            if options.should_resolve_via_http() {
                info.did_http = self.resolve_handle_to_did_via_http(actor).await.ok();
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
            _ => return Ok(info),
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
}
