

use std::collections::HashMap;

use super::{at_uri_to_bsky_url};

use crate::cli::get_arg;
use crate::log::{logger};
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;


/// Gets a post and prints all URIs found in the response.
pub async fn cmd_get_post(args: &HashMap<String, String>) {
    let log = logger();

    let uri = match get_arg(args, "uri") {
        Some(u) => u,
        None => {
            log.error("missing /uri argument");
            log.error("Usage: rustproto /command GetPost /uri <at_uri_or_bsky_url>");
            return;
        }
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

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
fn find_and_print_uris(value: &serde_json::Value, path: &str, log: &crate::log::Logger) {
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



