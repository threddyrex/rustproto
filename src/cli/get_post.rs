//! get post and print all URIs found in the response

use std::collections::HashMap;

use super::{at_uri_to_bsky_url};

use crate::cli::get_arg;
use crate::log::{logger};
use crate::uri::{AtUri};
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;


/// Gets a post and prints all URIs found in the response.
pub async fn cmd_get_post(args: &HashMap<String, String>) {
    let log = logger();
    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    // get uri arg
    let uri_arg = match get_arg(args, "uri") {
        Some(u) => u,
        None => {
            log.error("missing /uri argument");
            log.error("Usage: rustproto /command GetPost /uri <at_uri_or_bsky_url>");
            return;
        }
    };

    log.info(&format!("uri_arg: {}", uri_arg));


    // Parse to AtUri struct
    let at_uri = match AtUri::from_bsky_post_url(uri_arg)
    {
        Some(a) => a,
        None => {
            match AtUri::from_at_uri(uri_arg) {
                Some(a) => a,
                None => {
                    log.error("Invalid URI format. Expected a Bluesky post URL like 'https://bsky.app/profile/{did or handle}/post/{rkey}' or an AT URI like 'at://{authority}/{collection}/{rkey}'");
                    return;
                }
            }
        }
    };

    log.info(&format!("Parsed AT URI: {:?}", at_uri));


    // Need to convert authority to DID if it's a handle
    let did = if at_uri.authority.starts_with("did:") {
        at_uri.authority.clone()
    } else {
        // Resolve handle to DID using the BlueskyClient
        match client.resolve_actor_info(&at_uri.authority, None).await {
            Ok(info) => {
                match info.did {
                    Some(d) => d,
                    None => {
                        log.error(&format!("Could not resolve DID for handle: {}", at_uri.authority));
                        return;
                    }
                }
            }
            Err(e) => {
                log.error(&format!("Error resolving handle to DID: {}", e));
                return;
            }
        }
    };

    // get at uri string
    let at_uri_new = AtUri::new(&did, &at_uri.collection, &at_uri.rkey);
    let at_uri_str = at_uri_new.to_at_uri();
    log.info(&format!("at_uri_str: {}", at_uri_str));

    // Get posts
    match client.get_posts(&[&at_uri_str]).await {
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



