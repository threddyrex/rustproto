
use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;



/// Gets PDS info including health, description, and repo list.
pub async fn cmd_get_pds_info(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command GetPdsInfo /actor <handle_or_did>");
            return;
        }
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    // Resolve actor to get PDS
    let info = match client.resolve_actor_info(actor, None).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Error resolving actor: {}", e));
            return;
        }
    };

    let pds = match &info.pds {
        Some(p) => p.clone(),
        None => {
            log.error("Could not resolve PDS for actor");
            return;
        }
    };

    log.info(&format!("PDS: {}", pds));

    // Health
    log.info("");
    log.info("HEALTH");
    match client.pds_health(&pds).await {
        Ok(health) => {
            log.info(&serde_json::to_string_pretty(&health).unwrap_or_default());
        }
        Err(e) => {
            log.error(&format!("Error getting health: {}", e));
        }
    }

    // Describe Server
    log.info("");
    log.info("DESCRIBE SERVER");
    match client.pds_describe_server(&pds).await {
        Ok(desc) => {
            log.info(&serde_json::to_string_pretty(&desc).unwrap_or_default());
        }
        Err(e) => {
            log.error(&format!("Error describing server: {}", e));
        }
    }

    // List Repos
    log.info("");
    log.info("LIST REPOS");
    match client.list_repos(&pds, 100).await {
        Ok(repos) => {
            log.info(&format!("repo count: {}", repos.len()));
            for repo in repos {
                log.info(&serde_json::to_string_pretty(&repo).unwrap_or_default());
            }
        }
        Err(e) => {
            log.error(&format!("Error listing repos: {}", e));
        }
    }
}


