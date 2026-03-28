
use std::collections::HashMap;
use crate::cli::get_arg;
use crate::log::{logger};
use crate::ws::BlueskyClient;
use crate::ws::DEFAULT_APP_VIEW_HOST_NAME;


/// Validate and normalize a PLC-provided PDS endpoint into a safe hostname.
pub fn sanitize_pds_host_for_repo_status(endpoint: &str) -> Result<String, String> {
    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err("PDS endpoint is empty".to_string());
    }

    // Accept both full URLs and bare hostnames from PLC history.
    let endpoint_url = if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("https://{}", endpoint)
    };

    let parsed = reqwest::Url::parse(&endpoint_url)
        .map_err(|e| format!("Invalid PDS endpoint URL '{}': {}", endpoint, e))?;

    let scheme = parsed.scheme().to_ascii_lowercase();
    if scheme != "https" && scheme != "http" {
        return Err(format!("Unsupported URL scheme '{}'", parsed.scheme()));
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("PDS endpoint must not include user info".to_string());
    }

    // We only accept a bare authority (optional trailing slash) from PLC endpoint.
    let path = parsed.path();
    if path != "/" && !path.is_empty() {
        return Err(format!("PDS endpoint must not include path '{}'.", path));
    }

    if parsed.query().is_some() {
        return Err("PDS endpoint must not include query parameters".to_string());
    }

    if parsed.fragment().is_some() {
        return Err("PDS endpoint must not include a URL fragment".to_string());
    }

    if parsed.port().is_some() {
        return Err("PDS endpoint must not include an explicit port".to_string());
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| "PDS endpoint is missing a hostname".to_string())?
        .to_ascii_lowercase();

    if host == "localhost" || host.ends_with(".localhost") {
        return Err("Localhost PDS endpoints are not allowed".to_string());
    }

    if host.parse::<std::net::IpAddr>().is_ok() {
        return Err("IP address PDS endpoints are not allowed".to_string());
    }

    if !is_valid_dns_hostname(&host) {
        return Err(format!("Invalid DNS hostname '{}'.", host));
    }

    Ok(host)
}

fn is_valid_dns_hostname(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }

    let labels: Vec<&str> = host.split('.').collect();
    if labels.iter().any(|label| label.is_empty() || label.len() > 63) {
        return false;
    }

    labels.iter().all(|label| {
        let bytes = label.as_bytes();

        let first = bytes.first().copied().unwrap_or_default();
        let last = bytes.last().copied().unwrap_or_default();
        if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
            return false;
        }

        bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
    })
}

/// Gets PLC history for an actor and checks repo status on each PDS.
pub async fn cmd_get_plc_history(args: &HashMap<String, String>) {
    let log = logger();

    let actor = match get_arg(args, "actor") {
        Some(a) => a,
        None => {
            log.error("missing /actor argument");
            log.error("Usage: rustproto /command GetPlcHistory /actor <handle_or_did>");
            return;
        }
    };

    let client = BlueskyClient::new(DEFAULT_APP_VIEW_HOST_NAME);

    // Resolve actor to get DID
    let info = match client.resolve_actor_info(actor, None).await {
        Ok(info) => info,
        Err(e) => {
            log.error(&format!("Error resolving actor: {}", e));
            return;
        }
    };

    let did = match &info.did {
        Some(d) => d.clone(),
        None => {
            log.error("Could not resolve DID for actor");
            return;
        }
    };

    if did.starts_with("did:web") {
        log.error(&format!("'{}' is a did:web and does not contain plc info.", did));
        return;
    }

    // Get PLC history
    let history = match client.get_plc_history(&did).await {
        Ok(h) => h,
        Err(e) => {
            log.error(&format!("Error getting PLC history: {}", e));
            return;
        }
    };

    // Track PDS status
    let mut pds_status: HashMap<String, String> = HashMap::new();
    pds_status.insert("bsky.social".to_string(), "<na>".to_string());

    let mut console_output: Vec<String> = Vec::new();

    if let Some(entries) = history.as_array() {
        for entry in entries {
            let pds = entry["operation"]["services"]["atproto_pds"]["endpoint"]
                .as_str()
                .map(|s| s.to_string());
            let created_at = entry["createdAt"].as_str();
            let also_known_as = entry["operation"]["alsoKnownAs"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str());

            if let Some(pds_url) = &pds {
                let pds_host = match sanitize_pds_host_for_repo_status(pds_url) {
                    Ok(host) => host,
                    Err(e) => {
                        console_output.push(format!(
                            "{}  pds: {}, handle: {}, active: <invalid-endpoint>, reason: {}",
                            created_at.unwrap_or("<unknown>"),
                            pds_url,
                            also_known_as.unwrap_or("<unknown>"),
                            e
                        ));
                        continue;
                    }
                };

                let repo_status_url = format!(
                    "https://{}/xrpc/com.atproto.sync.getRepoStatus?did={}",
                    pds_host, did
                );

                let active = if !pds_status.contains_key(&pds_host) {
                    log.info(&format!("Repo status URL: {}", repo_status_url));
                    match client.get_repo_status(&pds_host, &did).await {
                        Ok(status) => {
                            let active_val = status["active"]
                                .as_bool()
                                .map(|b| b.to_string())
                                .unwrap_or_else(|| "<null>".to_string());
                            pds_status.insert(pds_host.clone(), active_val.clone());
                            active_val
                        }
                        Err(_) => {
                            pds_status.insert(pds_host.clone(), "<exception>".to_string());
                            "<exception>".to_string()
                        }
                    }
                } else {
                    pds_status.get(&pds_host).cloned().unwrap_or_default()
                };

                console_output.push(format!(
                    "{}  pds: {}, handle: {}, active: {}",
                    created_at.unwrap_or("<unknown>"),
                    pds_url,
                    also_known_as.unwrap_or("<unknown>"),
                    active
                ));
            }
        }
    }

    // Print results
    log.info("");
    log.info(&format!("PDS History for {}:", did));
    for line in &console_output {
        log.info(line);
    }
    log.info("");

    // Check if account is active on multiple PDSs
    let active_pds_count = pds_status.values().filter(|s| s.eq_ignore_ascii_case("true")).count();
    if active_pds_count > 1 {
        log.error(&format!(
            "Account is active on {} PDSs. Expected at most 1.",
            active_pds_count
        ));
        log.info("");
    }
}


