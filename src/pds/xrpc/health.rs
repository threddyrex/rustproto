//! Health endpoint - health check for monitoring.
//!
//! Returns version information for the PDS server.
//! Looks for a code-rev.txt file in the pds data directory, otherwise
//! returns a default version.

use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use serde::Serialize;

use crate::pds::db::StatisticKey;
use crate::pds::server::PdsState;

/// Health response structure.
#[derive(Serialize)]
struct HealthResponse {
    version: String,
}

/// GET /xrpc/_health - Health check endpoint.
///
/// Returns the current version of the PDS server.
/// Checks for a code-rev.txt file in the pds data directory first,
/// falling back to a default version if not found.
pub async fn health(State(state): State<Arc<PdsState>>) -> impl IntoResponse {
    // Increment statistics
    let stat_key = StatisticKey {
        name: "xrpc/_health".to_string(),
        ip_address: "global".to_string(),
        user_agent: "unknown".to_string(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Check for code-rev.txt file
    let code_rev_path = state.lfs.get_data_dir().join("pds").join("code-rev.txt");
    
    let version = if code_rev_path.exists() {
        match std::fs::read_to_string(&code_rev_path) {
            Ok(content) => {
                let rev = content.trim();
                if !rev.is_empty() {
                    format!("rustproto {}", rev)
                } else {
                    "rustproto 0.0.001".to_string()
                }
            }
            Err(_) => "rustproto 0.0.001".to_string(),
        }
    } else {
        "rustproto 0.0.001".to_string()
    };

    Json(HealthResponse { version })
}
