//! PDS Server module.
//!
//! This module provides the HTTP/HTTPS server implementation for the PDS,
//! using Axum as the web framework.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    extract::{ConnectInfo, Request, State},
    middleware::{self, Next},
    response::Response,
};
use tokio::net::TcpListener;
use tower_cookies::CookieManagerLayer;
use tower_http::cors::{Any, CorsLayer};

use super::admin;
use super::db::{PdsDb, StatisticKey};
use super::xrpc;
use crate::fs::LocalFileSystem;
use crate::log::Logger;

/// Shared state for the PDS server.
///
/// This is passed to all handlers via Axum's state extraction.
pub struct PdsState {
    /// Logger instance.
    pub log: &'static Logger,
    /// Local file system access.
    pub lfs: LocalFileSystem,
    /// PDS database access.
    pub db: PdsDb,
}

/// PDS Server - runs the Personal Data Server HTTP endpoints.
pub struct PdsServer {
    /// Shared state for all handlers.
    state: Arc<PdsState>,
    /// Listen scheme (http or https).
    listen_scheme: String,
    /// Listen host.
    listen_host: String,
    /// Listen port.
    listen_port: i32,
}

impl PdsServer {
    /// Initialize a new PDS server.
    ///
    /// Loads configuration from the database and prepares the server for running.
    ///
    /// # Arguments
    ///
    /// * `lfs` - LocalFileSystem instance
    /// * `log` - Logger instance reference (static lifetime)
    ///
    /// # Returns
    ///
    /// A PdsServer instance ready to run, or an error if initialization fails.
    pub fn initialize(lfs: LocalFileSystem, log: &'static Logger) -> Result<Self, PdsServerError> {
        // Connect to PDS database
        let db = PdsDb::connect(&lfs)?;

        // Load server configuration
        let listen_scheme = db.get_config_property("ServerListenScheme")?;
        let listen_host = db.get_config_property("ServerListenHost")?;
        let listen_port = db.get_config_property_int("ServerListenPort")?;

        log.info(&format!(
            "PDS server initialized: {}://{}:{}",
            listen_scheme, listen_host, listen_port
        ));

        let state = Arc::new(PdsState { log, lfs, db });

        Ok(Self {
            state,
            listen_scheme,
            listen_host,
            listen_port,
        })
    }

    /// Run the PDS server.
    ///
    /// This starts the HTTP server and blocks until shutdown.
    pub async fn run(&self) -> Result<(), PdsServerError> {
        self.state.log.info("");
        self.state.log.info("!! Running PDS !!");
        self.state.log.info("");
        self.state.log.info(&format!(
            "admin: {}://{}:{}/admin/",
            self.listen_scheme, self.listen_host, self.listen_port
        ));
        self.state.log.info("");

        // Build the router
        let app = self.build_router();

        // Create the listener
        let bind_addr = format!("{}:{}", self.listen_host, self.listen_port);
        let listener = TcpListener::bind(&bind_addr).await.map_err(|e| {
            PdsServerError::IoError(format!("Failed to bind to {}: {}", bind_addr, e))
        })?;

        self.state
            .log
            .info(&format!("Listening on {}", bind_addr));

        // Run the server with ConnectInfo to capture client socket addresses
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
            .await
            .map_err(|e| PdsServerError::IoError(format!("Server error: {}", e)))?;

        Ok(())
    }

    /// Build the Axum router with all endpoints.
    fn build_router(&self) -> Router {
        // CORS layer - allow any origin for development
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        // =================================================================
        // XRPC ROUTES - AT Protocol XRPC endpoints.
        // =================================================================
        // =================================================================
        // ADMIN ROUTES - This is the AUTHORITATIVE location for all admin routes.
        // When adding a new admin page, add the route here (not in admin/mod.rs).
        // See admin/mod.rs for the checklist of steps to add a new admin page.
        // =================================================================
        Router::new()
            // XRPC endpoints
            .route("/hello", axum::routing::get(xrpc::hello))
            .route("/xrpc/_health", axum::routing::get(xrpc::health))
            .route("/xrpc/com.atproto.server.describeServer", axum::routing::get(xrpc::describe_server))
            .route("/xrpc/com.atproto.identity.resolveHandle", axum::routing::get(xrpc::resolve_handle))
            // Authentication endpoints
            .route("/xrpc/com.atproto.server.createSession", axum::routing::post(xrpc::create_session))
            .route("/xrpc/com.atproto.server.getSession", axum::routing::get(xrpc::get_session))
            .route("/xrpc/com.atproto.server.refreshSession", axum::routing::post(xrpc::refresh_session))
            .route("/xrpc/com.atproto.server.getServiceAuth", axum::routing::get(xrpc::get_service_auth))
            .route("/xrpc/com.atproto.server.checkAccountStatus", axum::routing::get(xrpc::check_account_status))
            .route("/xrpc/com.atproto.server.activateAccount", axum::routing::post(xrpc::activate_account))
            .route("/xrpc/com.atproto.server.deactivateAccount", axum::routing::post(xrpc::deactivate_account))
            // Repo operation endpoints
            .route("/xrpc/com.atproto.repo.describeRepo", axum::routing::get(xrpc::describe_repo))
            .route("/xrpc/com.atproto.repo.getRecord", axum::routing::get(xrpc::get_record))
            .route("/xrpc/com.atproto.repo.listRecords", axum::routing::get(xrpc::list_records))
            .route("/xrpc/com.atproto.repo.createRecord", axum::routing::post(xrpc::create_record))
            .route("/xrpc/com.atproto.repo.putRecord", axum::routing::post(xrpc::put_record))
            .route("/xrpc/com.atproto.repo.deleteRecord", axum::routing::post(xrpc::delete_record))
            .route("/xrpc/com.atproto.repo.applyWrites", axum::routing::post(xrpc::apply_writes))
            // Blob endpoints
            .route("/xrpc/com.atproto.repo.uploadBlob", axum::routing::post(xrpc::upload_blob))
            .route("/xrpc/com.atproto.sync.listBlobs", axum::routing::get(xrpc::list_blobs))
            .route("/xrpc/com.atproto.sync.getBlob", axum::routing::get(xrpc::get_blob))
            // Admin endpoints
            .route("/admin", axum::routing::get(admin::admin_home))
            .route("/admin/", axum::routing::get(admin::admin_home))
            .route("/admin/login", axum::routing::get(admin::admin_login_get).post(admin::admin_login_post))
            .route("/admin/login/", axum::routing::get(admin::admin_login_get).post(admin::admin_login_post))
            .route("/admin/logout", axum::routing::post(admin::admin_logout))
            .route("/admin/sessions", axum::routing::get(admin::admin_sessions))
            .route("/admin/sessions/", axum::routing::get(admin::admin_sessions))
            .route("/admin/deletelegacysession", axum::routing::post(admin::admin_delete_legacy_session))
            .route("/admin/deleteoauthsession", axum::routing::post(admin::admin_delete_oauth_session))
            .route("/admin/deleteadminsession", axum::routing::post(admin::admin_delete_admin_session))
            .route("/admin/stats", axum::routing::get(admin::admin_stats))
            .route("/admin/stats/", axum::routing::get(admin::admin_stats))
            .route("/admin/deletestatistic", axum::routing::post(admin::admin_delete_statistic))
            .route("/admin/deleteallstatistics", axum::routing::post(admin::admin_delete_all_statistics))
            .route("/admin/deleteoldstatistics", axum::routing::post(admin::admin_delete_old_statistics))
            .route("/admin/config", axum::routing::get(admin::admin_config_get).post(admin::admin_config_post))
            .route("/admin/config/", axum::routing::get(admin::admin_config_get).post(admin::admin_config_post))
            .route("/admin/actions", axum::routing::get(admin::admin_actions_get).post(admin::admin_actions_post))
            .route("/admin/actions/", axum::routing::get(admin::admin_actions_get).post(admin::admin_actions_post))
            .layer(middleware::from_fn_with_state(
                self.state.clone(),
                logging_middleware,
            ))
            .layer(CookieManagerLayer::new())
            .layer(cors)
            .with_state(self.state.clone())
    }
}

/// Logging middleware that logs all HTTP requests and responses.
async fn logging_middleware(
    State(state): State<Arc<PdsState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path().to_string();
    let start = std::time::Instant::now();

    // Extract caller info for statistics
    let (ip_address, user_agent) = get_caller_info(&request, &addr);

    // Log the connection
    state.log.info(&format!(
        "[CONNECT] {} {} {}",
        ip_address, path, user_agent
    ));

    // Increment connection statistics (don't fail on error)
    let stat_key = StatisticKey {
        name: "Connect".to_string(),
        ip_address: ip_address.clone(),
        user_agent: user_agent.clone(),
    };
    let _ = state.db.increment_statistic(&stat_key);

    // Run the next handler
    let response = next.run(request).await;

    let elapsed = start.elapsed();
    let status = response.status();

    state.log.info(&format!(
        "{} {} -> {} ({:.2?})",
        method, path, status.as_u16(), elapsed
    ));

    response
}

/// Extract caller info (IP address and user agent) from request headers.
///
/// IP address is extracted from X-Forwarded-For header if present,
/// otherwise falls back to the socket address.
fn get_caller_info(request: &Request, socket_addr: &SocketAddr) -> (String, String) {
    // Get User-Agent
    let user_agent = request
        .headers()
        .get("User-Agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    // Get IP address from X-Forwarded-For, or fall back to socket address
    let mut ip_address = request
        .headers()
        .get("X-Forwarded-For")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            // X-Forwarded-For can contain multiple IPs, take the first one
            s.split(',').next().unwrap_or(s).trim().to_string()
        })
        .unwrap_or_else(|| socket_addr.ip().to_string());

    // Group uptimerobot requests together (they come from many IPs)
    if user_agent.contains("www.uptimerobot.com") {
        ip_address = "global".to_string();
    }

    (ip_address, user_agent)
}

/// Errors that can occur during PDS server operations.
#[derive(thiserror::Error, Debug)]
pub enum PdsServerError {
    #[error("Database error: {0}")]
    DbError(#[from] super::db::PdsDbError),

    #[error("IO error: {0}")]
    IoError(String),
}
