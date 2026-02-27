//! PDS Server module.
//!
//! This module provides the HTTP/HTTPS server implementation for the PDS,
//! using Axum as the web framework.

use std::sync::Arc;

use axum::{
    Router,
    extract::{Request, State},
    middleware::{self, Next},
    response::Response,
};
use tokio::net::TcpListener;
use tower_cookies::CookieManagerLayer;
use tower_http::cors::{Any, CorsLayer};

use super::admin;
use super::db::PdsDb;
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

        // Run the server
        axum::serve(listener, app)
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

        // Admin routes - define at top level to avoid nest trailing slash issues
        Router::new()
            .route("/hello", axum::routing::get(|| async { "rustproto PDS is running" }))
            .route("/admin", axum::routing::get(admin::admin_home))
            .route("/admin/", axum::routing::get(admin::admin_home))
            .route("/admin/login", axum::routing::get(admin::admin_login_get).post(admin::admin_login_post))
            .route("/admin/login/", axum::routing::get(admin::admin_login_get).post(admin::admin_login_post))
            .route("/admin/logout", axum::routing::post(admin::admin_logout))
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
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let start = std::time::Instant::now();

    // Run the next handler
    let response = next.run(request).await;

    let elapsed = start.elapsed();
    let status = response.status();

    state.log.info(&format!(
        "{} {} -> {} ({:.2?})",
        method, uri, status.as_u16(), elapsed
    ));

    response
}

/// Errors that can occur during PDS server operations.
#[derive(thiserror::Error, Debug)]
pub enum PdsServerError {
    #[error("Database error: {0}")]
    DbError(#[from] super::db::PdsDbError),

    #[error("IO error: {0}")]
    IoError(String),
}
