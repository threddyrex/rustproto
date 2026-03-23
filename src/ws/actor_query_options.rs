//! Options for controlling actor information resolution.

/// Options for querying actor information.
///
/// By default, it enables Bluesky API for handle resolution and DID document
/// resolution. The caller can turn on/off any options as needed.
///
#[derive(Debug, Clone)]
pub struct ActorQueryOptions {
    /// Enable all resolution methods.
    pub all: bool,

    /// Resolve handle via Bluesky public API.
    /// Default: true
    pub resolve_handle_via_bluesky: bool,

    /// Resolve handle via DNS TXT record (_atproto.{handle}).
    /// Default: false
    pub resolve_handle_via_dns: bool,

    /// Resolve handle via HTTP (.well-known/atproto-did).
    /// Default: false
    pub resolve_handle_via_http: bool,

    /// Resolve DID to DID document.
    /// Default: true
    pub resolve_did_doc: bool,
}

impl Default for ActorQueryOptions {
    fn default() -> Self {
        Self {
            all: false,
            resolve_handle_via_bluesky: true,
            resolve_handle_via_dns: false,
            resolve_handle_via_http: false,
            resolve_did_doc: true,
        }
    }
}

impl ActorQueryOptions {
    /// Creates options with all resolution methods enabled.
    pub fn all() -> Self {
        Self {
            all: true,
            resolve_handle_via_bluesky: true,
            resolve_handle_via_dns: true,
            resolve_handle_via_http: true,
            resolve_did_doc: true,
        }
    }

    /// Creates minimal options (Bluesky API only, no DID doc).
    pub fn minimal() -> Self {
        Self {
            all: false,
            resolve_handle_via_bluesky: true,
            resolve_handle_via_dns: false,
            resolve_handle_via_http: false,
            resolve_did_doc: false,
        }
    }

    /// Builder method to set DNS resolution.
    pub fn with_dns(mut self, enabled: bool) -> Self {
        self.resolve_handle_via_dns = enabled;
        self
    }

    /// Builder method to set HTTP resolution.
    pub fn with_http(mut self, enabled: bool) -> Self {
        self.resolve_handle_via_http = enabled;
        self
    }

    /// Builder method to set Bluesky API resolution.
    pub fn with_bluesky(mut self, enabled: bool) -> Self {
        self.resolve_handle_via_bluesky = enabled;
        self
    }

    /// Builder method to set DID document resolution.
    pub fn with_did_doc(mut self, enabled: bool) -> Self {
        self.resolve_did_doc = enabled;
        self
    }

    /// Returns true if Bluesky API resolution should be used.
    pub fn should_resolve_via_bluesky(&self) -> bool {
        self.all || self.resolve_handle_via_bluesky
    }

    /// Returns true if DNS resolution should be used.
    pub fn should_resolve_via_dns(&self) -> bool {
        self.all || self.resolve_handle_via_dns
    }

    /// Returns true if HTTP resolution should be used.
    pub fn should_resolve_via_http(&self) -> bool {
        self.all || self.resolve_handle_via_http
    }

    /// Returns true if DID document should be resolved.
    pub fn should_resolve_did_doc(&self) -> bool {
        self.all || self.resolve_did_doc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = ActorQueryOptions::default();
        assert!(opts.should_resolve_via_bluesky());
        assert!(!opts.should_resolve_via_dns());
        assert!(!opts.should_resolve_via_http());
        assert!(opts.should_resolve_did_doc());
    }

    #[test]
    fn test_all_options() {
        let opts = ActorQueryOptions::all();
        assert!(opts.should_resolve_via_bluesky());
        assert!(opts.should_resolve_via_dns());
        assert!(opts.should_resolve_via_http());
        assert!(opts.should_resolve_did_doc());
    }

    #[test]
    fn test_builder() {
        let opts = ActorQueryOptions::default()
            .with_dns(true)
            .with_http(true);
        assert!(opts.should_resolve_via_dns());
        assert!(opts.should_resolve_via_http());
    }
}
