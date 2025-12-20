//! # Configuration
//!
//! This module defines the configuration structure for workplane agents.
//! Configuration values can be set via environment variables or command-line arguments.
//!
//! # Example
//!
//! ```rust,ignore
//! use workplane::Config;
//!
//! let mut config = Config {
//!     namespace: "default".to_string(),
//!     workload_name: "my-app".to_string(),
//!     pod_name: "my-app-abc123".to_string(),
//!     // ... other fields
//! };
//! config.apply_defaults();
//! ```

use std::time::Duration;

/// Configuration for a workplane agent instance.
#[derive(Clone, Debug)]
pub struct Config {
    /// Optional peer ID string (for debugging)
    pub peer_id_str: Option<String>,

    /// Private key bytes for cryptographic identity
    pub private_key: Vec<u8>,

    /// Kubernetes namespace
    pub namespace: String,

    /// Name of the workload (Deployment, StatefulSet, etc.)
    pub workload_name: String,

    /// Name of this pod instance
    pub pod_name: String,

    /// Desired number of replicas for the workload
    pub replicas: usize,

    /// Workload kind (Deployment, StatefulSet, DaemonSet)
    pub workload_kind: String,

    /// HTTP endpoint for liveness probes (empty = disabled)
    pub liveness_url: String,

    /// HTTP endpoint for readiness probes (empty = disabled)
    pub readiness_url: String,

    /// List of bootstrap peer addresses for mesh discovery
    pub bootstrap_peer_strings: Vec<String>,

    /// URL of the machineplane API
    pub magik_api: String,

    /// How often to check and reconcile replica count
    pub replica_check_interval: Duration,

    /// Time-to-live for DHT records
    pub dht_ttl: Duration,

    /// How often to send DHT heartbeat announcements
    pub heartbeat_interval: Duration,

    /// How often to run health probes
    pub health_probe_interval: Duration,

    /// Timeout for health probe HTTP requests
    pub health_probe_timeout: Duration,

    /// Whether to allow cross-namespace service discovery
    pub allow_cross_namespace: bool,

    /// Workload name patterns to allow (empty = allow all)
    pub allowed_workloads: Vec<String>,

    /// Workload name patterns to deny
    pub denied_workloads: Vec<String>,

    /// Addresses to listen on for mesh connections
    pub listen_addrs: Vec<String>,

    /// Command to execute as a child process (init supervisor mode).
    /// When set, workplane runs as PID 1 and spawns this command.
    pub exec_command: Option<String>,
}

impl Config {
    /// Returns true if this is a stateful workload (StatefulSet).
    ///
    /// Stateful workloads have different scheduling and identity semantics.
    pub fn is_stateful(&self) -> bool {
        let kind = self.workload_kind.trim();
        kind.eq_ignore_ascii_case("StatefulSet") || kind.eq_ignore_ascii_case("StatefulWorkload")
    }

    /// Returns the canonical task kind for the workload.
    ///
    /// Maps various workload kinds to standardized internal types:
    /// - Deployment, StatelessWorkload → "StatelessWorkload"
    /// - StatefulSet, StatefulWorkload → "StatefulWorkload"
    /// - DaemonSet → "DaemonSet"
    /// - Other → "CustomWorkload"
    pub fn task_kind(&self) -> &str {
        let kind = self.workload_kind.trim();
        if kind.eq_ignore_ascii_case("Deployment") || kind.eq_ignore_ascii_case("StatelessWorkload")
        {
            "StatelessWorkload"
        } else if kind.eq_ignore_ascii_case("StatefulSet")
            || kind.eq_ignore_ascii_case("StatefulWorkload")
        {
            "StatefulWorkload"
        } else if kind.eq_ignore_ascii_case("DaemonSet") {
            "DaemonSet"
        } else {
            "CustomWorkload"
        }
    }

    /// Applies sensible defaults for any unset or zero values.
    ///
    /// Should be called after loading configuration from CLI or environment.
    pub fn apply_defaults(&mut self) {
        if self.namespace.is_empty() {
            self.namespace = "default".to_string();
        }
        if self.replicas == 0 {
            self.replicas = 1;
        }
        if self.workload_kind.is_empty() {
            self.workload_kind = "Deployment".to_string();
        }
        if self.magik_api.is_empty() {
            self.magik_api = "http://localhost:8080".to_string();
        }

        if self.replica_check_interval == Duration::from_secs(0) {
            self.replica_check_interval = Duration::from_secs(30);
        }
        if self.dht_ttl == Duration::from_secs(0) {
            self.dht_ttl = Duration::from_secs(15);
        }
        if self.heartbeat_interval == Duration::from_secs(0) {
            self.heartbeat_interval = Duration::from_secs(5);
        }
        if self.health_probe_interval == Duration::from_secs(0) {
            self.health_probe_interval = Duration::from_secs(10);
        }
        if self.health_probe_timeout == Duration::from_secs(0) {
            self.health_probe_timeout = Duration::from_secs(5);
        }
        if self.listen_addrs.is_empty() {
            self.listen_addrs = Vec::new();
        }
    }

    /// Returns the canonical workload identifier.
    ///
    /// Format: `{namespace}/{kind}/{name}`
    pub fn workload_id(&self) -> String {
        format!(
            "{}/{}/{}",
            self.namespace, self.workload_kind, self.workload_name
        )
    }

    /// Extracts the ordinal index from a StatefulSet pod name.
    ///
    /// For pod names like "my-app-0", "my-app-1", returns the numeric suffix.
    /// Returns `None` for non-stateful pod names.
    pub fn ordinal(&self) -> Option<u32> {
        let suffix = self.pod_name.split('-').next_back()?;
        suffix.parse().ok()
    }
}
