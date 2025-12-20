//! # Runtime Engine Framework
//!
//! This module provides the abstraction layer for workload runtime engines.
//! It defines the [`RuntimeEngine`] trait and a [`RuntimeRegistry`] for
//! managing engine implementations.
//!
//! # Available Engines
//!
//! - [`krun::KrunEngine`]: MicroVM isolation via libkrun (Linux KVM / macOS HVF)
//! - [`crun::CrunEngine`]: Container isolation via crun (OCI runtime for IoT/constrained devices)
//!
//! # Runtime Selection
//!
//! The runtime is selected via Kubernetes annotation on the manifest:
//!
//! ```yaml
//! metadata:
//!   annotations:
//!     magik.io/runtime: "crun"  # or "krun" (default)
//! ```
//!
//! - **krun** (default): Hardware-isolated microVMs using KVM/HVF
//! - **crun**: Container isolation for IoT/embedded devices without virtualization
//!
//! # Security Note
//!
//! Container isolation (crun) is **weaker** than microVM isolation (krun).
//! Use crun only when hardware constraints prohibit microVM overhead.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                RuntimeRegistry                       │
//! │  ┌─────────────────────────────────────────────────┐ │
//! │  │            trait RuntimeEngine                  │ │
//! │  ├─────────────────────────────────────────────────┤ │
//! │  │  apply()    - Deploy a manifest                 │ │
//! │  │  list()     - List running pods                 │ │
//! │  │  delete()   - Remove a pod                      │ │
//! │  │  logs()     - Get pod logs                      │ │
//! │  │  validate() - Validate manifest before deploy   │ │
//! │  └─────────────────────────────────────────────────┘ │
//! │                        ▲                             │
//! │                        │                             │
//! │          ┌─────────────┴─────────────┐              │
//! │          │                           │              │
//! │   ┌──────┴──────┐             ┌──────┴──────┐       │
//! │   │ KrunEngine  │             │ CrunEngine  │       │
//! │   │ (microVM)   │             │ (container) │       │
//! │   └─────────────┘             └─────────────┘       │
//! └─────────────────────────────────────────────────────┘
//! ```

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use thiserror::Error;

// KrunEngine: microVM isolation via libkrun (Linux KVM / macOS HVF)
// Only available when runtime-krun feature is enabled
#[cfg(feature = "runtime-krun")]
pub mod krun;

// Stub module when krun feature is disabled
#[cfg(not(feature = "runtime-krun"))]
pub mod krun {
    //! Stub module when libkrun is not available.
    use super::*;

    /// Stub KrunEngine when libkrun is not compiled in.
    pub struct KrunEngine;

    impl KrunEngine {
        pub fn new() -> Self {
            Self
        }
    }

    impl Default for KrunEngine {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl RuntimeEngine for KrunEngine {
        fn name(&self) -> &str {
            "krun"
        }

        async fn is_available(&self) -> bool {
            false
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn apply(
            &self,
            _manifest_content: &[u8],
            _config: &DeploymentConfig,
        ) -> RuntimeResult<PodInfo> {
            Err(RuntimeError::EngineNotAvailable(
                "krun: libkrun not available (enable runtime-krun feature)".to_string(),
            ))
        }

        async fn get_status(&self, _pod_id: &str) -> RuntimeResult<PodInfo> {
            Err(RuntimeError::EngineNotAvailable(
                "krun: not available".to_string(),
            ))
        }

        async fn list(&self) -> RuntimeResult<Vec<PodInfo>> {
            Err(RuntimeError::EngineNotAvailable(
                "krun: not available".to_string(),
            ))
        }

        async fn delete(&self, _pod_id: &str) -> RuntimeResult<()> {
            Err(RuntimeError::EngineNotAvailable(
                "krun: not available".to_string(),
            ))
        }

        async fn logs(&self, _pod_id: &str, _tail: Option<usize>) -> RuntimeResult<String> {
            Err(RuntimeError::EngineNotAvailable(
                "krun: not available".to_string(),
            ))
        }

        async fn validate(&self, _manifest_content: &[u8]) -> RuntimeResult<()> {
            Err(RuntimeError::EngineNotAvailable(
                "krun: not available".to_string(),
            ))
        }

        async fn export(&self, _pod_id: &str) -> RuntimeResult<Vec<u8>> {
            Err(RuntimeError::EngineNotAvailable(
                "krun: not available".to_string(),
            ))
        }
    }
}

// CrunEngine: container isolation via libcrun (functional on Linux, stubs on other platforms)
pub mod crun;

/// Errors that can occur during runtime operations.
#[derive(Error, Debug)]
pub enum RuntimeError {
    /// The requested engine is not available or not running
    #[error("Runtime engine not available: {0}")]
    EngineNotAvailable(String),

    /// The manifest content is malformed or unsupported
    #[error("Invalid manifest format: {0}")]
    InvalidManifest(String),

    /// Deployment to the runtime failed
    #[error("Deployment failed: {0}")]
    DeploymentFailed(String),

    /// The specified pod/container was not found
    #[error("Instance not found: {0}")]
    InstanceNotFound(String),

    /// The pod exists but is not in a ready state
    #[error("Instance not ready: {0}")]
    InstanceNotReady(String),

    /// I/O error during runtime operations
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// A command to the runtime engine failed
    #[error("Command execution failed: {0}")]
    CommandFailed(String),
}

/// Result type for runtime operations.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Status of a pod/microVM in the runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PodStatus {
    /// Pod is starting up
    Starting,
    /// Pod is running normally
    Running,
    /// Pod has stopped
    Stopped,
    /// Pod failed with an error message
    Failed(String),
    /// Pod status could not be determined
    Unknown,
}

/// Information about a deployed pod/microVM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodInfo {
    /// Unique pod identifier (runtime-specific)
    pub id: String,
    /// Kubernetes namespace (or "default")
    pub namespace: String,
    /// Resource kind (e.g., "Deployment", "Pod")
    pub kind: String,
    /// Resource name
    pub name: String,
    /// Current pod status
    pub status: PodStatus,
    /// Additional metadata (labels, annotations)
    pub metadata: HashMap<String, String>,
    /// When the pod was created
    pub created_at: std::time::SystemTime,
    /// When the pod was last updated
    pub updated_at: std::time::SystemTime,
    /// Port mappings for the pod
    pub ports: Vec<PortMapping>,
}

/// Mapping between microVM and host ports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortMapping {
    /// Port inside the microVM
    pub container_port: u16,
    /// Port on the host (if published)
    pub host_port: Option<u16>,
    /// Protocol (tcp/udp)
    pub protocol: String,
}

/// Configuration for the workplane agent in the microVM.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InfraConfig {
    /// Path to the workplane binary to inject into the microVM.
    /// If None, uses the embedded binary.
    pub binary_path: Option<String>,

    /// Command-line arguments for the workplane agent.
    pub args: Option<Vec<String>>,

    /// Environment variables for the workplane agent.
    pub env: HashMap<String, String>,
}

/// Configuration for deploying a manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentConfig {
    /// Number of replicas to deploy (typically 1 in distributed model)
    pub replicas: u32,
    /// Environment variables to inject into workload
    pub env: HashMap<String, String>,
    /// Engine-specific options
    pub runtime_options: HashMap<String, String>,
    /// Workplane agent configuration
    pub infra: Option<InfraConfig>,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            replicas: 1,
            env: HashMap::new(),
            runtime_options: HashMap::new(),
            infra: None,
        }
    }
}

/// Trait for microVM runtime engine implementations.
///
/// Magik uses libkrun for hardware-isolated microVM workloads.
#[async_trait]
pub trait RuntimeEngine: Send + Sync {
    /// Returns the engine name (e.g., "krun")
    fn name(&self) -> &str;

    /// Checks if the engine is available and operational
    async fn is_available(&self) -> bool;

    /// Returns self as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;

    /// Deploys a manifest and returns pod information
    async fn apply(
        &self,
        manifest_content: &[u8],
        config: &DeploymentConfig,
    ) -> RuntimeResult<PodInfo>;

    /// Gets status of a specific pod
    async fn get_status(&self, pod_id: &str) -> RuntimeResult<PodInfo>;

    /// Lists all pods managed by this engine
    async fn list(&self) -> RuntimeResult<Vec<PodInfo>>;

    /// Deletes a pod by ID
    async fn delete(&self, pod_id: &str) -> RuntimeResult<()>;

    /// Gets logs from a pod
    async fn logs(&self, pod_id: &str, tail: Option<usize>) -> RuntimeResult<String>;

    /// Validates a manifest without deploying
    async fn validate(&self, manifest_content: &[u8]) -> RuntimeResult<()>;

    /// Exports a pod's state for migration
    async fn export(&self, pod_id: &str) -> RuntimeResult<Vec<u8>>;
}

/// Registry for managing runtime engines.
///
/// Provides lookup, enumeration, and availability checking for
/// registered engines.
pub struct RuntimeRegistry {
    engines: HashMap<String, Box<dyn RuntimeEngine>>,
    default_engine: Option<String>,
}

impl RuntimeRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            engines: HashMap::new(),
            default_engine: None,
        }
    }

    /// Registers a new engine. The first registered engine becomes the default.
    pub fn register(&mut self, engine: Box<dyn RuntimeEngine>) {
        let name = engine.name().to_string();
        self.engines.insert(name.clone(), engine);

        if self.default_engine.is_none() {
            self.default_engine = Some(name);
        }
    }

    /// Gets an engine by name.
    pub fn get_engine(&self, name: &str) -> Option<&dyn RuntimeEngine> {
        self.engines.get(name).map(|e| e.as_ref())
    }

    /// Gets a mutable reference to an engine (test only).
    #[cfg(test)]
    pub fn get_engine_mut(&mut self, name: &str) -> Option<&mut Box<dyn RuntimeEngine>> {
        self.engines.get_mut(name)
    }

    /// Gets the default engine.
    pub fn get_default_engine(&self) -> Option<&dyn RuntimeEngine> {
        self.default_engine
            .as_ref()
            .and_then(|name| self.get_engine(name))
    }

    /// Sets the default engine by name.
    pub fn set_default_engine(&mut self, name: &str) -> RuntimeResult<()> {
        if self.engines.contains_key(name) {
            self.default_engine = Some(name.to_string());
            Ok(())
        } else {
            Err(RuntimeError::EngineNotAvailable(name.to_string()))
        }
    }

    /// Lists all registered engine names.
    pub fn list_engines(&self) -> Vec<&str> {
        self.engines.keys().map(|s| s.as_str()).collect()
    }

    /// Checks availability of all registered engines.
    pub async fn check_available_engines(&self) -> HashMap<String, bool> {
        let mut results = HashMap::new();
        for (name, engine) in &self.engines {
            results.insert(name.clone(), engine.is_available().await);
        }
        results
    }
}

impl Default for RuntimeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Creates the default runtime registry with available engines.
///
/// Registers both KrunEngine (microVM) and CrunEngine (container).
/// Each engine uses `is_available()` for runtime detection - stubs return false
/// on unsupported platforms.
///
/// # Runtime Selection
///
/// Use `magik.io/runtime` annotation on the manifest:
/// - `krun` (default): MicroVM isolation via libkrun
/// - `crun`: Container isolation for constrained devices (Linux only)
pub async fn create_default_registry() -> RuntimeRegistry {
    let mut registry = RuntimeRegistry::new();

    // Register KrunEngine (default for hardware isolation)
    // Available on Linux (KVM) and macOS (HVF), stubs on other platforms
    let krun_engine = krun::KrunEngine::new();
    if krun_engine.is_available().await {
        registry.register(Box::new(krun_engine));
        log::info!("KrunEngine registered for microVM isolation");
    } else {
        log::warn!(
            "KrunEngine not available - libkrun not found. \
             Install libkrun: https://github.com/containers/libkrun"
        );
    }

    // Register CrunEngine (for IoT/constrained devices)
    // Available on Linux only, stubs on other platforms
    let crun_engine = crun::CrunEngine::new();
    if crun_engine.is_available().await {
        registry.register(Box::new(crun_engine));
        log::info!("CrunEngine registered for container isolation (IoT/embedded)");
    } else {
        log::debug!(
            "CrunEngine not available - libcrun not found or platform unsupported. \
             Container runtime option will be unavailable."
        );
    }

    // Ensure at least one engine is available
    if registry.list_engines().is_empty() {
        log::error!(
            "No runtime engines available! \
             Install libkrun (recommended) or crun."
        );
    }

    registry
}

#[cfg(all(test, debug_assertions))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_runtime_registry() {
        let mut registry = RuntimeRegistry::new();
        let krun_engine = krun::KrunEngine::new();

        registry.register(Box::new(krun_engine));

        assert!(registry.get_engine("krun").is_some());
        assert_eq!(registry.get_default_engine().unwrap().name(), "krun");

        let engines = registry.list_engines();
        assert!(engines.contains(&"krun"));
    }

    #[tokio::test]
    async fn test_default_deployment_config() {
        let config = DeploymentConfig::default();
        assert_eq!(config.replicas, 1);
        assert!(config.env.is_empty());
    }
}
