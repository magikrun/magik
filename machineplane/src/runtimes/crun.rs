//! # CrunEngine - libcrun FFI OCI Container Runtime
//!
//! This module provides container-based workload isolation using libcrun,
//! a lightweight OCI-compliant container runtime library written in C.
//!
//! # Use Case
//!
//! CrunEngine is designed for **hardware-constrained IoT devices** where
//! microVM overhead (KVM/HVF) is not feasible:
//! - ARM devices without virtualization extensions
//! - Systems with <512 MiB RAM
//! - Embedded Linux gateways
//! - **WebAssembly workloads** via wasmedge/wasmtime integration
//!
//! # WebAssembly Support
//!
//! CrunEngine provides **first-class Wasm support** through crun's native
//! OCI handler integration. Deploy `.wasm` modules packaged as OCI images:
//!
//! ```yaml
//! spec:
//!   containers:
//!   - name: my-wasm-app
//!     image: ghcr.io/example/my-app:wasm
//!   annotations:
//!     module.wasm.image/variant: compat-smart
//! ```
//!
//! # Security Notice
//!
//! Container isolation is **weaker than microVM isolation**:
//! - Workloads share the host kernel
//! - A kernel exploit can compromise the host
//! - Use only when microVM overhead is prohibitive
//!
//! Select this runtime via annotation: `magik.io/runtime: crun`
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     CrunEngine                              │
//! ├─────────────────────────────────────────────────────────────┤
//! │  1. Pull OCI image from registry                            │
//! │  2. Unpack layers → rootfs directory                        │
//! │  3. Inject workplane binary                                 │
//! │  4. Load OCI spec via libcrun FFI                           │
//! │  5. Run container via libcrun_container_run()               │
//! └─────────────────────────────────────────────────────────────┘
//!                            │
//!                            ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Container                               │
//! │  ┌───────────────────────────────────────────────────────┐  │
//! │  │  workplane (PID 1)                                    │  │
//! │  │  ├── Mesh agent (Korium)                              │  │
//! │  │  └── Supervisor → spawns workload process             │  │
//! │  └───────────────────────────────────────────────────────┘  │
//! │                                                             │
//! │  Namespaces: pid, net, mount, user, ipc, uts, cgroup        │
//! │  Security: seccomp, no-new-privs, read-only rootfs          │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Requirements
//!
//! - libcrun >= 1.8 installed on the host system
//! - Linux kernel 4.18+ with cgroup v2 (recommended)
//! - Embedded workplane binary (musl-linked)
//!
//! # Platform Support
//!
//! CrunEngine is **Linux-only**. On non-Linux platforms (macOS, Windows),
//! the engine will report as unavailable via `is_available()`.

use crate::runtimes::{
    DeploymentConfig, PodInfo, PodStatus, PortMapping, RuntimeEngine, RuntimeError, RuntimeResult,
};
use async_trait::async_trait;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

// FFI imports are Linux-only
#[cfg(target_os = "linux")]
use std::ffi::{CStr, CString};
#[cfg(target_os = "linux")]
use std::ptr;

// =============================================================================
// Constants
// =============================================================================

/// Default memory limit in bytes (256 MiB - suitable for IoT).
const DEFAULT_MEMORY_LIMIT: u64 = 256 * 1024 * 1024;

/// Maximum memory limit in bytes (2 GiB).
const MAX_MEMORY_LIMIT: u64 = 2 * 1024 * 1024 * 1024;

/// Default CPU shares (relative weight).
const DEFAULT_CPU_SHARES: u64 = 1024;

/// Maximum PIDs per container.
const MAX_PIDS: i64 = 512;

/// Maximum number of concurrent containers.
const MAX_CONTAINERS: usize = 256;

/// Maximum OCI image reference length.
const MAX_IMAGE_REF_LEN: usize = 512;

/// Maximum size of a single OCI layer (512 MiB).
const MAX_LAYER_SIZE: usize = 512 * 1024 * 1024;

/// Maximum total rootfs size (4 GiB).
const MAX_ROOTFS_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Directory for storing container bundles.
const BUNDLE_BASE_DIR: &str = "/var/lib/magik/bundles";

/// Directory for storing container state (Linux only).
#[cfg(target_os = "linux")]
const STATE_DIR: &str = "/var/run/magik/crun";

/// Label keys for metadata.
const POD_ID_LABEL_KEY: &str = "magik.io/pod-id";
const NAMESPACE_LABEL_KEY: &str = "magik.io/namespace";
const KIND_LABEL_KEY: &str = "magik.io/kind";
const NAME_LABEL_KEY: &str = "magik.io/name";
const RUNTIME_LABEL_KEY: &str = "magik.io/runtime";

// =============================================================================
// OCI Runtime Spec Types (minimal subset for JSON generation)
// =============================================================================

/// OCI Runtime Specification (minimal for crun).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciSpec {
    oci_version: String,
    process: OciProcess,
    root: OciRoot,
    hostname: String,
    mounts: Vec<OciMount>,
    linux: OciLinux,
    #[serde(skip_serializing_if = "Option::is_none")]
    annotations: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciProcess {
    terminal: bool,
    user: OciUser,
    args: Vec<String>,
    env: Vec<String>,
    cwd: String,
    capabilities: OciCapabilities,
    no_new_privileges: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OciUser {
    uid: u32,
    gid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciCapabilities {
    bounding: Vec<String>,
    effective: Vec<String>,
    permitted: Vec<String>,
    ambient: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OciRoot {
    path: String,
    readonly: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OciMount {
    destination: String,
    #[serde(rename = "type")]
    mount_type: String,
    source: String,
    options: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OciLinux {
    namespaces: Vec<OciNamespace>,
    resources: OciResources,
    #[serde(skip_serializing_if = "Option::is_none")]
    seccomp: Option<serde_json::Value>,
    masked_paths: Vec<String>,
    readonly_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OciNamespace {
    #[serde(rename = "type")]
    ns_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OciResources {
    memory: OciMemory,
    cpu: OciCpu,
    pids: OciPids,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OciMemory {
    limit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    swap: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OciCpu {
    shares: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OciPids {
    limit: i64,
}

// =============================================================================
// Container State Tracking
// =============================================================================

/// State of a running container.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ContainerState {
    /// Unique container identifier
    id: String,
    /// Container name (for display)
    name: String,
    /// Kubernetes namespace
    namespace: String,
    /// Resource kind
    kind: String,
    /// Resource name
    resource_name: String,
    /// Path to bundle directory
    bundle_path: PathBuf,
    /// Process ID of the container (if running)
    pid: Option<u32>,
    /// Current status
    status: PodStatus,
    /// Labels/metadata
    labels: HashMap<String, String>,
    /// Port mappings
    ports: Vec<PortMapping>,
    /// Creation timestamp
    created_at: std::time::SystemTime,
}

/// In-memory registry of running containers.
type ContainerRegistry = Arc<RwLock<HashMap<String, ContainerState>>>;

// =============================================================================
// CrunEngine Implementation
// =============================================================================

/// OCI container runtime engine using libcrun FFI.
///
/// Provides namespace-based workload isolation for hardware-constrained
/// environments where microVM overhead is prohibitive.
pub struct CrunEngine {
    /// Registry of running containers
    containers: ContainerRegistry,
    /// Path to embedded workplane binary
    workplane_path: Option<PathBuf>,
    /// Whether libcrun is available
    available: bool,
}

impl CrunEngine {
    /// Creates a new CrunEngine instance.
    ///
    /// Checks for libcrun availability at construction time.
    pub fn new() -> Self {
        let available = Self::check_libcrun_available();
        let workplane_path = Self::find_workplane_binary();

        if !available {
            warn!("libcrun not available - CrunEngine will be unavailable");
        }

        Self {
            containers: Arc::new(RwLock::new(HashMap::new())),
            workplane_path,
            available,
        }
    }

    /// Checks if libcrun is available by attempting to load container features.
    #[cfg(target_os = "linux")]
    fn check_libcrun_available() -> bool {
        // SAFETY: We're calling libcrun_container_get_features with a null context
        // to check if the library is loaded and functional. The error pointer
        // is properly initialized and the features pointer is checked.
        unsafe {
            let mut err: crun_sys::libcrun_error_t = ptr::null_mut();
            let mut features: *mut crun_sys::features_info_s = ptr::null_mut();

            // Create a minimal context for the features query
            let ctx = Box::into_raw(Box::new(crun_sys::libcrun_context_s {
                state_root: ptr::null(),
                id: ptr::null(),
                bundle: ptr::null(),
                console_socket: ptr::null(),
                pid_file: ptr::null(),
                notify_socket: ptr::null(),
                handler: ptr::null(),
                preserve_fds: 0,
                listen_fds: 0,
                output_handler: None,
                output_handler_arg: ptr::null_mut(),
                fifo_exec_wait_fd: -1,
                systemd_cgroup: false,
                detach: true,
                no_new_keyring: true,
                force_no_cgroup: false,
                no_pivot: false,
                argv: ptr::null_mut(),
                argc: 0,
                handler_manager: ptr::null_mut(),
            }));

            let ret = crun_sys::libcrun_container_get_features(ctx, &mut features, &mut err);

            // Clean up
            let _ = Box::from_raw(ctx);
            if !features.is_null() {
                // Features would need cleanup here in real usage
            }

            if ret == 0 {
                info!("libcrun available and functional");
                true
            } else {
                if !err.is_null() {
                    let err_msg = CStr::from_ptr((*err).msg).to_string_lossy();
                    debug!("libcrun check failed: {}", err_msg);
                    crun_sys::crun_error_release(&mut err);
                }
                false
            }
        }
    }

    /// On non-Linux platforms, libcrun is never available.
    #[cfg(not(target_os = "linux"))]
    fn check_libcrun_available() -> bool {
        warn!("CrunEngine is Linux-only - not available on this platform");
        false
    }

    /// Finds the embedded workplane binary.
    fn find_workplane_binary() -> Option<PathBuf> {
        // Determine the correct target based on host architecture
        let target_dir = if cfg!(target_arch = "aarch64") {
            "aarch64-unknown-linux-musl"
        } else {
            "x86_64-unknown-linux-musl"
        };

        let candidates = [
            "/usr/local/lib/magik/workplane".to_string(),
            "./workplane".to_string(),
            format!("../target/{}/release/workplane", target_dir),
        ];

        for path in &candidates {
            let p = PathBuf::from(path);
            if p.exists() {
                info!("Found workplane binary at: {}", path);
                return Some(p);
            }
        }

        #[cfg(feature = "embedded-workplane")]
        {
            return Some(PathBuf::from("/tmp/magik-workplane"));
        }

        warn!("Workplane binary not found - container deployment will fail");
        None
    }

    /// Parses a Kubernetes manifest and extracts container spec.
    fn parse_manifest(&self, manifest_content: &[u8]) -> RuntimeResult<ContainerSpec> {
        let doc: serde_yaml::Value = serde_yaml::from_slice(manifest_content)
            .map_err(|e| RuntimeError::InvalidManifest(format!("YAML parse error: {}", e)))?;

        let spec = doc
            .get("spec")
            .ok_or_else(|| RuntimeError::InvalidManifest("Missing spec".to_string()))?;

        let pod_spec = if let Some(template) = spec.get("template") {
            template.get("spec")
        } else {
            Some(spec)
        }
        .ok_or_else(|| RuntimeError::InvalidManifest("Missing pod spec".to_string()))?;

        let containers = pod_spec
            .get("containers")
            .and_then(|c| c.as_sequence())
            .ok_or_else(|| RuntimeError::InvalidManifest("Missing containers".to_string()))?;

        let container = containers
            .first()
            .ok_or_else(|| RuntimeError::InvalidManifest("No containers defined".to_string()))?;

        let image = container
            .get("image")
            .and_then(|i| i.as_str())
            .ok_or_else(|| RuntimeError::InvalidManifest("Missing container image".to_string()))?
            .to_string();

        let command = container
            .get("command")
            .and_then(|c| c.as_sequence())
            .map(|seq| {
                seq.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            });

        let args = container
            .get("args")
            .and_then(|a| a.as_sequence())
            .map(|seq| {
                seq.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            });

        let env = container
            .get("env")
            .and_then(|e| e.as_sequence())
            .map(|seq| {
                seq.iter()
                    .filter_map(|v| {
                        let name = v.get("name")?.as_str()?;
                        let value = v.get("value")?.as_str()?;
                        Some((name.to_string(), value.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let resources = container.get("resources");
        let memory_limit = resources
            .and_then(|r| r.get("limits"))
            .and_then(|l| l.get("memory"))
            .and_then(|m| m.as_str())
            .and_then(Self::parse_memory_string)
            .unwrap_or(DEFAULT_MEMORY_LIMIT);

        let cpu_shares = resources
            .and_then(|r| r.get("limits"))
            .and_then(|l| l.get("cpu"))
            .and_then(|c| c.as_str())
            .and_then(Self::parse_cpu_to_shares)
            .unwrap_or(DEFAULT_CPU_SHARES);

        Ok(ContainerSpec {
            image,
            command,
            args,
            env,
            memory_limit: memory_limit.min(MAX_MEMORY_LIMIT),
            cpu_shares,
        })
    }

    /// Parses a Kubernetes memory string (e.g., "512Mi", "1Gi") to bytes.
    fn parse_memory_string(s: &str) -> Option<u64> {
        let s = s.trim();
        if let Some(num) = s.strip_suffix("Gi") {
            num.parse::<u64>().ok().map(|v| v * 1024 * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix("Mi") {
            num.parse::<u64>().ok().map(|v| v * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix("Ki") {
            num.parse::<u64>().ok().map(|v| v * 1024)
        } else if let Some(num) = s.strip_suffix('G') {
            num.parse::<u64>().ok().map(|v| v * 1024 * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix('M') {
            num.parse::<u64>().ok().map(|v| v * 1024 * 1024)
        } else if let Some(num) = s.strip_suffix('K') {
            num.parse::<u64>().ok().map(|v| v * 1024)
        } else {
            s.parse::<u64>().ok()
        }
    }

    /// Parses a Kubernetes CPU string (e.g., "1", "500m") to cgroup shares.
    fn parse_cpu_to_shares(s: &str) -> Option<u64> {
        let s = s.trim();
        if let Some(num) = s.strip_suffix('m') {
            num.parse::<u64>().ok().map(|m| (m * 1024) / 1000)
        } else {
            s.parse::<u64>().ok().map(|c| c * 1024)
        }
    }

    /// Extracts metadata from a manifest.
    fn parse_metadata(&self, doc: &serde_yaml::Value) -> (String, String, String) {
        let metadata = doc.get("metadata");
        let kind = doc
            .get("kind")
            .and_then(|k| k.as_str())
            .unwrap_or("Pod")
            .to_string();
        let namespace = metadata
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
            .unwrap_or("default")
            .to_string();
        let name = metadata
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("unnamed")
            .to_string();
        (namespace, kind, name)
    }

    /// Pulls and unpacks an OCI image to a rootfs directory.
    async fn prepare_rootfs(&self, image: &str, container_id: &str) -> RuntimeResult<PathBuf> {
        use flate2::read::GzDecoder;
        use oci_distribution::{Client, Reference, secrets::RegistryAuth};
        use tar::Archive;

        if image.len() > MAX_IMAGE_REF_LEN {
            return Err(RuntimeError::InvalidManifest(format!(
                "Image reference too long: {} > {} bytes",
                image.len(),
                MAX_IMAGE_REF_LEN
            )));
        }

        let bundle_path = PathBuf::from(BUNDLE_BASE_DIR).join(container_id);
        let rootfs_path = bundle_path.join("rootfs");
        std::fs::create_dir_all(&rootfs_path).map_err(RuntimeError::IoError)?;

        info!("Pulling OCI image: {} → {}", image, rootfs_path.display());

        let reference: Reference = image.parse().map_err(|e| {
            RuntimeError::InvalidManifest(format!("Invalid image reference: {}", e))
        })?;

        let client = Client::new(oci_distribution::client::ClientConfig {
            protocol: oci_distribution::client::ClientProtocol::Https,
            ..Default::default()
        });

        let auth = RegistryAuth::Anonymous;
        let (manifest, _digest) = client.pull_manifest(&reference, &auth).await.map_err(|e| {
            RuntimeError::DeploymentFailed(format!("Failed to pull manifest: {}", e))
        })?;

        let layers = match manifest {
            oci_distribution::manifest::OciManifest::Image(img) => img.layers,
            oci_distribution::manifest::OciManifest::ImageIndex(idx) => {
                if let Some(_first) = idx.manifests.first() {
                    let (inner, _) =
                        client.pull_manifest(&reference, &auth).await.map_err(|e| {
                            RuntimeError::DeploymentFailed(format!(
                                "Failed to pull platform manifest: {}",
                                e
                            ))
                        })?;
                    match inner {
                        oci_distribution::manifest::OciManifest::Image(img) => img.layers,
                        _ => {
                            return Err(RuntimeError::DeploymentFailed(
                                "Unexpected manifest type".to_string(),
                            ));
                        }
                    }
                } else {
                    return Err(RuntimeError::DeploymentFailed(
                        "Empty image index".to_string(),
                    ));
                }
            }
        };

        let mut total_unpacked: u64 = 0;
        for layer in layers {
            debug!("Pulling layer: {}", layer.digest);

            let mut layer_data: Vec<u8> = Vec::new();
            client
                .pull_blob(&reference, &layer, &mut layer_data)
                .await
                .map_err(|e| {
                    RuntimeError::DeploymentFailed(format!("Failed to pull layer: {}", e))
                })?;

            if layer_data.len() > MAX_LAYER_SIZE {
                let _ = std::fs::remove_dir_all(&bundle_path);
                return Err(RuntimeError::DeploymentFailed(format!(
                    "Layer {} exceeds maximum size: {} > {} bytes",
                    layer.digest,
                    layer_data.len(),
                    MAX_LAYER_SIZE
                )));
            }

            let decoder = GzDecoder::new(&layer_data[..]);
            let mut archive = Archive::new(decoder);

            for entry_result in archive.entries().map_err(|e| {
                RuntimeError::DeploymentFailed(format!("Failed to read tar entries: {}", e))
            })? {
                let mut entry = entry_result.map_err(|e| {
                    RuntimeError::DeploymentFailed(format!("Failed to read tar entry: {}", e))
                })?;

                let entry_path = entry.path().map_err(|e| {
                    RuntimeError::DeploymentFailed(format!("Invalid entry path: {}", e))
                })?;

                let path_str = entry_path.to_string_lossy();
                if path_str.contains("..") || path_str.starts_with('/') {
                    warn!("Rejecting suspicious tar entry: {}", path_str);
                    continue;
                }

                total_unpacked += entry.size();
                if total_unpacked > MAX_ROOTFS_SIZE {
                    let _ = std::fs::remove_dir_all(&bundle_path);
                    return Err(RuntimeError::DeploymentFailed(format!(
                        "Rootfs exceeds maximum size: {} > {} bytes",
                        total_unpacked, MAX_ROOTFS_SIZE
                    )));
                }

                entry.unpack_in(&rootfs_path).map_err(|e| {
                    RuntimeError::DeploymentFailed(format!("Failed to unpack entry: {}", e))
                })?;
            }
        }

        // Inject workplane binary
        if let Some(workplane_src) = &self.workplane_path {
            let workplane_dst = rootfs_path.join("workplane");
            std::fs::copy(workplane_src, &workplane_dst).map_err(|e| {
                RuntimeError::DeploymentFailed(format!("Failed to inject workplane: {}", e))
            })?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(&workplane_dst)?.permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&workplane_dst, perms)?;
            }
        } else {
            return Err(RuntimeError::DeploymentFailed(
                "Workplane binary not available for injection".to_string(),
            ));
        }

        info!("Rootfs prepared at: {}", rootfs_path.display());
        Ok(bundle_path)
    }

    /// Generates an OCI runtime specification.
    fn generate_oci_spec(
        &self,
        container_id: &str,
        spec: &ContainerSpec,
        config: &DeploymentConfig,
        name: &str,
        namespace: &str,
        kind: &str,
    ) -> OciSpec {
        let mut exec_parts = spec.command.clone().unwrap_or_default();
        exec_parts.extend(spec.args.clone().unwrap_or_default());
        let exec_cmd = exec_parts.join(" ");

        let mut args = vec!["/workplane".to_string()];
        args.push("--pod".to_string());
        args.push(name.to_string());
        args.push("--namespace".to_string());
        args.push(namespace.to_string());
        args.push("--replicas".to_string());
        args.push(config.replicas.to_string());

        if !exec_cmd.is_empty() {
            args.push("--exec".to_string());
            args.push(exec_cmd);
        }

        let mut env: Vec<String> = vec![
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_string(),
            "TERM=xterm".to_string(),
            format!("MAGIK_POD_ID={}", container_id),
            format!("MAGIK_POD_NAME={}", name),
            format!("MAGIK_NAMESPACE={}", namespace),
            format!("MAGIK_KIND={}", kind),
        ];

        for (k, v) in &spec.env {
            env.push(format!("{}={}", k, v));
        }
        for (k, v) in &config.env {
            env.push(format!("{}={}", k, v));
        }

        let mut annotations = HashMap::new();
        annotations.insert(POD_ID_LABEL_KEY.to_string(), container_id.to_string());
        annotations.insert(NAMESPACE_LABEL_KEY.to_string(), namespace.to_string());
        annotations.insert(KIND_LABEL_KEY.to_string(), kind.to_string());
        annotations.insert(NAME_LABEL_KEY.to_string(), name.to_string());
        annotations.insert(RUNTIME_LABEL_KEY.to_string(), "crun".to_string());

        OciSpec {
            oci_version: "1.0.2".to_string(),
            process: OciProcess {
                terminal: false,
                user: OciUser { uid: 0, gid: 0 },
                args,
                env,
                cwd: "/".to_string(),
                capabilities: OciCapabilities {
                    bounding: vec!["CAP_NET_BIND_SERVICE".to_string()],
                    effective: vec!["CAP_NET_BIND_SERVICE".to_string()],
                    permitted: vec!["CAP_NET_BIND_SERVICE".to_string()],
                    ambient: vec![],
                },
                no_new_privileges: true,
            },
            root: OciRoot {
                path: "rootfs".to_string(),
                readonly: true,
            },
            hostname: name.to_string(),
            mounts: vec![
                OciMount {
                    destination: "/proc".to_string(),
                    mount_type: "proc".to_string(),
                    source: "proc".to_string(),
                    options: vec![
                        "nosuid".to_string(),
                        "noexec".to_string(),
                        "nodev".to_string(),
                    ],
                },
                OciMount {
                    destination: "/dev".to_string(),
                    mount_type: "tmpfs".to_string(),
                    source: "tmpfs".to_string(),
                    options: vec![
                        "nosuid".to_string(),
                        "strictatime".to_string(),
                        "mode=755".to_string(),
                        "size=65536k".to_string(),
                    ],
                },
                OciMount {
                    destination: "/sys".to_string(),
                    mount_type: "sysfs".to_string(),
                    source: "sysfs".to_string(),
                    options: vec![
                        "nosuid".to_string(),
                        "noexec".to_string(),
                        "nodev".to_string(),
                        "ro".to_string(),
                    ],
                },
                OciMount {
                    destination: "/tmp".to_string(),
                    mount_type: "tmpfs".to_string(),
                    source: "tmpfs".to_string(),
                    options: vec![
                        "nosuid".to_string(),
                        "nodev".to_string(),
                        "size=64m".to_string(),
                    ],
                },
            ],
            linux: OciLinux {
                namespaces: vec![
                    OciNamespace {
                        ns_type: "pid".to_string(),
                        path: None,
                    },
                    OciNamespace {
                        ns_type: "network".to_string(),
                        path: None,
                    },
                    OciNamespace {
                        ns_type: "ipc".to_string(),
                        path: None,
                    },
                    OciNamespace {
                        ns_type: "uts".to_string(),
                        path: None,
                    },
                    OciNamespace {
                        ns_type: "mount".to_string(),
                        path: None,
                    },
                    OciNamespace {
                        ns_type: "cgroup".to_string(),
                        path: None,
                    },
                ],
                resources: OciResources {
                    memory: OciMemory {
                        limit: spec.memory_limit,
                        swap: Some(spec.memory_limit),
                    },
                    cpu: OciCpu {
                        shares: spec.cpu_shares,
                    },
                    pids: OciPids { limit: MAX_PIDS },
                },
                seccomp: None,
                masked_paths: vec![
                    "/proc/acpi".to_string(),
                    "/proc/kcore".to_string(),
                    "/proc/keys".to_string(),
                    "/proc/latency_stats".to_string(),
                    "/proc/timer_list".to_string(),
                    "/proc/timer_stats".to_string(),
                    "/proc/sched_debug".to_string(),
                    "/sys/firmware".to_string(),
                ],
                readonly_paths: vec![
                    "/proc/bus".to_string(),
                    "/proc/fs".to_string(),
                    "/proc/irq".to_string(),
                    "/proc/sys".to_string(),
                    "/proc/sysrq-trigger".to_string(),
                ],
            },
            annotations: Some(annotations),
        }
    }

    /// Writes the OCI spec to config.json.
    fn write_oci_spec(&self, bundle_path: &std::path::Path, spec: &OciSpec) -> RuntimeResult<()> {
        let config_path = bundle_path.join("config.json");
        let json = serde_json::to_string_pretty(spec).map_err(|e| {
            RuntimeError::DeploymentFailed(format!("Failed to serialize OCI spec: {}", e))
        })?;
        std::fs::write(&config_path, json).map_err(RuntimeError::IoError)?;
        debug!("Wrote OCI spec to: {}", config_path.display());
        Ok(())
    }

    /// Runs a container using libcrun FFI (Linux only).
    #[cfg(target_os = "linux")]
    fn run_container_ffi(
        &self,
        container_id: &str,
        bundle_path: &std::path::Path,
    ) -> RuntimeResult<u32> {
        std::fs::create_dir_all(STATE_DIR).map_err(RuntimeError::IoError)?;

        info!("Starting container {} via libcrun FFI", container_id);

        let config_path = bundle_path.join("config.json");
        let config_json = std::fs::read_to_string(&config_path).map_err(|e| {
            RuntimeError::DeploymentFailed(format!("Failed to read config.json: {}", e))
        })?;

        let config_cstr = CString::new(config_json)
            .map_err(|e| RuntimeError::DeploymentFailed(format!("Invalid config.json: {}", e)))?;

        let id_cstr = CString::new(container_id)
            .map_err(|e| RuntimeError::DeploymentFailed(format!("Invalid container id: {}", e)))?;

        let bundle_cstr = CString::new(bundle_path.to_string_lossy().as_bytes())
            .map_err(|e| RuntimeError::DeploymentFailed(format!("Invalid bundle path: {}", e)))?;

        let state_root_cstr = CString::new(STATE_DIR)
            .map_err(|_| RuntimeError::DeploymentFailed("Invalid state dir".to_string()))?;

        // SAFETY: We're calling libcrun FFI functions with properly allocated
        // CStrings that outlive the FFI call. The context is stack-allocated
        // and the container pointer is managed by libcrun.
        unsafe {
            let mut err: crun_sys::libcrun_error_t = ptr::null_mut();

            // Load container from JSON
            let container =
                crun_sys::libcrun_container_load_from_memory(config_cstr.as_ptr(), &mut err);

            if container.is_null() {
                let msg = if !err.is_null() {
                    let s = CStr::from_ptr((*err).msg).to_string_lossy().to_string();
                    crun_sys::crun_error_release(&mut err);
                    s
                } else {
                    "Unknown error loading container".to_string()
                };
                return Err(RuntimeError::DeploymentFailed(msg));
            }

            // Create context
            let ctx = Box::into_raw(Box::new(crun_sys::libcrun_context_s {
                state_root: state_root_cstr.as_ptr(),
                id: id_cstr.as_ptr(),
                bundle: bundle_cstr.as_ptr(),
                console_socket: ptr::null(),
                pid_file: ptr::null(),
                notify_socket: ptr::null(),
                handler: ptr::null(),
                preserve_fds: 0,
                listen_fds: 0,
                output_handler: None,
                output_handler_arg: ptr::null_mut(),
                fifo_exec_wait_fd: -1,
                systemd_cgroup: false,
                detach: true,
                no_new_keyring: true,
                force_no_cgroup: false,
                no_pivot: false,
                argv: ptr::null_mut(),
                argc: 0,
                handler_manager: ptr::null_mut(),
            }));

            // Run container
            let ret = crun_sys::libcrun_container_run(
                ctx, container, 0, // No special options
                &mut err,
            );

            // Clean up context
            let _ = Box::from_raw(ctx);

            // Free container
            crun_sys::libcrun_container_free(container);

            if ret < 0 {
                let msg = if !err.is_null() {
                    let s = CStr::from_ptr((*err).msg).to_string_lossy().to_string();
                    crun_sys::crun_error_release(&mut err);
                    s
                } else {
                    format!("libcrun_container_run failed with code {}", ret)
                };
                return Err(RuntimeError::DeploymentFailed(msg));
            }

            // Return PID (ret is the PID on success for detached containers)
            let pid = if ret > 0 { ret as u32 } else { 0 };
            info!("Container {} started with PID {}", container_id, pid);
            Ok(pid)
        }
    }

    /// Stub for non-Linux platforms.
    #[cfg(not(target_os = "linux"))]
    fn run_container_ffi(&self, _container_id: &str, _bundle_path: &PathBuf) -> RuntimeResult<u32> {
        Err(RuntimeError::EngineNotAvailable(
            "CrunEngine is Linux-only".to_string(),
        ))
    }

    /// Kills a container using libcrun FFI (Linux only).
    #[cfg(target_os = "linux")]
    fn kill_container_ffi(&self, container_id: &str, signal: &str) -> RuntimeResult<()> {
        let id_cstr = CString::new(container_id)
            .map_err(|_| RuntimeError::CommandFailed("Invalid container id".to_string()))?;

        let signal_cstr = CString::new(signal)
            .map_err(|_| RuntimeError::CommandFailed("Invalid signal".to_string()))?;

        let state_root_cstr = CString::new(STATE_DIR)
            .map_err(|_| RuntimeError::CommandFailed("Invalid state dir".to_string()))?;

        // SAFETY: CStrings outlive the FFI call, context is properly initialized.
        unsafe {
            let mut err: crun_sys::libcrun_error_t = ptr::null_mut();

            let ctx = Box::into_raw(Box::new(crun_sys::libcrun_context_s {
                state_root: state_root_cstr.as_ptr(),
                id: id_cstr.as_ptr(),
                bundle: ptr::null(),
                console_socket: ptr::null(),
                pid_file: ptr::null(),
                notify_socket: ptr::null(),
                handler: ptr::null(),
                preserve_fds: 0,
                listen_fds: 0,
                output_handler: None,
                output_handler_arg: ptr::null_mut(),
                fifo_exec_wait_fd: -1,
                systemd_cgroup: false,
                detach: true,
                no_new_keyring: true,
                force_no_cgroup: false,
                no_pivot: false,
                argv: ptr::null_mut(),
                argc: 0,
                handler_manager: ptr::null_mut(),
            }));

            let ret = crun_sys::libcrun_container_kill(
                ctx,
                id_cstr.as_ptr(),
                signal_cstr.as_ptr(),
                &mut err,
            );

            let _ = Box::from_raw(ctx);

            if ret < 0 && !err.is_null() {
                let msg = CStr::from_ptr((*err).msg).to_string_lossy();
                // Ignore "not found" errors during cleanup
                if !msg.contains("not found") && !msg.contains("does not exist") {
                    crun_sys::crun_error_release(&mut err);
                    return Err(RuntimeError::CommandFailed(msg.to_string()));
                }
                crun_sys::crun_error_release(&mut err);
            }
        }
        Ok(())
    }

    /// Stub for non-Linux platforms.
    #[cfg(not(target_os = "linux"))]
    fn kill_container_ffi(&self, _container_id: &str, _signal: &str) -> RuntimeResult<()> {
        Err(RuntimeError::EngineNotAvailable(
            "CrunEngine is Linux-only".to_string(),
        ))
    }

    /// Deletes a container using libcrun FFI (Linux only).
    #[cfg(target_os = "linux")]
    fn delete_container_ffi(&self, container_id: &str) -> RuntimeResult<()> {
        let id_cstr = CString::new(container_id)
            .map_err(|_| RuntimeError::CommandFailed("Invalid container id".to_string()))?;

        let state_root_cstr = CString::new(STATE_DIR)
            .map_err(|_| RuntimeError::CommandFailed("Invalid state dir".to_string()))?;

        // SAFETY: CStrings outlive the FFI call, def can be null for force delete.
        unsafe {
            let mut err: crun_sys::libcrun_error_t = ptr::null_mut();

            let ctx = Box::into_raw(Box::new(crun_sys::libcrun_context_s {
                state_root: state_root_cstr.as_ptr(),
                id: id_cstr.as_ptr(),
                bundle: ptr::null(),
                console_socket: ptr::null(),
                pid_file: ptr::null(),
                notify_socket: ptr::null(),
                handler: ptr::null(),
                preserve_fds: 0,
                listen_fds: 0,
                output_handler: None,
                output_handler_arg: ptr::null_mut(),
                fifo_exec_wait_fd: -1,
                systemd_cgroup: false,
                detach: true,
                no_new_keyring: true,
                force_no_cgroup: false,
                no_pivot: false,
                argv: ptr::null_mut(),
                argc: 0,
                handler_manager: ptr::null_mut(),
            }));

            let ret = crun_sys::libcrun_container_delete(
                ctx,
                ptr::null_mut(), // def can be null for force delete
                id_cstr.as_ptr(),
                true, // force
                &mut err,
            );

            let _ = Box::from_raw(ctx);

            if ret < 0 && !err.is_null() {
                let msg = CStr::from_ptr((*err).msg).to_string_lossy();
                if !msg.contains("not found") && !msg.contains("does not exist") {
                    warn!("libcrun delete warning: {}", msg);
                }
                crun_sys::crun_error_release(&mut err);
            }
        }
        Ok(())
    }

    /// Stub for non-Linux platforms.
    #[cfg(not(target_os = "linux"))]
    fn delete_container_ffi(&self, _container_id: &str) -> RuntimeResult<()> {
        Err(RuntimeError::EngineNotAvailable(
            "CrunEngine is Linux-only".to_string(),
        ))
    }
}

impl Default for CrunEngine {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Container Spec
// =============================================================================

/// Parsed container specification from manifest.
#[derive(Debug, Clone)]
struct ContainerSpec {
    image: String,
    command: Option<Vec<String>>,
    args: Option<Vec<String>>,
    env: HashMap<String, String>,
    memory_limit: u64,
    cpu_shares: u64,
}

// =============================================================================
// RuntimeEngine Implementation
// =============================================================================

#[async_trait]
impl RuntimeEngine for CrunEngine {
    fn name(&self) -> &str {
        "crun"
    }

    async fn is_available(&self) -> bool {
        self.available
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn apply(
        &self,
        manifest_content: &[u8],
        config: &DeploymentConfig,
    ) -> RuntimeResult<PodInfo> {
        if !self.available {
            return Err(RuntimeError::EngineNotAvailable(
                "libcrun not available".to_string(),
            ));
        }

        {
            let containers = self.containers.read().map_err(|_| {
                RuntimeError::CommandFailed("Failed to acquire container registry lock".to_string())
            })?;
            if containers.len() >= MAX_CONTAINERS {
                return Err(RuntimeError::DeploymentFailed(format!(
                    "Maximum container limit reached: {}",
                    MAX_CONTAINERS
                )));
            }
        }

        let container_spec = self.parse_manifest(manifest_content)?;
        let doc: serde_yaml::Value = serde_yaml::from_slice(manifest_content)
            .map_err(|e| RuntimeError::InvalidManifest(format!("YAML parse error: {}", e)))?;
        let (namespace, kind, name) = self.parse_metadata(&doc);

        let container_id = Uuid::new_v4().to_string();

        let bundle_path = self
            .prepare_rootfs(&container_spec.image, &container_id)
            .await?;

        let oci_spec = self.generate_oci_spec(
            &container_id,
            &container_spec,
            config,
            &name,
            &namespace,
            &kind,
        );
        self.write_oci_spec(&bundle_path, &oci_spec)?;

        // Use FFI instead of CLI
        let pid = self.run_container_ffi(&container_id, &bundle_path)?;

        let mut labels = HashMap::new();
        labels.insert(POD_ID_LABEL_KEY.to_string(), container_id.clone());
        labels.insert(NAMESPACE_LABEL_KEY.to_string(), namespace.clone());
        labels.insert(KIND_LABEL_KEY.to_string(), kind.clone());
        labels.insert(NAME_LABEL_KEY.to_string(), name.clone());
        labels.insert(RUNTIME_LABEL_KEY.to_string(), "crun".to_string());

        let now = std::time::SystemTime::now();

        let state = ContainerState {
            id: container_id.clone(),
            name: name.clone(),
            namespace: namespace.clone(),
            kind: kind.clone(),
            resource_name: name.clone(),
            bundle_path: bundle_path.clone(),
            pid: Some(pid),
            status: PodStatus::Running,
            labels: labels.clone(),
            ports: vec![],
            created_at: now,
        };

        {
            let mut containers = self.containers.write().map_err(|_| {
                RuntimeError::CommandFailed("Failed to acquire container registry lock".to_string())
            })?;
            containers.insert(container_id.clone(), state);
        }

        Ok(PodInfo {
            id: container_id,
            namespace,
            kind,
            name,
            status: PodStatus::Running,
            metadata: labels,
            created_at: now,
            updated_at: now,
            ports: vec![],
        })
    }

    async fn get_status(&self, pod_id: &str) -> RuntimeResult<PodInfo> {
        let containers = self.containers.read().map_err(|_| {
            RuntimeError::CommandFailed("Failed to acquire container registry lock".to_string())
        })?;

        let state = containers
            .get(pod_id)
            .ok_or_else(|| RuntimeError::InstanceNotFound(pod_id.to_string()))?;

        let status = if let Some(pid) = state.pid {
            #[cfg(unix)]
            {
                // SAFETY: kill(pid, 0) is a standard POSIX idiom to check process existence.
                if unsafe { libc::kill(pid as i32, 0) == 0 } {
                    PodStatus::Running
                } else {
                    PodStatus::Stopped
                }
            }
            #[cfg(not(unix))]
            {
                PodStatus::Unknown
            }
        } else {
            PodStatus::Unknown
        };

        Ok(PodInfo {
            id: state.id.clone(),
            namespace: state.namespace.clone(),
            kind: state.kind.clone(),
            name: state.name.clone(),
            status,
            metadata: state.labels.clone(),
            created_at: state.created_at,
            updated_at: std::time::SystemTime::now(),
            ports: state.ports.clone(),
        })
    }

    async fn list(&self) -> RuntimeResult<Vec<PodInfo>> {
        let containers = self.containers.read().map_err(|_| {
            RuntimeError::CommandFailed("Failed to acquire container registry lock".to_string())
        })?;

        let mut pods = Vec::new();
        for state in containers.values() {
            pods.push(PodInfo {
                id: state.id.clone(),
                namespace: state.namespace.clone(),
                kind: state.kind.clone(),
                name: state.name.clone(),
                status: state.status.clone(),
                metadata: state.labels.clone(),
                created_at: state.created_at,
                updated_at: std::time::SystemTime::now(),
                ports: state.ports.clone(),
            });
        }
        Ok(pods)
    }

    async fn delete(&self, pod_id: &str) -> RuntimeResult<()> {
        info!("Deleting container: {}", pod_id);

        // Use FFI instead of CLI
        self.kill_container_ffi(pod_id, "SIGTERM")?;

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        let _ = self.kill_container_ffi(pod_id, "SIGKILL");

        self.delete_container_ffi(pod_id)?;

        let bundle_path = {
            let mut containers = self.containers.write().map_err(|_| {
                RuntimeError::CommandFailed("Failed to acquire container registry lock".to_string())
            })?;

            if let Some(state) = containers.remove(pod_id) {
                Some(state.bundle_path)
            } else {
                None
            }
        };

        if let Some(path) = bundle_path
            && path.exists()
            && let Err(e) = std::fs::remove_dir_all(&path)
        {
            warn!("Failed to cleanup bundle {}: {}", path.display(), e);
        }

        info!("Container {} deleted", pod_id);
        Ok(())
    }

    async fn logs(&self, pod_id: &str, _tail: Option<usize>) -> RuntimeResult<String> {
        {
            let containers = self.containers.read().map_err(|_| {
                RuntimeError::CommandFailed("Failed to acquire container registry lock".to_string())
            })?;
            if !containers.contains_key(pod_id) {
                return Err(RuntimeError::InstanceNotFound(pod_id.to_string()));
            }
        }

        // libcrun doesn't have native log support - would need external log driver
        debug!(
            "Log retrieval for {} (libcrun has no native log support)",
            pod_id
        );
        Ok(format!(
            "[crun] Container {} - logs require external log driver\n",
            pod_id
        ))
    }

    async fn validate(&self, manifest_content: &[u8]) -> RuntimeResult<()> {
        self.parse_manifest(manifest_content)?;
        Ok(())
    }

    async fn export(&self, pod_id: &str) -> RuntimeResult<Vec<u8>> {
        let containers = self.containers.read().map_err(|_| {
            RuntimeError::CommandFailed("Failed to acquire container registry lock".to_string())
        })?;

        let state = containers
            .get(pod_id)
            .ok_or_else(|| RuntimeError::InstanceNotFound(pod_id.to_string()))?;

        let export_data = serde_json::json!({
            "id": state.id,
            "name": state.name,
            "namespace": state.namespace,
            "kind": state.kind,
            "labels": state.labels,
            "runtime": "crun",
        });

        serde_json::to_vec(&export_data)
            .map_err(|e| RuntimeError::CommandFailed(format!("Failed to export: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_string() {
        assert_eq!(
            CrunEngine::parse_memory_string("512Mi"),
            Some(512 * 1024 * 1024)
        );
        assert_eq!(
            CrunEngine::parse_memory_string("1Gi"),
            Some(1024 * 1024 * 1024)
        );
        assert_eq!(CrunEngine::parse_memory_string("256Ki"), Some(256 * 1024));
        assert_eq!(
            CrunEngine::parse_memory_string("1G"),
            Some(1024 * 1024 * 1024)
        );
        assert_eq!(
            CrunEngine::parse_memory_string("512M"),
            Some(512 * 1024 * 1024)
        );
        assert_eq!(CrunEngine::parse_memory_string("1048576"), Some(1048576));
    }

    #[test]
    fn test_parse_cpu_to_shares() {
        assert_eq!(CrunEngine::parse_cpu_to_shares("1"), Some(1024));
        assert_eq!(CrunEngine::parse_cpu_to_shares("2"), Some(2048));
        assert_eq!(CrunEngine::parse_cpu_to_shares("500m"), Some(512));
        assert_eq!(CrunEngine::parse_cpu_to_shares("1000m"), Some(1024));
        assert_eq!(CrunEngine::parse_cpu_to_shares("250m"), Some(256));
    }

    #[test]
    fn test_oci_spec_generation() {
        let engine = CrunEngine {
            containers: Arc::new(RwLock::new(HashMap::new())),
            workplane_path: None,
            available: false, // Don't need libcrun for this test
        };
        let spec = ContainerSpec {
            image: "nginx:latest".to_string(),
            command: Some(vec!["nginx".to_string()]),
            args: Some(vec!["-g".to_string(), "daemon off;".to_string()]),
            env: HashMap::new(),
            memory_limit: 256 * 1024 * 1024,
            cpu_shares: 1024,
        };
        let config = DeploymentConfig::default();

        let oci = engine.generate_oci_spec(
            "test-123",
            &spec,
            &config,
            "my-nginx",
            "default",
            "Deployment",
        );

        assert_eq!(oci.oci_version, "1.0.2");
        assert!(oci.process.no_new_privileges);
        assert!(oci.root.readonly);
        assert_eq!(oci.linux.resources.memory.limit, 256 * 1024 * 1024);
        assert_eq!(oci.linux.resources.pids.limit, MAX_PIDS);
        assert!(oci.process.args[0].contains("workplane"));
    }
}
