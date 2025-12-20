//! # KrunEngine - libkrun MicroVM Runtime
//!
//! This module provides microVM-based workload isolation using libkrun.
//! Each workload runs in a lightweight VM with hardware-assisted virtualization:
//! - Linux: KVM
//! - macOS: Hypervisor.framework (HVF)
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     KrunEngine                              │
//! ├─────────────────────────────────────────────────────────────┤
//! │  1. Pull OCI image from registry                            │
//! │  2. Unpack layers → rootfs directory                        │
//! │  3. Inject workplane binary                                 │
//! │  4. Create microVM via libkrun                              │
//! │  5. Boot with workplane as PID 1                            │
//! └─────────────────────────────────────────────────────────────┘
//!                            │
//!                            ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      microVM                                │
//! │  ┌───────────────────────────────────────────────────────┐  │
//! │  │  workplane (PID 1)                                    │  │
//! │  │  ├── Mesh agent (Korium)                              │  │
//! │  │  └── Supervisor → spawns workload process             │  │
//! │  └───────────────────────────────────────────────────────┘  │
//! │                                                             │
//! │  Networking: TSI (Transparent Socket Impersonation)         │
//! │  Storage: virtio-fs (host directory → guest /)              │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Requirements
//!
//! - libkrun and libkrunfw installed on the host system
//! - KVM (Linux) or Hypervisor.framework (macOS)
//! - Embedded workplane binary (musl-linked)

use crate::runtimes::{
    DeploymentConfig, PodInfo, PodStatus, PortMapping, RuntimeEngine, RuntimeError, RuntimeResult,
};
use async_trait::async_trait;
use log::{debug, info, warn};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

// =============================================================================
// Constants
// =============================================================================

/// Default number of vCPUs for microVMs.
const DEFAULT_VCPUS: u32 = 1;

/// Default memory in MiB for microVMs.
const DEFAULT_MEMORY_MIB: u32 = 512;

/// Maximum memory in MiB for microVMs.
const MAX_MEMORY_MIB: u32 = 4096;

/// Maximum vCPUs for microVMs.
const MAX_VCPUS: u32 = 4;

/// Maximum number of concurrent VMs to prevent memory exhaustion.
const MAX_VMS: usize = 1024;

/// Maximum OCI image reference length to prevent injection attacks.
const MAX_IMAGE_REF_LEN: usize = 512;

/// Maximum size of a single OCI layer (512 MiB) to prevent disk exhaustion.
const MAX_LAYER_SIZE: usize = 512 * 1024 * 1024;

/// Maximum total rootfs size (4 GiB) to prevent disk exhaustion.
const MAX_ROOTFS_SIZE: u64 = 4 * 1024 * 1024 * 1024;

/// Returns the base directory for storing unpacked rootfs.
/// Uses ~/.magik/krun/rootfs or ./magik/krun/rootfs as fallback.
fn rootfs_base_dir() -> std::path::PathBuf {
    if let Some(home) = dirs::home_dir() {
        home.join(".magik").join("krun").join("rootfs")
    } else {
        std::path::PathBuf::from("./magik/krun/rootfs")
    }
}

/// Returns the directory for storing VM state.
/// Uses ~/.magik/krun/vms or ./magik/krun/vms as fallback.
fn vm_state_dir() -> std::path::PathBuf {
    if let Some(home) = dirs::home_dir() {
        home.join(".magik").join("krun").join("vms")
    } else {
        std::path::PathBuf::from("./magik/krun/vms")
    }
}

/// Label keys for metadata.
const POD_ID_LABEL_KEY: &str = "magik.io/pod-id";
const NAMESPACE_LABEL_KEY: &str = "magik.io/namespace";
const KIND_LABEL_KEY: &str = "magik.io/kind";
const NAME_LABEL_KEY: &str = "magik.io/name";

// =============================================================================
// VM State Tracking
// =============================================================================

/// State of a running microVM.
#[derive(Debug, Clone)]
struct VmState {
    /// Unique VM identifier
    id: String,
    /// Container/pod name
    name: String,
    /// Kubernetes namespace
    namespace: String,
    /// Resource kind
    kind: String,
    /// Resource name
    resource_name: String,
    /// Path to rootfs
    rootfs_path: PathBuf,
    /// Process ID of the VM (if running)
    pid: Option<u32>,
    /// Current status
    #[allow(dead_code)]
    status: PodStatus,
    /// Labels/metadata
    labels: HashMap<String, String>,
    /// Port mappings
    ports: Vec<PortMapping>,
    /// Creation timestamp
    created_at: std::time::SystemTime,
}

/// In-memory registry of running VMs.
type VmRegistry = Arc<RwLock<HashMap<String, VmState>>>;

// =============================================================================
// KrunEngine Implementation
// =============================================================================

/// MicroVM runtime engine using libkrun.
///
/// Provides hardware-isolated workload execution with:
/// - TSI networking (transparent socket impersonation)
/// - virtio-fs for rootfs mounting
/// - Workplane as PID 1 init supervisor
pub struct KrunEngine {
    /// Registry of running VMs
    vms: VmRegistry,
    /// Path to injected workplane binary
    workplane_path: Option<PathBuf>,
    /// Whether libkrun is available
    available: bool,
}

impl KrunEngine {
    /// Creates a new KrunEngine instance.
    ///
    /// Checks for libkrun availability at construction time.
    pub fn new() -> Self {
        let available = Self::check_libkrun_available();
        if !available {
            warn!("libkrun not available - microVM isolation will fail");
        }

        Self {
            vms: Arc::new(RwLock::new(HashMap::new())),
            workplane_path: crate::runtime::get_injected_workplane_path(),
            available,
        }
    }

    /// Checks if libkrun is available on the system.
    fn check_libkrun_available() -> bool {
        // SAFETY: krun_create_ctx and krun_free_ctx are paired correctly.
        // If creation fails (ctx < 0), we don't call free_ctx.
        // If creation succeeds, we immediately free the context.
        unsafe {
            let ctx = krun_sys::krun_create_ctx();
            if ctx >= 0 {
                krun_sys::krun_free_ctx(ctx as u32);
                return true;
            }
        }
        false
    }

    /// Ensures the rootfs and VM state directories exist.
    fn ensure_directories(&self) -> RuntimeResult<()> {
        std::fs::create_dir_all(rootfs_base_dir()).map_err(|e| {
            RuntimeError::IoError(std::io::Error::other(format!(
                "Failed to create rootfs directory: {}",
                e
            )))
        })?;
        std::fs::create_dir_all(vm_state_dir()).map_err(|e| {
            RuntimeError::IoError(std::io::Error::other(format!(
                "Failed to create VM state directory: {}",
                e
            )))
        })?;
        Ok(())
    }

    /// Extracts container specification from a K8s manifest.
    fn parse_container_spec(&self, doc: &serde_yaml::Value) -> RuntimeResult<ContainerSpec> {
        let spec = doc
            .get("spec")
            .ok_or_else(|| RuntimeError::InvalidManifest("Missing spec".to_string()))?;

        // Handle Pod, Deployment, StatefulSet, etc.
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

        // Parse resource limits
        let resources = container.get("resources");
        let memory_mib = resources
            .and_then(|r| r.get("limits"))
            .and_then(|l| l.get("memory"))
            .and_then(|m| m.as_str())
            .and_then(Self::parse_memory_string)
            .unwrap_or(DEFAULT_MEMORY_MIB);

        let vcpus = resources
            .and_then(|r| r.get("limits"))
            .and_then(|l| l.get("cpu"))
            .and_then(|c| c.as_str())
            .and_then(Self::parse_cpu_string)
            .unwrap_or(DEFAULT_VCPUS);

        Ok(ContainerSpec {
            image,
            command,
            args,
            env,
            memory_mib: memory_mib.min(MAX_MEMORY_MIB),
            vcpus: vcpus.min(MAX_VCPUS),
        })
    }

    /// Parses a Kubernetes memory string (e.g., "512Mi", "1Gi") to MiB.
    fn parse_memory_string(s: &str) -> Option<u32> {
        let s = s.trim();
        if let Some(v) = s.strip_suffix("Gi") {
            v.parse::<u32>().ok().map(|n| n * 1024)
        } else if let Some(v) = s.strip_suffix("Mi") {
            v.parse::<u32>().ok()
        } else if let Some(v) = s.strip_suffix('G') {
            v.parse::<u32>().ok().map(|n| n * 1024)
        } else if let Some(v) = s.strip_suffix('M') {
            v.parse::<u32>().ok()
        } else {
            // Assume bytes, convert to MiB
            s.parse::<u64>().ok().map(|v| (v / 1024 / 1024) as u32)
        }
    }

    /// Parses a Kubernetes CPU string (e.g., "1", "500m") to vCPU count.
    fn parse_cpu_string(s: &str) -> Option<u32> {
        let s = s.trim();
        if let Some(v) = s.strip_suffix('m') {
            // Millicores - round up to nearest vCPU
            v.parse::<u32>().ok().map(|m| m.div_ceil(1000))
        } else {
            s.parse::<u32>().ok()
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

    /// Detects the current platform for multi-arch image resolution.
    ///
    /// Returns (architecture, os) tuple using OCI platform naming conventions:
    /// - amd64, arm64, arm, etc. for architecture
    /// - linux, darwin for OS
    fn detect_platform() -> (&'static str, &'static str) {
        // Map Rust target_arch to OCI platform names
        let arch = if cfg!(target_arch = "x86_64") {
            "amd64"
        } else if cfg!(target_arch = "aarch64") {
            "arm64"
        } else if cfg!(target_arch = "arm") {
            "arm"
        } else {
            "unknown"
        };

        // For krun VMs, we always want Linux images regardless of host OS
        // because the VM runs a Linux kernel
        let os = "linux";

        (arch, os)
    }

    /// Pulls and unpacks an OCI image to a rootfs directory.
    ///
    /// # Security
    ///
    /// - Validates image reference length to prevent injection attacks
    /// - Limits layer sizes to prevent disk exhaustion
    /// - Sanitizes tar entries to prevent path traversal
    async fn prepare_rootfs(&self, image: &str, vm_id: &str) -> RuntimeResult<PathBuf> {
        use flate2::read::GzDecoder;
        use oci_distribution::{Client, Reference, secrets::RegistryAuth};
        use tar::Archive;

        // Validate image reference length to prevent injection attacks
        if image.len() > MAX_IMAGE_REF_LEN {
            return Err(RuntimeError::InvalidManifest(format!(
                "Image reference too long: {} > {} bytes",
                image.len(),
                MAX_IMAGE_REF_LEN
            )));
        }

        // Validate image reference contains only safe characters
        if !image.chars().all(|c| {
            c.is_ascii_alphanumeric()
                || c == '/'
                || c == ':'
                || c == '.'
                || c == '-'
                || c == '_'
                || c == '@'
        }) {
            return Err(RuntimeError::InvalidManifest(
                "Image reference contains invalid characters".to_string(),
            ));
        }

        let rootfs_path = rootfs_base_dir().join(vm_id);
        std::fs::create_dir_all(&rootfs_path).map_err(RuntimeError::IoError)?;

        info!("Pulling OCI image: {}", image);

        // Parse image reference
        let reference: Reference = image.parse().map_err(|e| {
            RuntimeError::DeploymentFailed(format!("Invalid image reference: {}", e))
        })?;

        // Create OCI client
        let client = Client::new(oci_distribution::client::ClientConfig {
            protocol: oci_distribution::client::ClientProtocol::Https,
            ..Default::default()
        });

        // Pull image manifest
        let auth = RegistryAuth::Anonymous;
        let (manifest, _digest) = client.pull_manifest(&reference, &auth).await.map_err(|e| {
            RuntimeError::DeploymentFailed(format!("Failed to pull manifest: {}", e))
        })?;

        // Get layer digests from manifest, resolving multi-arch if needed
        let layers = match manifest {
            oci_distribution::manifest::OciManifest::Image(img) => img.layers,
            oci_distribution::manifest::OciManifest::ImageIndex(index) => {
                // Detect current platform
                let (target_arch, target_os) = Self::detect_platform();
                info!(
                    "Multi-arch image detected, resolving for {}/{}",
                    target_os, target_arch
                );

                // Find matching manifest from the index
                let matching_manifest = index.manifests.iter().find(|m| {
                    if let Some(platform) = &m.platform {
                        platform.architecture == target_arch && platform.os == target_os
                    } else {
                        false
                    }
                });

                let manifest_desc = matching_manifest.ok_or_else(|| {
                    RuntimeError::DeploymentFailed(format!(
                        "No matching manifest for platform {}/{}. Available: {}",
                        target_os,
                        target_arch,
                        index
                            .manifests
                            .iter()
                            .filter_map(|m| m.platform.as_ref())
                            .map(|p| format!("{}/{}", p.os, p.architecture))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                })?;

                // Build a new reference with the digest for the platform-specific manifest
                let digest_ref_str = format!(
                    "{}/{}@{}",
                    reference.registry(),
                    reference.repository(),
                    manifest_desc.digest
                );
                let platform_ref: Reference = digest_ref_str.parse().map_err(|e| {
                    RuntimeError::DeploymentFailed(format!(
                        "Failed to build digest reference: {}",
                        e
                    ))
                })?;

                // Pull the platform-specific manifest using the digest
                let (platform_manifest, _) = client
                    .pull_manifest(&platform_ref, &auth)
                    .await
                    .map_err(|e| {
                        RuntimeError::DeploymentFailed(format!(
                            "Failed to pull platform manifest: {}",
                            e
                        ))
                    })?;

                match platform_manifest {
                    oci_distribution::manifest::OciManifest::Image(img) => img.layers,
                    _ => {
                        return Err(RuntimeError::DeploymentFailed(
                            "Nested image index not supported".to_string(),
                        ));
                    }
                }
            }
        };

        // Pull and unpack each layer with size validation
        let mut total_unpacked: u64 = 0;
        for layer in layers {
            debug!("Pulling layer: {}", layer.digest);

            // Pull layer into a buffer
            let mut layer_data: Vec<u8> = Vec::new();
            client
                .pull_blob(&reference, &layer, &mut layer_data)
                .await
                .map_err(|e| {
                    RuntimeError::DeploymentFailed(format!("Failed to pull layer: {}", e))
                })?;

            // Validate layer size to prevent disk exhaustion
            if layer_data.len() > MAX_LAYER_SIZE {
                // Cleanup on failure
                let _ = std::fs::remove_dir_all(&rootfs_path);
                return Err(RuntimeError::DeploymentFailed(format!(
                    "Layer {} exceeds maximum size: {} > {} bytes",
                    layer.digest,
                    layer_data.len(),
                    MAX_LAYER_SIZE
                )));
            }

            // Decompress and untar with path traversal protection
            let decoder = GzDecoder::new(&layer_data[..]);
            let mut archive = Archive::new(decoder);

            // Iterate entries manually to validate paths (prevent path traversal)
            for entry_result in archive.entries().map_err(|e| {
                RuntimeError::DeploymentFailed(format!("Failed to read tar entries: {}", e))
            })? {
                let mut entry = entry_result.map_err(|e| {
                    RuntimeError::DeploymentFailed(format!("Failed to read tar entry: {}", e))
                })?;

                // Get the path and validate it
                let entry_path = entry.path().map_err(|e| {
                    RuntimeError::DeploymentFailed(format!("Invalid entry path: {}", e))
                })?;

                // Security: Reject entries with path traversal attempts
                let path_str = entry_path.to_string_lossy();
                if path_str.contains("..") || path_str.starts_with('/') {
                    warn!("Rejecting suspicious tar entry: {}", path_str);
                    continue;
                }

                // Track total size
                total_unpacked += entry.size();
                if total_unpacked > MAX_ROOTFS_SIZE {
                    let _ = std::fs::remove_dir_all(&rootfs_path);
                    return Err(RuntimeError::DeploymentFailed(format!(
                        "Rootfs exceeds maximum size: {} > {} bytes",
                        total_unpacked, MAX_ROOTFS_SIZE
                    )));
                }

                // Unpack to rootfs
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

            // Set executable permissions
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
        Ok(rootfs_path)
    }

    /// Starts a microVM with the prepared rootfs.
    ///
    /// # Safety
    ///
    /// This function uses FFI calls to libkrun. The following invariants are maintained:
    /// - All CStrings are null-terminated and valid for the duration of the FFI calls
    /// - Context is freed on any error path before returning
    /// - The context handle is valid (created via krun_create_ctx)
    /// - String pointers remain valid while the FFI call executes
    fn start_vm(
        &self,
        vm_id: &str,
        rootfs_path: &std::path::Path,
        spec: &ContainerSpec,
        config: &DeploymentConfig,
        name: &str,
    ) -> RuntimeResult<u32> {
        use std::ffi::CString;

        info!(
            "Starting microVM {} with {} vCPUs, {} MiB RAM",
            vm_id, spec.vcpus, spec.memory_mib
        );

        // SAFETY: All libkrun FFI calls below follow these invariants:
        // 1. CStrings are created from validated inputs and remain live during FFI calls
        // 2. Context handle (ctx) is checked for validity after creation
        // 3. Context is freed on all error paths via krun_free_ctx
        // 4. Pointer arrays (argv, env_ptrs) are null-terminated as required by the C API
        // 5. krun_start_enter forks; parent gets child PID, child never returns
        unsafe {
            // Create VM context
            let ctx = krun_sys::krun_create_ctx();
            if ctx < 0 {
                return Err(RuntimeError::DeploymentFailed(
                    "Failed to create krun context".to_string(),
                ));
            }
            let ctx = ctx as u32;

            // Configure VM resources
            if krun_sys::krun_set_vm_config(ctx, spec.vcpus as u8, spec.memory_mib) != 0 {
                krun_sys::krun_free_ctx(ctx);
                return Err(RuntimeError::DeploymentFailed(
                    "Failed to set VM config".to_string(),
                ));
            }

            // Set rootfs
            let rootfs_cstr = CString::new(rootfs_path.to_string_lossy().as_bytes())
                .map_err(|_| RuntimeError::DeploymentFailed("Invalid rootfs path".to_string()))?;

            if krun_sys::krun_set_root(ctx, rootfs_cstr.as_ptr()) != 0 {
                krun_sys::krun_free_ctx(ctx);
                return Err(RuntimeError::DeploymentFailed(
                    "Failed to set rootfs".to_string(),
                ));
            }

            // Build workplane command line
            let mut exec_parts = spec.command.clone().unwrap_or_default();
            exec_parts.extend(spec.args.clone().unwrap_or_default());
            let exec_cmd = exec_parts.join(" ");

            let workplane_args = if exec_cmd.is_empty() {
                format!("/workplane --pod {} --replicas {}", name, config.replicas)
            } else {
                format!(
                    "/workplane --pod {} --replicas {} --exec \"{}\"",
                    name, config.replicas, exec_cmd
                )
            };

            // Set entrypoint (workplane as PID 1)
            let entrypoint = CString::new("/workplane").unwrap();
            let args_cstr = CString::new(workplane_args.as_bytes())
                .map_err(|_| RuntimeError::DeploymentFailed("Invalid args".to_string()))?;

            // Build environment
            let mut env_strs: Vec<CString> = Vec::new();
            for (k, v) in &spec.env {
                env_strs.push(CString::new(format!("{}={}", k, v)).unwrap());
            }
            for (k, v) in &config.env {
                env_strs.push(CString::new(format!("{}={}", k, v)).unwrap());
            }
            // Add pod metadata as env vars
            env_strs.push(CString::new(format!("MAGIK_POD_ID={}", vm_id)).unwrap());
            env_strs.push(CString::new(format!("MAGIK_POD_NAME={}", name)).unwrap());

            let mut env_ptrs: Vec<*const i8> = env_strs.iter().map(|s| s.as_ptr()).collect();
            env_ptrs.push(std::ptr::null());

            // Set workload (entrypoint + args + env)
            let argv = [entrypoint.as_ptr(), args_cstr.as_ptr(), std::ptr::null()];
            if krun_sys::krun_set_exec(ctx, entrypoint.as_ptr(), argv.as_ptr(), env_ptrs.as_ptr())
                != 0
            {
                krun_sys::krun_free_ctx(ctx);
                return Err(RuntimeError::DeploymentFailed(
                    "Failed to set exec".to_string(),
                ));
            }

            // Start the VM (this forks and returns the child PID)
            let ret = krun_sys::krun_start_enter(ctx);
            if ret < 0 {
                return Err(RuntimeError::DeploymentFailed(format!(
                    "Failed to start VM: {}",
                    ret
                )));
            }

            // The parent process returns here with the child PID
            // The child process enters the VM and never returns
            info!("MicroVM {} started with PID {}", vm_id, ret);
            Ok(ret as u32)
        }
    }

    /// Stops a running VM by PID.
    ///
    /// Sends SIGTERM first, then SIGKILL if SIGTERM fails.
    fn stop_vm(&self, pid: u32) -> RuntimeResult<()> {
        #[cfg(unix)]
        {
            #![allow(unused_imports)]
            use std::os::unix::process::CommandExt;

            // SAFETY: libc::kill with a valid signal (SIGTERM/SIGKILL) and PID is safe.
            // The PID was obtained from krun_start_enter and stored in our registry.
            // If the process doesn't exist, kill returns -1 which is handled.
            let result = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
            if result != 0 {
                // SAFETY: Same as above - SIGKILL is a valid signal number.
                unsafe { libc::kill(pid as i32, libc::SIGKILL) };
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            Err(RuntimeError::CommandFailed(
                "VM stop not supported on this platform".to_string(),
            ))
        }
    }

    /// Checks if a VM process is still running.
    fn is_vm_running(&self, pid: u32) -> bool {
        #[cfg(unix)]
        {
            // SAFETY: kill(pid, 0) is a standard POSIX idiom to check process existence.
            // Signal 0 doesn't actually send a signal - it just checks permissions.
            // Returns 0 if the process exists and we can signal it, -1 otherwise.
            unsafe { libc::kill(pid as i32, 0) == 0 }
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    /// Cleans up rootfs directory.
    fn cleanup_rootfs(&self, rootfs_path: &PathBuf) -> RuntimeResult<()> {
        if rootfs_path.exists() {
            std::fs::remove_dir_all(rootfs_path).map_err(RuntimeError::IoError)?;
        }
        Ok(())
    }
}

impl Default for KrunEngine {
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
    memory_mib: u32,
    vcpus: u32,
}

// =============================================================================
// RuntimeEngine Implementation
// =============================================================================

#[async_trait]
impl RuntimeEngine for KrunEngine {
    fn name(&self) -> &str {
        "krun"
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
                "libkrun not available".to_string(),
            ));
        }

        self.ensure_directories()?;

        // Parse manifest
        let manifest_str = String::from_utf8_lossy(manifest_content);
        let doc: serde_yaml::Value = serde_yaml::from_str(&manifest_str)
            .map_err(|e| RuntimeError::InvalidManifest(format!("YAML parse error: {}", e)))?;

        let (namespace, kind, resource_name) = self.parse_metadata(&doc);
        let container_spec = self.parse_container_spec(&doc)?;

        // Generate unique VM ID
        let vm_id = Uuid::new_v4().to_string();
        let vm_name = format!("{}.{}.{}", resource_name, namespace, &vm_id[..8]);

        // Prepare rootfs (pull image, unpack, inject workplane)
        let rootfs_path = self.prepare_rootfs(&container_spec.image, &vm_id).await?;

        // Start the microVM
        let pid = self.start_vm(&vm_id, &rootfs_path, &container_spec, config, &vm_name)?;

        // Build labels
        let mut labels = HashMap::new();
        labels.insert(POD_ID_LABEL_KEY.to_string(), vm_id.clone());
        labels.insert(NAMESPACE_LABEL_KEY.to_string(), namespace.clone());
        labels.insert(KIND_LABEL_KEY.to_string(), kind.clone());
        labels.insert(NAME_LABEL_KEY.to_string(), resource_name.clone());
        labels.insert("magik.io/isolation".to_string(), "microvm".to_string());
        labels.insert("magik.io/runtime".to_string(), "krun".to_string());

        // Store VM state with capacity check
        let now = std::time::SystemTime::now();
        let vm_state = VmState {
            id: vm_id.clone(),
            name: vm_name,
            namespace: namespace.clone(),
            kind: kind.clone(),
            resource_name: resource_name.clone(),
            rootfs_path,
            pid: Some(pid),
            status: PodStatus::Running,
            labels: labels.clone(),
            ports: vec![], // TSI handles networking transparently
            created_at: now,
        };

        {
            let mut vms = self.vms.write().map_err(|_| {
                RuntimeError::CommandFailed("Failed to acquire VM registry lock".to_string())
            })?;

            // Enforce VM limit to prevent memory exhaustion
            if vms.len() >= MAX_VMS {
                return Err(RuntimeError::DeploymentFailed(format!(
                    "Maximum VM limit reached: {} VMs. Delete existing VMs first.",
                    MAX_VMS
                )));
            }

            vms.insert(vm_id.clone(), vm_state);
        }

        Ok(PodInfo {
            id: vm_id,
            namespace,
            kind,
            name: resource_name,
            status: PodStatus::Running,
            metadata: labels,
            created_at: now,
            updated_at: now,
            ports: vec![],
        })
    }

    async fn get_status(&self, pod_id: &str) -> RuntimeResult<PodInfo> {
        let vms = self.vms.read().map_err(|_| {
            RuntimeError::CommandFailed("Failed to acquire VM registry lock".to_string())
        })?;

        let vm = vms
            .get(pod_id)
            .ok_or_else(|| RuntimeError::InstanceNotFound(format!("VM {} not found", pod_id)))?;

        let status = if let Some(pid) = vm.pid {
            if self.is_vm_running(pid) {
                PodStatus::Running
            } else {
                PodStatus::Stopped
            }
        } else {
            PodStatus::Unknown
        };

        Ok(PodInfo {
            id: vm.id.clone(),
            namespace: vm.namespace.clone(),
            kind: vm.kind.clone(),
            name: vm.resource_name.clone(),
            status,
            metadata: vm.labels.clone(),
            created_at: vm.created_at,
            updated_at: std::time::SystemTime::now(),
            ports: vm.ports.clone(),
        })
    }

    async fn list(&self) -> RuntimeResult<Vec<PodInfo>> {
        let vms = self.vms.read().map_err(|_| {
            RuntimeError::CommandFailed("Failed to acquire VM registry lock".to_string())
        })?;

        let mut pods = Vec::new();
        for vm in vms.values() {
            let status = if let Some(pid) = vm.pid {
                if self.is_vm_running(pid) {
                    PodStatus::Running
                } else {
                    PodStatus::Stopped
                }
            } else {
                PodStatus::Unknown
            };

            pods.push(PodInfo {
                id: vm.id.clone(),
                namespace: vm.namespace.clone(),
                kind: vm.kind.clone(),
                name: vm.resource_name.clone(),
                status,
                metadata: vm.labels.clone(),
                created_at: vm.created_at,
                updated_at: std::time::SystemTime::now(),
                ports: vm.ports.clone(),
            });
        }

        Ok(pods)
    }

    async fn delete(&self, pod_id: &str) -> RuntimeResult<()> {
        let vm = {
            let mut vms = self.vms.write().map_err(|_| {
                RuntimeError::CommandFailed("Failed to acquire VM registry lock".to_string())
            })?;
            vms.remove(pod_id)
        };

        let vm =
            vm.ok_or_else(|| RuntimeError::InstanceNotFound(format!("VM {} not found", pod_id)))?;

        // Stop the VM if running
        if let Some(pid) = vm.pid
            && self.is_vm_running(pid)
        {
            info!("Stopping microVM {} (PID {})", pod_id, pid);
            self.stop_vm(pid)?;
        }

        // Cleanup rootfs
        self.cleanup_rootfs(&vm.rootfs_path)?;

        info!("Deleted microVM: {}", pod_id);
        Ok(())
    }

    async fn logs(&self, pod_id: &str, _tail: Option<usize>) -> RuntimeResult<String> {
        // VMs don't have persistent logs in the same way containers do
        // Would need to implement vsock-based log streaming
        let vms = self.vms.read().map_err(|_| {
            RuntimeError::CommandFailed("Failed to acquire VM registry lock".to_string())
        })?;

        if vms.contains_key(pod_id) {
            Ok("Log streaming not yet implemented for microVMs".to_string())
        } else {
            Err(RuntimeError::InstanceNotFound(format!(
                "VM {} not found",
                pod_id
            )))
        }
    }

    async fn validate(&self, manifest_content: &[u8]) -> RuntimeResult<()> {
        let manifest_str = String::from_utf8_lossy(manifest_content);
        let doc: serde_yaml::Value = serde_yaml::from_str(&manifest_str)
            .map_err(|e| RuntimeError::InvalidManifest(format!("YAML parse error: {}", e)))?;

        // Validate we can parse the container spec
        let _ = self.parse_container_spec(&doc)?;

        Ok(())
    }

    async fn export(&self, pod_id: &str) -> RuntimeResult<Vec<u8>> {
        // Export VM state for migration
        let vms = self.vms.read().map_err(|_| {
            RuntimeError::CommandFailed("Failed to acquire VM registry lock".to_string())
        })?;

        let vm = vms
            .get(pod_id)
            .ok_or_else(|| RuntimeError::InstanceNotFound(format!("VM {} not found", pod_id)))?;

        // For now, just serialize the state - full migration would need
        // memory checkpointing which libkrun doesn't currently support
        let state = serde_json::json!({
            "id": vm.id,
            "name": vm.name,
            "namespace": vm.namespace,
            "kind": vm.kind,
            "resource_name": vm.resource_name,
            "labels": vm.labels,
        });

        Ok(state.to_string().into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_memory_string() {
        assert_eq!(KrunEngine::parse_memory_string("512Mi"), Some(512));
        assert_eq!(KrunEngine::parse_memory_string("1Gi"), Some(1024));
        assert_eq!(KrunEngine::parse_memory_string("2G"), Some(2048));
        assert_eq!(KrunEngine::parse_memory_string("256M"), Some(256));
        assert_eq!(KrunEngine::parse_memory_string("1073741824"), Some(1024)); // 1GB in bytes
    }

    #[test]
    fn test_parse_cpu_string() {
        assert_eq!(KrunEngine::parse_cpu_string("1"), Some(1));
        assert_eq!(KrunEngine::parse_cpu_string("2"), Some(2));
        assert_eq!(KrunEngine::parse_cpu_string("500m"), Some(1)); // Round up
        assert_eq!(KrunEngine::parse_cpu_string("1500m"), Some(2)); // Round up
        assert_eq!(KrunEngine::parse_cpu_string("1000m"), Some(1));
    }
}
