//! # Runtime Engine Layer
//!
//! This module provides the abstraction layer for workload runtime engines.
//! It handles manifest processing, engine selection, and deployment orchestration.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │                    Runtime Layer                            │
//! ├────────────────────────────────────────────────────────────┤
//! │  ApplyRequest ──▶ decode_manifest ──▶ Engine Selection     │
//! │                                              │               │
//! │                    ┌─────────────────────────▼───────────┐  │
//! │                    │       RuntimeRegistry               │  │
//! │                    │     ┌─────────┐  ┌─────────┐        │  │
//! │                    │     │  Krun   │  │  Crun   │        │  │
//! │                    │     │ (microVM)│  │(contain)│        │  │
//! │                    │     └─────────┘  └─────────┘        │  │
//! │                    └─────────────────────────────────────┘  │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Engine Selection
//!
//! Magik supports two runtime engines, selectable via Kubernetes annotation:
//!
//! ```yaml
//! metadata:
//!   annotations:
//!     magik.io/runtime: "crun"  # or "krun" (default)
//! ```
//!
//! - **krun** (default): Hardware-isolated microVMs via libkrun (KVM/HVF)
//! - **crun**: Container isolation for IoT/embedded devices without virtualization
//!
//! # Thread Safety
//!
//! The [`RuntimeRegistry`] is stored in a global [`RwLock`] for concurrent access.

use crate::messages::ApplyRequest;
use crate::runtimes::{DeploymentConfig, InfraConfig, RuntimeRegistry, create_default_registry};
use base64::Engine;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

/// Maximum allowed manifest size (1 MiB) to prevent memory exhaustion
/// from oversized or malicious YAML payloads.
pub const MAX_MANIFEST_SIZE: usize = 1024 * 1024;

use tokio::sync::RwLock;

/// Global runtime registry instance.
/// Protected by RwLock for concurrent read access and exclusive write during initialization.
static RUNTIME_REGISTRY: LazyLock<Arc<RwLock<Option<RuntimeRegistry>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(None)));

/// Global path to the injected workplane binary.
/// Set during initialization, used by microVM deployments.
static INJECTED_WORKPLANE_PATH: LazyLock<std::sync::RwLock<Option<std::path::PathBuf>>> =
    LazyLock::new(|| std::sync::RwLock::new(None));

/// Returns the embedded workplane binary if available.
///
/// The binary is embedded at compile time if it exists at the expected path.
/// Build with: `cargo build -p workplane --release --target x86_64-unknown-linux-musl`
/// then rebuild machineplane.
fn get_embedded_workplane_binary() -> Option<&'static [u8]> {
    #[cfg(feature = "embedded-workplane")]
    {
        Some(include_bytes!(env!("WORKPLANE_BINARY_PATH")))
    }
    #[cfg(not(feature = "embedded-workplane"))]
    {
        None
    }
}

/// Injects the embedded workplane binary to the filesystem for microVM bind-mounting.
///
/// Writes the binary to `/var/lib/magik/bin/workplane` (if writable) or falls
/// back to a temp directory. Sets executable permissions (0o755).
///
/// # Returns
///
/// The path to the injected binary, or `None` if no binary is embedded.
fn inject_workplane_binary() -> Option<std::path::PathBuf> {
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    let binary = get_embedded_workplane_binary()?;

    // Try the preferred system path first
    let preferred_path = std::path::PathBuf::from("/var/lib/magik/bin/workplane");

    // Attempt to write to preferred location
    if let Some(parent) = preferred_path.parent() {
        if fs::create_dir_all(parent).is_ok() {
            if let Ok(mut file) = fs::File::create(&preferred_path) {
                if file.write_all(binary).is_ok() {
                    let mut perms = file.metadata().ok()?.permissions();
                    perms.set_mode(0o755);
                    if fs::set_permissions(&preferred_path, perms).is_ok() {
                        info!(
                            "Injected workplane binary to {} ({} bytes)",
                            preferred_path.display(),
                            binary.len()
                        );
                        return Some(preferred_path);
                    }
                }
            }
        }
    }

    // Fallback to temp directory
    let temp_dir = std::env::temp_dir().join("magik");
    if fs::create_dir_all(&temp_dir).is_err() {
        error!("Failed to create temp directory for workplane binary");
        return None;
    }

    let temp_path = temp_dir.join("workplane");
    let mut file = match fs::File::create(&temp_path) {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to create workplane binary file: {}", e);
            return None;
        }
    };

    if let Err(e) = file.write_all(binary) {
        error!("Failed to write workplane binary: {}", e);
        return None;
    }

    // Set executable permissions
    let mut perms = match file.metadata() {
        Ok(m) => m.permissions(),
        Err(e) => {
            error!("Failed to get file metadata: {}", e);
            return None;
        }
    };
    perms.set_mode(0o755);
    if let Err(e) = fs::set_permissions(&temp_path, perms) {
        error!("Failed to set executable permissions: {}", e);
        return None;
    }

    info!(
        "Injected workplane binary to {} ({} bytes)",
        temp_path.display(),
        binary.len()
    );
    Some(temp_path)
}

/// Returns the path to the injected workplane binary.
///
/// Must be called after `initialize_runtime()`.
/// Returns `None` if no binary was injected.
pub fn get_injected_workplane_path() -> Option<std::path::PathBuf> {
    INJECTED_WORKPLANE_PATH
        .read()
        .ok()
        .and_then(|guard| guard.clone())
}

/// Initializes the runtime registry with available engines.
///
/// Registers both KrunEngine (microVM) and CrunEngine (container).
/// At least one engine must be available for the node to function.
///
/// # Errors
///
/// Returns error if no runtime engines are available.
pub async fn initialize_runtime() -> Result<(), Box<dyn std::error::Error>> {
    info!("Initializing runtime registry");

    // Inject workplane binary for microVM bind-mounts
    if let Some(path) = inject_workplane_binary() {
        let mut guard = INJECTED_WORKPLANE_PATH
            .write()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?;
        *guard = Some(path.clone());
        info!("Workplane binary injected at: {}", path.display());
    } else {
        // Determine the correct target architecture for the message
        let target_arch = if cfg!(target_arch = "aarch64") {
            "aarch64-unknown-linux-musl"
        } else {
            "x86_64-unknown-linux-musl"
        };
        warn!(
            "Workplane binary not available for injection. \
             Deployments will fail. Build with:\n\
             1. cargo build -p workplane --release --target {}\n\
             2. cargo build -p machineplane --release",
            target_arch
        );
    }

    let registry = create_default_registry().await;
    let available_engines = registry.check_available_engines().await;

    info!("Available runtime engines: {:?}", available_engines);

    // Check that at least one engine is available
    let krun_available = *available_engines.get("krun").unwrap_or(&false);
    let crun_available = *available_engines.get("crun").unwrap_or(&false);

    if !krun_available && !crun_available {
        return Err("No runtime engines available. Install libkrun (recommended) or crun.".into());
    }

    if krun_available {
        info!("KrunEngine available for microVM isolation");
    } else {
        warn!("KrunEngine not available - microVM isolation disabled");
    }

    if crun_available {
        info!("CrunEngine available for container isolation (IoT/embedded)");
    } else {
        debug!("CrunEngine not available - container isolation disabled");
    }

    {
        let mut global_registry = RUNTIME_REGISTRY.write().await;
        *global_registry = Some(registry);
    }

    info!("Runtime registry initialized successfully");
    Ok(())
}

/// Annotation key for runtime engine selection.
const RUNTIME_ANNOTATION_KEY: &str = "magik.io/runtime";

/// Default runtime engine (microVM isolation).
const DEFAULT_RUNTIME: &str = "krun";

/// Valid runtime engine names.
const VALID_RUNTIMES: [&str; 2] = ["krun", "crun"];

/// Processes a manifest deployment request end-to-end.
///
/// # Workflow
///
/// 1. Decodes manifest content (handles base64 or raw YAML)
/// 2. Extracts metadata (namespace, kind, name)
/// 3. Extracts runtime annotation (defaults to "krun")
/// 4. Modifies replicas to 1 (for distributed scheduling)
/// 5. Deploys via selected engine (krun or crun)
///
/// # Runtime Selection
///
/// The runtime is selected via the `magik.io/runtime` annotation:
///
/// ```yaml
/// metadata:
///   annotations:
///     magik.io/runtime: "crun"  # or "krun" (default)
/// ```
///
/// - **krun** (default): Hardware-isolated microVMs via libkrun (KVM/HVF)
/// - **crun**: Container isolation for IoT/embedded devices
///
/// # Arguments
///
/// * `apply_req` - The apply request with operation context
/// * `manifest_json` - Manifest content (base64-encoded or raw YAML)
///
/// # Returns
///
/// The deployed pod ID on success.
///
/// # Errors
///
/// Returns error if:
/// - Runtime registry is not initialized
/// - Selected engine is not available
/// - Deployment fails
pub async fn process_manifest_deployment(
    apply_req: &ApplyRequest,
    manifest_json: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    info!("Processing manifest deployment");

    let manifest_content = decode_manifest_content(manifest_json);

    // Reject oversized manifests before parsing to prevent YAML bombs
    if manifest_content.len() > MAX_MANIFEST_SIZE {
        return Err(format!(
            "Manifest size {} exceeds maximum allowed size {}",
            manifest_content.len(),
            MAX_MANIFEST_SIZE
        )
        .into());
    }

    let manifest_str = String::from_utf8_lossy(&manifest_content);
    let (namespace, kind, name, runtime_engine) =
        if let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&manifest_str) {
            let ns = doc
                .get("metadata")
                .and_then(|m| m.get("namespace"))
                .and_then(|n| n.as_str())
                .unwrap_or("default");
            let k = doc.get("kind").and_then(|k| k.as_str()).unwrap_or("Pod");
            let n = doc
                .get("metadata")
                .and_then(|m| m.get("name"))
                .and_then(|n| n.as_str())
                .unwrap_or("unknown");

            // Extract runtime annotation
            let rt = doc
                .get("metadata")
                .and_then(|m| m.get("annotations"))
                .and_then(|a| a.get(RUNTIME_ANNOTATION_KEY))
                .and_then(|r| r.as_str())
                .unwrap_or(DEFAULT_RUNTIME);

            // Validate runtime selection
            let validated_rt = if VALID_RUNTIMES.contains(&rt) {
                rt
            } else {
                warn!(
                    "Invalid runtime '{}' in annotation, falling back to '{}'",
                    rt, DEFAULT_RUNTIME
                );
                DEFAULT_RUNTIME
            };

            (
                ns.to_string(),
                k.to_string(),
                n.to_string(),
                validated_rt.to_string(),
            )
        } else {
            (
                "default".to_string(),
                "Pod".to_string(),
                "unknown".to_string(),
                DEFAULT_RUNTIME.to_string(),
            )
        };

    info!(
        "Processing manifest deployment for {}/{}/{} using {} engine",
        namespace, kind, name, runtime_engine
    );

    let modified_manifest_content = modify_manifest_replicas(&manifest_content)?;
    let deployment_config = create_deployment_config(apply_req);

    let registry_guard = RUNTIME_REGISTRY.read().await;
    let registry = registry_guard
        .as_ref()
        .ok_or("Runtime registry not initialized")?;

    let engine = registry.get_engine(&runtime_engine).ok_or_else(|| {
        format!(
            "{} engine not available. {}",
            runtime_engine,
            if runtime_engine == "krun" {
                "Install libkrun: https://github.com/containers/libkrun"
            } else {
                "Install crun: https://github.com/containers/crun"
            }
        )
    })?;

    let pod_info = engine
        .apply(&modified_manifest_content, &deployment_config)
        .await?;

    info!(
        "{} deployed successfully via {}: {}, status: {:?}",
        kind, runtime_engine, pod_info.id, pod_info.status
    );

    Ok(pod_info.id)
}

/// Modifies a manifest to set replicas to 1.
///
/// In the distributed scheduling model, each node runs exactly one replica.
/// The scheduler distributes replicas across multiple nodes.
fn modify_manifest_replicas(
    manifest_content: &[u8],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let manifest_str = String::from_utf8_lossy(manifest_content);

    if let Ok(mut doc) = serde_yaml::from_str::<serde_yaml::Value>(&manifest_str) {
        if let Some(spec) = doc.get_mut("spec") {
            if let Some(spec_map) = spec.as_mapping_mut() {
                spec_map.insert(
                    serde_yaml::Value::String("replicas".to_string()),
                    serde_yaml::Value::Number(serde_yaml::Number::from(1)),
                );
            }
        } else if doc.get("replicas").is_some()
            && let Some(doc_map) = doc.as_mapping_mut()
        {
            doc_map.insert(
                serde_yaml::Value::String("replicas".to_string()),
                serde_yaml::Value::Number(serde_yaml::Number::from(1)),
            );
        }

        let modified_yaml = serde_yaml::to_string(&doc)?;
        info!("Modified manifest to set replicas=1 for single-node deployment");
        Ok(modified_yaml.into_bytes())
    } else {
        warn!("Failed to parse manifest for replica modification, using original");
        Ok(manifest_content.to_vec())
    }
}

/// Decodes manifest content from base64 or returns raw content.
pub fn decode_manifest_content(manifest_json: &str) -> Vec<u8> {
    match base64::engine::general_purpose::STANDARD.decode(manifest_json) {
        Ok(decoded) => {
            if String::from_utf8(decoded.clone()).is_ok() {
                debug!("Decoded base64 manifest content ({} bytes)", decoded.len());
                decoded
            } else {
                debug!("Base64 content was not valid UTF-8; using raw manifest");
                manifest_json.as_bytes().to_vec()
            }
        }
        Err(_) => manifest_json.as_bytes().to_vec(),
    }
}

/// Creates deployment configuration from an apply request.
///
/// Configures the workplane agent to run in the microVM, passing workload
/// identity via command-line arguments.
fn create_deployment_config(apply_req: &ApplyRequest) -> DeploymentConfig {
    let mut config = DeploymentConfig {
        replicas: apply_req.replicas,
        ..Default::default()
    };

    if !apply_req.operation_id.is_empty() {
        config.env.insert(
            "BEEMESH_OPERATION_ID".to_string(),
            apply_req.operation_id.clone(),
        );
    }

    // Extract workload metadata from the manifest
    let (namespace, kind, name) = extract_workload_metadata(&apply_req.manifest_json);

    // Build command-line arguments for the workplane agent
    let command = vec![
        "/workplane".to_string(),
        "--namespace".to_string(),
        namespace,
        "--workload".to_string(),
        name.clone(),
        "--workload-kind".to_string(),
        kind,
        "--replicas".to_string(),
        apply_req.replicas.to_string(),
        "--pod".to_string(),
        name,
    ];

    // Configure workplane agent to run in microVM
    config.infra = Some(InfraConfig {
        binary_path: get_injected_workplane_path().map(|p| p.to_string_lossy().to_string()),
        args: Some(command),
        env: HashMap::new(),
    });

    config
}

/// Extracts workload metadata (namespace, kind, name) from a manifest.
fn extract_workload_metadata(manifest_json: &str) -> (String, String, String) {
    let manifest_content = decode_manifest_content(manifest_json);
    let manifest_str = String::from_utf8_lossy(&manifest_content);

    if let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&manifest_str) {
        let namespace = doc
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|n| n.as_str())
            .unwrap_or("default")
            .to_string();
        let kind = doc
            .get("kind")
            .and_then(|k| k.as_str())
            .unwrap_or("Pod")
            .to_string();
        let name = doc
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("unknown")
            .to_string();
        (namespace, kind, name)
    } else {
        (
            "default".to_string(),
            "Pod".to_string(),
            "unknown".to_string(),
        )
    }
}

/// Processes a self-apply request (local deployment without mesh routing).
pub async fn process_enhanced_self_apply_request(manifest: &[u8]) {
    debug!(
        "Processing enhanced self-apply request (manifest len={})",
        manifest.len()
    );

    match crate::messages::deserialize_safe::<ApplyRequest>(manifest) {
        Ok(apply_req) => {
            debug!(
                "Enhanced self-apply request - operation_id={:?} replicas={}",
                apply_req.operation_id, apply_req.replicas
            );

            if !apply_req.manifest_json.is_empty() {
                let manifest_json = apply_req.manifest_json.clone();

                match process_manifest_deployment(&apply_req, &manifest_json).await {
                    Ok(pod_id) => {
                        info!("Successfully deployed self-applied pod: {}", pod_id);
                    }
                    Err(e) => {
                        error!("Failed to deploy self-applied pod: {}", e);
                    }
                }
            } else {
                warn!("Self-apply request missing manifest JSON");
            }
        }
        Err(e) => {
            error!("Failed to parse self-apply request: {}", e);
        }
    }
}

/// Returns availability status for all registered runtime engines.
pub async fn get_runtime_registry_stats() -> HashMap<String, bool> {
    if let Some(registry) = RUNTIME_REGISTRY.read().await.as_ref() {
        registry.check_available_engines().await
    } else {
        HashMap::new()
    }
}

/// Lists names of all registered runtime engines.
pub async fn list_available_engines() -> Vec<String> {
    if let Some(registry) = RUNTIME_REGISTRY.read().await.as_ref() {
        registry
            .list_engines()
            .iter()
            .map(|s| s.to_string())
            .collect()
    } else {
        Vec::new()
    }
}

/// Deletes all pods matching a resource specification.
///
/// # Arguments
///
/// * `namespace` - Kubernetes namespace
/// * `kind` - Resource kind (e.g., "Deployment", "Pod")
/// * `name` - Resource name
///
/// # Returns
///
/// List of deleted pod IDs.
pub async fn delete_pods_by_resource(
    namespace: &str,
    kind: &str,
    name: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let resource_key = format!("{}/{}/{}", namespace, kind, name);
    info!("delete_pods_by_resource: resource={}", resource_key);

    let registry_guard = RUNTIME_REGISTRY.read().await;
    let registry = registry_guard
        .as_ref()
        .ok_or("Runtime registry not initialized")?;

    let mut deleted_pods = Vec::new();

    if let Some(engine) = registry.get_engine("krun") {
        match engine.list().await {
            Ok(pods) => {
                for pod in pods {
                    if pod.namespace == namespace && pod.kind == kind && pod.name == name {
                        info!(
                            "Found matching pod: {} (resource: {})",
                            pod.id, resource_key
                        );

                        match engine.delete(&pod.id).await {
                            Ok(()) => {
                                info!("Successfully deleted pod: {}", pod.id);
                                deleted_pods.push(pod.id);
                            }
                            Err(e) => {
                                error!("Failed to delete pod {}: {}", pod.id, e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to list pods: {}", e);
            }
        }
    }

    info!(
        "delete_pods_by_resource completed: deleted {} pods for resource '{}'",
        deleted_pods.len(),
        resource_key
    );
    Ok(deleted_pods)
}

/// Lists all pods across all runtime engines.
///
/// Returns pods from the KrunEngine (microVM runtime).
pub async fn list_all_pods() -> Result<Vec<crate::runtimes::PodInfo>, Box<dyn std::error::Error>> {
    let registry_guard = RUNTIME_REGISTRY.read().await;
    let registry = registry_guard
        .as_ref()
        .ok_or("Runtime registry not initialized")?;

    if let Some(engine) = registry.get_engine("krun") {
        Ok(engine.list().await?)
    } else {
        Ok(Vec::new())
    }
}

/// Gets a specific pod by ID.
pub async fn get_pod(pod_id: &str) -> Result<crate::runtimes::PodInfo, Box<dyn std::error::Error>> {
    let registry_guard = RUNTIME_REGISTRY.read().await;
    let registry = registry_guard
        .as_ref()
        .ok_or("Runtime registry not initialized")?;

    if let Some(engine) = registry.get_engine("krun") {
        Ok(engine.get_status(pod_id).await?)
    } else {
        Err("KrunEngine not available".into())
    }
}

/// Deletes a pod by ID.
pub async fn delete_pod(pod_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let registry_guard = RUNTIME_REGISTRY.read().await;
    let registry = registry_guard
        .as_ref()
        .ok_or("Runtime registry not initialized")?;

    if let Some(engine) = registry.get_engine("krun") {
        Ok(engine.delete(pod_id).await?)
    } else {
        Err("KrunEngine not available".into())
    }
}

/// Gets logs from a pod.
pub async fn get_pod_logs(
    pod_id: &str,
    tail: Option<usize>,
) -> Result<String, Box<dyn std::error::Error>> {
    let registry_guard = RUNTIME_REGISTRY.read().await;
    let registry = registry_guard
        .as_ref()
        .ok_or("Runtime registry not initialized")?;

    if let Some(engine) = registry.get_engine("krun") {
        Ok(engine.logs(pod_id, tail).await?)
    } else {
        Err("KrunEngine not available".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_config_creation() {
        let config = DeploymentConfig::default();
        assert_eq!(config.replicas, 1);
        assert!(config.env.is_empty());
    }

    #[test]
    fn decode_manifest_content_handles_base64_and_plain() {
        let manifest = "apiVersion: v1\nkind: Pod";
        let encoded = base64::engine::general_purpose::STANDARD.encode(manifest);

        let decoded = decode_manifest_content(&encoded);
        assert_eq!(decoded, manifest.as_bytes());

        let plaintext = decode_manifest_content(manifest);
        assert_eq!(plaintext, manifest.as_bytes());
    }
}
