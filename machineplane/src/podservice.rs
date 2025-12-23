//! # Pod Service Layer
//!
//! This module provides the integration layer between machineplane and
//! the `magikpod` crate for pod lifecycle management.
//!
//! # Architecture
//!
//! - **Machineplane** owns workload abstractions (Deployment, StatefulSet, DaemonSet)
//! - **magikpod** handles pod-level operations (single pod lifecycle)
//!
//! # Runtime Selection
//!
//! Default runtime: `pod-microvm-containers` (MicroVmPodManager via libkrun)
//!
//! Override via annotation:
//! ```yaml
//! metadata:
//!   annotations:
//!     magik.io/runtime-class: pod-wasm  # or pod-containers
//! ```
//!
//! # Security Model
//!
//! - Default to hardware-isolated microVMs for zero-trust workloads
//! - Native containers available on Linux via annotation (lower isolation)
//! - WASM modules for cross-platform lightweight isolation

use anyhow::Result;
use log::{info, warn};
use magikpod::{
    MicroVmPodManager, PodManagerRegistry, PodOrchestrator, PodPhase, PodService, PodSummary,
    WasmPodManager,
};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::SystemTime;

/// Default runtime class for Magik workloads.
///
/// MicroVMs provide hardware-level isolation via libkrun (Linux KVM / macOS HVF).
pub const DEFAULT_RUNTIME_CLASS: &str = "pod-microvm-containers";

/// Annotation key for overriding runtime class.
pub const RUNTIME_CLASS_ANNOTATION: &str = "magik.io/runtime-class";

/// Default grace period for pod deletion (seconds).
pub const DEFAULT_GRACE_PERIOD_SECS: u32 = 30;

/// Global pod service singleton.
///
/// Initialized once at daemon startup via [`initialize`].
static PODSERVICE: OnceLock<Arc<dyn PodService>> = OnceLock::new();

/// Initializes the pod service with the runtime registry.
///
/// Registers available pod managers:
/// - `pod-microvm-containers` (default): MicroVMs via libkrun
/// - `pod-containers`: Native containers via youki (Linux only)
/// - `pod-wasm`: WASM modules via wasmtime
///
/// # Errors
///
/// Returns error if:
/// - libkrun initialization fails (required for default runtime)
/// - Already initialized (can only be called once)
pub async fn initialize() -> Result<()> {
    let mut registry = PodManagerRegistry::new();

    // Primary: MicroVM (default for Magik's security model)
    info!("Registering MicroVmPodManager (pod-microvm-containers)...");
    let krun = magikpod::KrunRuntime::new();
    registry.register(Box::new(MicroVmPodManager::new(Box::new(krun))));
    info!("MicroVmPodManager registered successfully");

    // Optional: Native containers (Linux only)
    #[cfg(target_os = "linux")]
    {
        info!("Registering NativePodManager (pod-containers)...");
        let native = magikpod::NativeRuntime::new();
        registry.register(Box::new(magikpod::NativePodManager::new(Box::new(native))));
        info!("NativePodManager registered successfully");
    }

    // Optional: WASM modules
    info!("Registering WasmPodManager (pod-wasm)...");
    let wasm = magikpod::WasmtimeRuntime::new();
    registry.register(Box::new(WasmPodManager::new(Box::new(wasm))));
    info!("WasmPodManager registered successfully");

    let orchestrator = Arc::new(PodOrchestrator::new(registry));

    PODSERVICE
        .set(orchestrator)
        .map_err(|_| anyhow::anyhow!("pod service already initialized"))?;

    info!("Pod service initialized with default runtime: {}", DEFAULT_RUNTIME_CLASS);
    Ok(())
}

/// Returns a reference to the global pod service.
///
/// # Panics
///
/// Panics if called before [`initialize`].
pub fn service() -> &'static Arc<dyn PodService> {
    PODSERVICE.get().expect("pod service not initialized - call podservice::initialize() first")
}

/// Preprocesses a Kubernetes manifest before passing to magikpod.
///
/// # Actions
///
/// 1. Injects `runtimeClassName` from annotation or default
/// 2. Ensures namespace is set (defaults to "default")
///
/// # Arguments
///
/// * `manifest_yaml` - Raw YAML manifest string
///
/// # Returns
///
/// Preprocessed manifest as YAML bytes ready for `run_pod_from_manifest`.
pub fn preprocess_manifest(manifest_yaml: &str) -> Result<Vec<u8>> {
    let mut doc: serde_yaml::Value = serde_yaml::from_str(manifest_yaml)?;

    // Read runtime class from annotation if present, clone to avoid borrow issues
    let runtime_class = doc
        .get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get(RUNTIME_CLASS_ANNOTATION))
        .and_then(|v| v.as_str())
        .unwrap_or(DEFAULT_RUNTIME_CLASS)
        .to_string();

    // Inject into spec.runtimeClassName
    if let Some(spec) = doc.get_mut("spec") {
        if let serde_yaml::Value::Mapping(spec_map) = spec {
            spec_map.insert(
                serde_yaml::Value::String("runtimeClassName".to_string()),
                serde_yaml::Value::String(runtime_class.clone()),
            );
        }
    }

    // Ensure namespace is set
    if let Some(metadata) = doc.get_mut("metadata") {
        if let serde_yaml::Value::Mapping(meta_map) = metadata {
            if !meta_map.contains_key(&serde_yaml::Value::String("namespace".to_string())) {
                meta_map.insert(
                    serde_yaml::Value::String("namespace".to_string()),
                    serde_yaml::Value::String("default".to_string()),
                );
            }
        }
    }

    Ok(serde_yaml::to_string(&doc)?.into_bytes())
}

/// Deploys a pod from a manifest.
///
/// Preprocesses the manifest to inject default runtime class,
/// then delegates to the pod service.
///
/// # Arguments
///
/// * `manifest_yaml` - Raw YAML manifest string
///
/// # Returns
///
/// The pod ID as a string on success.
pub async fn deploy_pod(manifest_yaml: &str) -> Result<String> {
    let processed = preprocess_manifest(manifest_yaml)?;
    let pod_id = service().run_pod_from_manifest(&processed).await?;
    Ok(pod_id.to_string())
}

/// Lists all pods managed by the pod service.
pub async fn list_pods() -> Result<Vec<PodSummary>> {
    service().list_pods().await.map_err(Into::into)
}

/// Deletes pods matching the given workload resource.
///
/// Filters pods by labels:
/// - `magik.io/workload-namespace` or namespace
/// - `magik.io/workload-kind` or kind  
/// - `magik.io/workload-name` or name
///
/// # Arguments
///
/// * `namespace` - Kubernetes namespace
/// * `kind` - Workload kind (Deployment, StatefulSet, etc.)
/// * `name` - Workload name
///
/// # Returns
///
/// Vector of deleted pod IDs.
pub async fn delete_pods_by_resource(
    namespace: &str,
    kind: &str,
    name: &str,
) -> Result<Vec<String>> {
    let pods = service().list_pods().await?;
    let mut deleted = Vec::new();

    for pod in pods {
        // Check namespace match
        let pod_namespace = pod
            .labels
            .get("magik.io/workload-namespace")
            .map(String::as_str)
            .unwrap_or(&pod.namespace);

        if pod_namespace != namespace {
            continue;
        }

        // Check kind match
        let pod_kind = pod
            .labels
            .get("magik.io/workload-kind")
            .map(String::as_str)
            .unwrap_or("");

        if !kind.is_empty() && pod_kind != kind {
            continue;
        }

        // Check name match
        let pod_workload_name = pod
            .labels
            .get("magik.io/workload-name")
            .or_else(|| pod.labels.get("app.kubernetes.io/name"))
            .or_else(|| pod.labels.get("app"))
            .map(String::as_str)
            .unwrap_or(&pod.name);

        if pod_workload_name != name {
            continue;
        }

        // Delete this pod
        match service().delete_pod(&pod.id, DEFAULT_GRACE_PERIOD_SECS).await {
            Ok(()) => {
                info!("Deleted pod {} for resource {}/{}/{}", pod.id, namespace, kind, name);
                deleted.push(pod.id.to_string());
            }
            Err(e) => {
                warn!("Failed to delete pod {}: {}", pod.id, e);
            }
        }
    }

    Ok(deleted)
}

/// Gets a pod by ID or name.
///
/// Searches through pods to find a match by ID string.
///
/// # Returns
///
/// `Some(pod)` if found, `None` otherwise.
pub async fn get_pod(pod_id_or_name: &str) -> Result<Option<magikpod::Pod>> {
    // List all pods and find by ID string match
    let summaries = service().list_pods().await?;
    for summary in summaries {
        if summary.id.to_string() == pod_id_or_name || summary.name == pod_id_or_name {
            return service().get_pod(&summary.id).await.map_err(Into::into);
        }
    }
    Ok(None)
}

/// Gets logs for a pod.
///
/// # Arguments
///
/// * `pod_id_or_name` - Pod identifier or name
/// * `tail` - Optional number of lines from end
///
/// # Returns
///
/// Log output as a string.
pub async fn get_pod_logs(pod_id_or_name: &str, tail: Option<usize>) -> Result<String> {
    // List all pods and find by ID or name match
    let summaries = service().list_pods().await?;
    for summary in summaries {
        if summary.id.to_string() == pod_id_or_name || summary.name == pod_id_or_name {
            return service()
                .get_logs(&summary.id, None, tail)
                .await
                .map_err(Into::into);
        }
    }
    anyhow::bail!("Pod not found: {}", pod_id_or_name)
}

/// Pod information for API responses.
///
/// This is a compatibility type that bridges between `magikpod::PodSummary`
/// and the existing API response format.
#[derive(Debug, Clone)]
pub struct PodInfo {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub kind: String,
    pub status: PodStatus,
    pub created_at: SystemTime,
    pub metadata: HashMap<String, String>,
    pub ports: Vec<u16>,
}

/// Pod status for API responses.
#[derive(Debug, Clone)]
pub enum PodStatus {
    Running,
    Starting,
    Stopped,
    Failed(String),
    Unknown,
}

impl From<PodPhase> for PodStatus {
    fn from(phase: PodPhase) -> Self {
        match phase {
            PodPhase::Running => PodStatus::Running,
            PodPhase::Pending => PodStatus::Starting,
            PodPhase::Succeeded => PodStatus::Stopped,
            PodPhase::Failed => PodStatus::Failed("Unknown error".to_string()),
            PodPhase::Unknown => PodStatus::Unknown,
        }
    }
}

impl From<PodSummary> for PodInfo {
    fn from(summary: PodSummary) -> Self {
        Self {
            id: summary.id.to_string(),
            name: summary.name,
            namespace: summary.namespace,
            kind: summary
                .labels
                .get("magik.io/workload-kind")
                .cloned()
                .unwrap_or_else(|| "Pod".to_string()),
            status: summary.phase.into(),
            created_at: summary.created_at,
            metadata: summary.labels,
            ports: Vec::new(), // Port info not available in PodSummary
        }
    }
}

/// Lists all pods as PodInfo for API compatibility.
pub async fn list_all_pods() -> Result<Vec<PodInfo>> {
    let summaries = service().list_pods().await?;
    Ok(summaries.into_iter().map(PodInfo::from).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_manifest_injects_runtime_class() {
        let manifest = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
spec:
  containers:
  - name: nginx
    image: nginx:1.25
"#;

        let result = preprocess_manifest(manifest).unwrap();
        let processed: serde_yaml::Value = serde_yaml::from_slice(&result).unwrap();

        let runtime_class = processed
            .get("spec")
            .and_then(|s| s.get("runtimeClassName"))
            .and_then(|v| v.as_str());

        assert_eq!(runtime_class, Some(DEFAULT_RUNTIME_CLASS));
    }

    #[test]
    fn test_preprocess_manifest_respects_annotation() {
        let manifest = r#"
apiVersion: v1
kind: Pod
metadata:
  name: test-pod
  annotations:
    magik.io/runtime-class: pod-wasm
spec:
  containers:
  - name: wasm-app
    image: wasm-app:latest
"#;

        let result = preprocess_manifest(manifest).unwrap();
        let processed: serde_yaml::Value = serde_yaml::from_slice(&result).unwrap();

        let runtime_class = processed
            .get("spec")
            .and_then(|s| s.get("runtimeClassName"))
            .and_then(|v| v.as_str());

        assert_eq!(runtime_class, Some("pod-wasm"));
    }

    #[test]
    fn test_pod_status_conversion() {
        assert!(matches!(PodStatus::from(PodPhase::Running), PodStatus::Running));
        assert!(matches!(PodStatus::from(PodPhase::Pending), PodStatus::Starting));
        assert!(matches!(PodStatus::from(PodPhase::Succeeded), PodStatus::Stopped));
        assert!(matches!(PodStatus::from(PodPhase::Failed), PodStatus::Failed(_)));
        assert!(matches!(PodStatus::from(PodPhase::Unknown), PodStatus::Unknown));
    }
}
