//! # REST API Layer
//!
//! This module provides the HTTP API for machineplane nodes, implementing
//! a Kubernetes-compatible API subset alongside BeeMesh-specific endpoints.
//!
//! # API Surface
//!
//! ## Kubernetes-Compatible Endpoints
//!
//! - `GET /api/v1/namespaces/{ns}/pods` - List pods
//! - `GET /apis/apps/v1/namespaces/{ns}/deployments` - List deployments
//! - `POST /apis/apps/v1/namespaces/{ns}/deployments` - Create deployment
//! - `DELETE /apis/apps/v1/namespaces/{ns}/deployments/{name}` - Delete deployment
//!
//! ## BeeMesh-Specific Endpoints
//!
//! - `GET /health` - Health check
//! - `GET /nodes` - List connected mesh peers
//! - `POST /apply` - Deploy a manifest via tender/bid/award
//! - `DELETE /delete` - Remove a deployed workload
//!
//! # Architecture
//!
//! The API layer translates HTTP requests into:
//! - [`NetworkControl`] commands for mesh operations
//! - Direct runtime engine calls for local queries
//!
//! # Thread Safety
//!
//! State is shared via [`RestState`] which contains thread-safe handles
//! to the network control channel and tender tracking.

use crate::messages::CandidateNode;
use crate::network::BEEMESH_FABRIC;
use crate::network::NetworkControl;
use crate::scheduler::{DEFAULT_SELECTION_WINDOW_MS, register_local_manifest};
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post},
};
use base64::Engine;
use log::{debug, error, info};
use rand::RngCore;
use serde_json::Value;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::{
    sync::watch,
    time::{Duration, timeout},
};
use uuid::Uuid;

/// Maximum number of tenders to track before evicting oldest entries.
const MAX_TRACKED_TENDERS: usize = 10_000;

/// Formats a SystemTime as an RFC 3339 timestamp string (for debug endpoints).
fn format_debug_timestamp(time: &std::time::SystemTime) -> Option<String> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| {
            let secs = d.as_secs();
            time::OffsetDateTime::from_unix_timestamp(secs as i64)
                .ok()
                .and_then(|offset_dt| {
                    offset_dt
                        .format(&time::format_description::well_known::Rfc3339)
                        .ok()
                })
        })
}

/// Creates an HTTP response with octet-stream content type.
async fn create_response_with_fallback(
    body: &[u8],
) -> Result<axum::response::Response<axum::body::Body>, axum::http::StatusCode> {
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(body.to_vec()))
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)
}

/// Kubernetes-compatible API handlers.
///
/// Implements a subset of the Kubernetes API to enable compatibility with
/// standard tools like `kubectl` while routing workloads through the mesh.
pub mod kube {

    use super::RestState;
    use crate::scheduler::register_local_manifest;
    use axum::{
        Json, Router,
        body::Bytes,
        extract::{Path, Query, State},
        http::StatusCode,
        routing::get,
    };
    use log::{error, info, warn};
    use rand::RngCore;
    use serde_json::{Map, Value, json};
    use sha2::{Digest, Sha256};
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, timeout};
    use uuid::Uuid;

    // Kubernetes resource kinds
    const DEPLOYMENT_KIND: &str = "Deployment";
    const REPLICASET_KIND: &str = "ReplicaSet";
    const STATEFULSET_KIND: &str = "StatefulSet";
    const POD_KIND: &str = "Pod";
    const APPS_V1: &str = "apps/v1";
    const CORE_V1: &str = "v1";

    // Standard Kubernetes labels
    const LABEL_APP: &str = "app";
    const LABEL_APP_NAME: &str = "app.kubernetes.io/name";
    const LABEL_APP_INSTANCE: &str = "app.kubernetes.io/instance";
    const LABEL_POD_NAMESPACE: &str = "io.kubernetes.pod.namespace";
    const LABEL_WORKLOAD_KIND: &str = "magik.io/workload-kind";

    /// Router for core Kubernetes API (v1).
    pub fn core_router() -> Router<RestState> {
        Router::new()
            .route("/", get(api_versions))
            .route("/v1", get(core_v1_resources))
            .route("/v1/namespaces/{namespace}/pods", get(list_pods))
            .route("/v1/namespaces/{namespace}/pods/{name}", get(get_pod))
            .route(
                "/v1/namespaces/{namespace}/pods/{name}/log",
                get(get_pod_logs),
            )
    }

    pub fn apps_v1_router() -> Router<RestState> {
        Router::new()
            .route("/", get(apps_v1_resources))
            .route(
                "/namespaces/{namespace}/deployments",
                get(list_deployments).post(create_deployment),
            )
            .route(
                "/namespaces/{namespace}/deployments/{name}",
                get(get_deployment).delete(delete_deployment),
            )
            .route(
                "/namespaces/{namespace}/statefulsets",
                get(list_statefulsets).post(create_statefulset),
            )
            .route(
                "/namespaces/{namespace}/statefulsets/{name}",
                get(get_statefulset).delete(delete_statefulset),
            )
            .route("/namespaces/{namespace}/replicasets", get(list_replicasets))
            .route(
                "/namespaces/{namespace}/replicasets/{name}",
                get(get_replicaset),
            )
    }

    pub async fn api_group_list() -> Json<Value> {
        Json(json!({
            "kind": "APIGroupList",
            "apiVersion": "v1",
            "groups": [
                {
                    "name": "apps",
                    "versions": [{ "groupVersion": "apps/v1", "version": "v1" }],
                    "preferredVersion": { "groupVersion": "apps/v1", "version": "v1" }
                }
            ]
        }))
    }

    pub async fn version() -> Json<Value> {
        Json(json!({
            "major": "1",
            "minor": "28",
            "gitVersion": "v1.28.0-magik",
            "platform": "linux/amd64"
        }))
    }

    async fn api_versions() -> Json<Value> {
        Json(json!({
            "kind": "APIVersions",
            "apiVersion": "v1",
            "versions": ["v1"],
            "serverAddressByClientCIDRs": []
        }))
    }

    async fn core_v1_resources() -> Json<Value> {
        Json(json!({
            "kind": "APIResourceList",
            "groupVersion": "v1",
            "resources": [
                {
                    "name": "pods",
                    "singularName": "pod",
                    "namespaced": true,
                    "kind": POD_KIND,
                    "verbs": ["get", "list"]
                },
                {
                    "name": "pods/log",
                    "singularName": "",
                    "namespaced": true,
                    "kind": POD_KIND,
                    "verbs": ["get"]
                }
            ]
        }))
    }

    async fn apps_v1_resources() -> Json<Value> {
        Json(json!({
            "kind": "APIResourceList",
            "groupVersion": "apps/v1",
            "resources": [
                {
                    "name": "deployments",
                    "singularName": "deployment",
                    "namespaced": true,
                    "kind": DEPLOYMENT_KIND,
                    "verbs": ["get", "list", "create", "delete"]
                },
                {
                    "name": "statefulsets",
                    "singularName": "statefulset",
                    "namespaced": true,
                    "kind": STATEFULSET_KIND,
                    "verbs": ["get", "list", "create", "delete"]
                },
                {
                    "name": "replicasets",
                    "singularName": "replicaset",
                    "namespaced": true,
                    "kind": REPLICASET_KIND,
                    "verbs": ["get", "list"]
                }
            ]
        }))
    }

    #[derive(Debug, Clone)]
    struct WorkloadInfo {
        name: String,
        namespace: String,
        kind: String, // "Deployment" or "StatefulSet"
        replicas: u32,
        ready_replicas: u32,
        created: Option<String>,
        labels: HashMap<String, String>,
    }

    /// Converts PodStatus to a Kubernetes-compatible phase string.
    fn pod_status_to_k8s_phase(status: &crate::runtimes::PodStatus) -> &'static str {
        match status {
            crate::runtimes::PodStatus::Running => "Running",
            crate::runtimes::PodStatus::Starting => "Pending",
            crate::runtimes::PodStatus::Stopped => "Succeeded",
            crate::runtimes::PodStatus::Failed(_) => "Failed",
            crate::runtimes::PodStatus::Unknown => "Unknown",
        }
    }

    /// Checks if a PodStatus indicates a running state.
    fn is_pod_running(status: &crate::runtimes::PodStatus) -> bool {
        matches!(status, crate::runtimes::PodStatus::Running)
    }

    /// Formats a SystemTime as an RFC 3339 timestamp string.
    fn format_timestamp(time: &std::time::SystemTime) -> Option<String> {
        time.duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| {
                let secs = d.as_secs();
                // Convert to RFC 3339 format: YYYY-MM-DDTHH:MM:SSZ
                let offset_dt = time::OffsetDateTime::from_unix_timestamp(secs as i64).ok()?;
                offset_dt
                    .format(&time::format_description::well_known::Rfc3339)
                    .ok()
            })
    }

    fn extract_workload_info(pod: &crate::runtimes::PodInfo) -> Option<WorkloadInfo> {
        let labels = &pod.metadata;

        let namespace = labels
            .get(LABEL_POD_NAMESPACE)
            .cloned()
            .unwrap_or_else(|| pod.namespace.clone());

        let workload_name = labels
            .get(LABEL_APP_NAME)
            .or_else(|| labels.get(LABEL_APP))
            .or_else(|| labels.get(LABEL_APP_INSTANCE))
            .cloned()
            .unwrap_or_else(|| pod.name.clone());

        if workload_name.is_empty() {
            return None;
        }

        let kind = if let Some(k) = labels.get(LABEL_WORKLOAD_KIND) {
            k.clone()
        } else {
            pod.kind.clone()
        };

        let is_running = is_pod_running(&pod.status);

        let created = format_timestamp(&pod.created_at);

        Some(WorkloadInfo {
            name: workload_name,
            namespace,
            kind,
            replicas: 1,
            ready_replicas: if is_running { 1 } else { 0 },
            created,
            labels: labels.clone(),
        })
    }

    fn aggregate_workloads(
        pods: &[crate::runtimes::PodInfo],
        namespace_filter: &str,
        kind_filter: Option<&str>,
    ) -> Vec<WorkloadInfo> {
        let mut workloads: HashMap<String, WorkloadInfo> = HashMap::new();

        for pod in pods {
            if let Some(info) = extract_workload_info(pod) {
                if info.namespace != namespace_filter {
                    continue;
                }

                if kind_filter.is_some_and(|kind| info.kind != kind) {
                    continue;
                }

                let key = format!("{}/{}/{}", info.namespace, info.kind, info.name);

                let entry = workloads.entry(key).or_insert_with(|| WorkloadInfo {
                    name: info.name.clone(),
                    namespace: info.namespace.clone(),
                    kind: info.kind.clone(),
                    replicas: 0,
                    ready_replicas: 0,
                    created: info.created.clone(),
                    labels: info.labels.clone(),
                });

                entry.replicas += 1;
                entry.ready_replicas += info.ready_replicas;

                if entry.created.is_none() {
                    entry.created = info.created.clone();
                }
            }
        }

        workloads.into_values().collect()
    }

    fn build_deployment_response(info: &WorkloadInfo) -> Value {
        json!({
            "apiVersion": APPS_V1,
            "kind": DEPLOYMENT_KIND,
            "metadata": {
                "name": info.name,
                "namespace": info.namespace,
                "uid": format!("deploy-{}-{}", info.namespace, info.name),
                "resourceVersion": "1",
                "creationTimestamp": info.created,
                "labels": info.labels
            },
            "spec": {
                "replicas": info.replicas,
                "selector": {
                    "matchLabels": {
                        "app": info.name
                    }
                }
            },
            "status": {
                "replicas": info.replicas,
                "readyReplicas": info.ready_replicas,
                "availableReplicas": info.ready_replicas,
                "updatedReplicas": info.replicas,
                "observedGeneration": 1,
                "conditions": [{
                    "type": "Available",
                    "status": if info.ready_replicas > 0 { "True" } else { "False" },
                    "reason": if info.ready_replicas > 0 { "MinimumReplicasAvailable" } else { "MinimumReplicasUnavailable" }
                }]
            }
        })
    }

    async fn list_deployments(
        Path(namespace): Path<String>,
        State(_state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let pods = crate::runtime::list_all_pods().await.map_err(|e| {
            warn!("Failed to list pods: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let workloads = aggregate_workloads(&pods, &namespace, Some(DEPLOYMENT_KIND));
        let items: Vec<Value> = workloads.iter().map(build_deployment_response).collect();

        Ok(Json(json!({
            "kind": "DeploymentList",
            "apiVersion": APPS_V1,
            "metadata": { "resourceVersion": "1" },
            "items": items
        })))
    }

    async fn get_deployment(
        Path((namespace, name)): Path<(String, String)>,
        State(_state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let pods = crate::runtime::list_all_pods().await.map_err(|e| {
            warn!("Failed to list pods: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let workloads = aggregate_workloads(&pods, &namespace, Some(DEPLOYMENT_KIND));

        for w in &workloads {
            if w.name == name {
                return Ok(Json(build_deployment_response(w)));
            }
        }

        Err(StatusCode::NOT_FOUND)
    }

    async fn create_deployment(
        Path(namespace): Path<String>,
        State(state): State<RestState>,
        body: Bytes,
    ) -> Result<Json<Value>, StatusCode> {
        let manifest = parse_manifest(&body)?;
        let (manifest, ns, name) = ensure_metadata(manifest, &namespace, DEPLOYMENT_KIND, APPS_V1)?;

        if ns != namespace {
            return Err(StatusCode::BAD_REQUEST);
        }

        let tender_id = publish_workload_tender(&state, &manifest).await?;

        info!(
            "Deployment {}/{} tender published (tender_id={})",
            namespace, name, tender_id
        );

        Ok(Json(json!({
            "apiVersion": APPS_V1,
            "kind": DEPLOYMENT_KIND,
            "tender_id": tender_id,
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": format!("deploy-{}-{}", namespace, name),
                "resourceVersion": "1"
            },
            "spec": {
                "replicas": 1
            },
            "status": {
                "replicas": 0,
                "readyReplicas": 0,
                "conditions": [{
                    "type": "Progressing",
                    "status": "True",
                    "reason": "TenderPublished"
                }]
            }
        })))
    }

    async fn delete_deployment(
        Path((namespace, name)): Path<(String, String)>,
        State(state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let delete_manifest = build_delete_manifest(&namespace, &name, DEPLOYMENT_KIND, APPS_V1);

        let _ = publish_workload_tender(&state, &delete_manifest).await?;

        info!("Deployment {}/{} delete tender published", namespace, name);

        Ok(Json(json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Success",
            "details": {
                "name": name,
                "group": "apps",
                "kind": DEPLOYMENT_KIND
            }
        })))
    }

    fn build_statefulset_response(info: &WorkloadInfo) -> Value {
        json!({
            "apiVersion": APPS_V1,
            "kind": STATEFULSET_KIND,
            "metadata": {
                "name": info.name,
                "namespace": info.namespace,
                "uid": format!("sts-{}-{}", info.namespace, info.name),
                "resourceVersion": "1",
                "creationTimestamp": info.created,
                "labels": info.labels
            },
            "spec": {
                "replicas": info.replicas,
                "serviceName": info.name,
                "selector": {
                    "matchLabels": {
                        "app": info.name
                    }
                }
            },
            "status": {
                "replicas": info.replicas,
                "readyReplicas": info.ready_replicas,
                "currentReplicas": info.replicas,
                "updatedReplicas": info.replicas,
                "currentRevision": format!("{}-1", info.name),
                "updateRevision": format!("{}-1", info.name),
                "observedGeneration": 1
            }
        })
    }

    async fn list_statefulsets(
        Path(namespace): Path<String>,
        State(_state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let pods = crate::runtime::list_all_pods().await.map_err(|e| {
            warn!("Failed to list pods: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let workloads = aggregate_workloads(&pods, &namespace, Some(STATEFULSET_KIND));
        let items: Vec<Value> = workloads.iter().map(build_statefulset_response).collect();

        Ok(Json(json!({
            "kind": "StatefulSetList",
            "apiVersion": APPS_V1,
            "metadata": { "resourceVersion": "1" },
            "items": items
        })))
    }

    async fn get_statefulset(
        Path((namespace, name)): Path<(String, String)>,
        State(_state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let pods = crate::runtime::list_all_pods().await.map_err(|e| {
            warn!("Failed to list pods: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let workloads = aggregate_workloads(&pods, &namespace, Some(STATEFULSET_KIND));

        for w in &workloads {
            if w.name == name {
                return Ok(Json(build_statefulset_response(w)));
            }
        }

        Err(StatusCode::NOT_FOUND)
    }

    async fn create_statefulset(
        Path(namespace): Path<String>,
        State(state): State<RestState>,
        body: Bytes,
    ) -> Result<Json<Value>, StatusCode> {
        let manifest = parse_manifest(&body)?;
        let (manifest, ns, name) =
            ensure_metadata(manifest, &namespace, STATEFULSET_KIND, APPS_V1)?;

        if ns != namespace {
            return Err(StatusCode::BAD_REQUEST);
        }

        let tender_id = publish_workload_tender(&state, &manifest).await?;

        info!(
            "StatefulSet {}/{} tender published (tender_id={})",
            namespace, name, tender_id
        );

        Ok(Json(json!({
            "apiVersion": APPS_V1,
            "kind": STATEFULSET_KIND,
            "tender_id": tender_id,
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": format!("sts-{}-{}", namespace, name),
                "resourceVersion": "1"
            },
            "spec": {
                "replicas": 1
            },
            "status": {
                "replicas": 0,
                "readyReplicas": 0
            }
        })))
    }

    async fn delete_statefulset(
        Path((namespace, name)): Path<(String, String)>,
        State(state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let delete_manifest = build_delete_manifest(&namespace, &name, STATEFULSET_KIND, APPS_V1);

        let _ = publish_workload_tender(&state, &delete_manifest).await?;

        info!("StatefulSet {}/{} delete tender published", namespace, name);

        Ok(Json(json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Success",
            "details": {
                "name": name,
                "group": "apps",
                "kind": STATEFULSET_KIND
            }
        })))
    }

    fn build_replicaset_response(info: &WorkloadInfo) -> Value {
        let rs_name = format!("{}-rs", info.name);
        json!({
            "apiVersion": APPS_V1,
            "kind": REPLICASET_KIND,
            "metadata": {
                "name": rs_name,
                "namespace": info.namespace,
                "uid": format!("rs-{}-{}", info.namespace, info.name),
                "resourceVersion": "1",
                "creationTimestamp": info.created,
                "ownerReferences": [{
                    "apiVersion": APPS_V1,
                    "kind": DEPLOYMENT_KIND,
                    "name": info.name,
                    "uid": format!("deploy-{}-{}", info.namespace, info.name),
                    "controller": true
                }],
                "labels": {
                    "app": info.name
                }
            },
            "spec": {
                "replicas": info.replicas,
                "selector": {
                    "matchLabels": {
                        "app": info.name
                    }
                }
            },
            "status": {
                "replicas": info.replicas,
                "readyReplicas": info.ready_replicas,
                "availableReplicas": info.ready_replicas,
                "observedGeneration": 1
            }
        })
    }

    async fn list_replicasets(
        Path(namespace): Path<String>,
        State(_state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let pods = crate::runtime::list_all_pods().await.map_err(|e| {
            warn!("Failed to list pods: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let workloads = aggregate_workloads(&pods, &namespace, Some(DEPLOYMENT_KIND));
        let items: Vec<Value> = workloads.iter().map(build_replicaset_response).collect();

        Ok(Json(json!({
            "kind": "ReplicaSetList",
            "apiVersion": APPS_V1,
            "metadata": { "resourceVersion": "1" },
            "items": items
        })))
    }

    async fn get_replicaset(
        Path((namespace, name)): Path<(String, String)>,
        State(_state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let pods = crate::runtime::list_all_pods().await.map_err(|e| {
            warn!("Failed to list pods: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let workloads = aggregate_workloads(&pods, &namespace, Some(DEPLOYMENT_KIND));

        for w in &workloads {
            let rs_name = format!("{}-rs", w.name);
            if rs_name == name {
                return Ok(Json(build_replicaset_response(w)));
            }
        }

        Err(StatusCode::NOT_FOUND)
    }

    fn build_pod_response(pod: &crate::runtimes::PodInfo) -> Value {
        let labels = &pod.metadata;
        let name = &pod.name;
        let namespace = labels
            .get(LABEL_POD_NAMESPACE)
            .cloned()
            .unwrap_or_else(|| pod.namespace.clone());
        let phase = pod_status_to_k8s_phase(&pod.status);
        let is_running = is_pod_running(&pod.status);

        let created_timestamp = format_timestamp(&pod.created_at);

        // For microVMs, we have a single "container" representing the workload
        let container_status = json!({
            "name": name,
            "state": if is_running {
                json!({ "running": { "startedAt": created_timestamp } })
            } else {
                match &pod.status {
                    crate::runtimes::PodStatus::Failed(reason) => json!({ "terminated": { "exitCode": 1, "reason": reason } }),
                    crate::runtimes::PodStatus::Stopped => json!({ "terminated": { "exitCode": 0, "reason": "Completed" } }),
                    _ => json!({ "waiting": { "reason": phase } })
                }
            },
            "ready": is_running,
            "restartCount": 0,
            "containerID": pod.id.clone()
        });

        json!({
            "apiVersion": CORE_V1,
            "kind": POD_KIND,
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": pod.id.clone(),
                "resourceVersion": "1",
                "creationTimestamp": created_timestamp,
                "labels": labels
            },
            "spec": {
                "containers": [{
                    "name": name
                }]
            },
            "status": {
                "phase": phase,
                "conditions": [
                    { "type": "Ready", "status": if is_running { "True" } else { "False" } },
                    { "type": "PodScheduled", "status": "True" }
                ],
                "containerStatuses": [container_status],
                "podIP": "",
                "hostIP": ""
            }
        })
    }

    async fn list_pods(
        Path(namespace): Path<String>,
        State(_state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        let pods = crate::runtime::list_all_pods().await.map_err(|e| {
            warn!("Failed to list pods: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let items: Vec<Value> = pods
            .iter()
            .filter(|pod| {
                pod.metadata
                    .get(LABEL_POD_NAMESPACE)
                    .map(|ns| ns == &namespace)
                    .unwrap_or(pod.namespace == namespace || namespace == "default")
            })
            .map(build_pod_response)
            .collect();

        Ok(Json(json!({
            "kind": "PodList",
            "apiVersion": CORE_V1,
            "metadata": { "resourceVersion": "1" },
            "items": items
        })))
    }

    async fn get_pod(
        Path((namespace, name)): Path<(String, String)>,
        State(_state): State<RestState>,
    ) -> Result<Json<Value>, StatusCode> {
        // Try to find the pod by name in the list
        let pods = crate::runtime::list_all_pods().await.map_err(|e| {
            warn!("Failed to list pods: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        for pod in &pods {
            if pod.name == name
                && (pod.namespace == namespace
                    || pod
                        .metadata
                        .get(LABEL_POD_NAMESPACE)
                        .map(|ns| ns == &namespace)
                        .unwrap_or(false))
            {
                return Ok(Json(build_pod_response(pod)));
            }
            // Also check by pod ID
            if pod.id == name {
                return Ok(Json(build_pod_response(pod)));
            }
        }

        // Try direct lookup by ID
        match crate::runtime::get_pod(&name).await {
            Ok(pod) => Ok(Json(build_pod_response(&pod))),
            Err(_) => Err(StatusCode::NOT_FOUND),
        }
    }

    async fn get_pod_logs(
        Path((_namespace, name)): Path<(String, String)>,
        Query(params): Query<HashMap<String, String>>,
        State(_state): State<RestState>,
    ) -> Result<String, StatusCode> {
        let tail = params.get("tailLines").and_then(|v| v.parse().ok());

        match crate::runtime::get_pod_logs(&name, tail).await {
            Ok(logs) => Ok(logs),
            Err(e) => {
                // Check if it's a not-found error
                let err_str = e.to_string();
                if err_str.contains("not found") || err_str.contains("InstanceNotFound") {
                    Err(StatusCode::NOT_FOUND)
                } else {
                    warn!("Failed to get logs for pod {}: {}", name, e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
    }

    /// Maximum allowed manifest size (1 MiB) to prevent memory exhaustion
    /// from oversized or malicious YAML/JSON payloads.
    const MAX_MANIFEST_SIZE: usize = 1024 * 1024;

    fn parse_manifest(body: &[u8]) -> Result<Value, StatusCode> {
        if body.is_empty() {
            return Err(StatusCode::BAD_REQUEST);
        }
        // Reject oversized manifests before parsing to prevent YAML bombs
        // and excessive memory allocation during deserialization.
        if body.len() > MAX_MANIFEST_SIZE {
            warn!(
                "Rejecting manifest: size {} exceeds limit {}",
                body.len(),
                MAX_MANIFEST_SIZE
            );
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }
        serde_json::from_slice(body).or_else(|_| {
            let yaml: serde_yaml::Value =
                serde_yaml::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;
            serde_json::to_value(yaml).map_err(|_| StatusCode::BAD_REQUEST)
        })
    }

    fn ensure_metadata(
        mut manifest: Value,
        namespace_from_path: &str,
        expected_kind: &str,
        expected_api_version: &str,
    ) -> Result<(Value, String, String), StatusCode> {
        let kind = manifest
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or(expected_kind);
        if kind != expected_kind {
            return Err(StatusCode::BAD_REQUEST);
        }
        manifest["kind"] = Value::String(expected_kind.to_string());
        manifest["apiVersion"] = Value::String(expected_api_version.to_string());

        let mut metadata = manifest
            .get("metadata")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_else(Map::new);

        let name = metadata
            .get("name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or(StatusCode::BAD_REQUEST)?;
        let namespace = metadata
            .get("namespace")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| namespace_from_path.to_string());

        metadata.insert("namespace".to_string(), Value::String(namespace.clone()));
        manifest["metadata"] = Value::Object(metadata);

        Ok((manifest, namespace, name))
    }

    fn build_delete_manifest(namespace: &str, name: &str, kind: &str, api_version: &str) -> Value {
        let mut annotations = Map::new();
        annotations.insert(
            "magik.io/operation".into(),
            Value::String("delete".to_string()),
        );

        let mut metadata = Map::new();
        metadata.insert("name".into(), Value::String(name.to_string()));
        metadata.insert("namespace".into(), Value::String(namespace.to_string()));
        metadata.insert("annotations".into(), Value::Object(annotations));

        let mut spec = Map::new();
        spec.insert("replicas".into(), Value::Number(0.into()));

        json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": metadata,
            "spec": spec
        })
    }

    async fn publish_workload_tender(
        state: &RestState,
        manifest: &Value,
    ) -> Result<String, StatusCode> {
        let manifest_str =
            serde_json::to_string(manifest).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .as_millis() as u64;

        let manifest_digest = hex::encode(Sha256::digest(manifest_str.as_bytes()));
        let tender_id = Uuid::new_v4().to_string();

        register_local_manifest(&tender_id, &manifest_str);

        let tender = crate::messages::Tender {
            id: tender_id.clone(),
            manifest_digest,
            qos_preemptible: false,
            timestamp,
            nonce: rand::thread_rng().next_u64(),
        };

        let tender_bytes = bincode::serialize(&tender).map_err(|e| {
            error!("apply: failed to serialize tender: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<Result<(), String>>();
        state
            .control_tx
            .send(crate::network::NetworkControl::PublishTender {
                payload: tender_bytes,
                reply_tx,
            })
            .map_err(|e| {
                warn!("Failed to dispatch tender: {}", e);
                StatusCode::BAD_GATEWAY
            })?;

        match timeout(Duration::from_secs(5), reply_rx.recv()).await {
            Ok(Some(Ok(_))) => Ok(tender_id),
            Ok(Some(Err(e))) => {
                warn!("Tender publication failed: {}", e);
                Err(StatusCode::BAD_GATEWAY)
            }
            Ok(None) => {
                warn!("Tender publication channel closed");
                Err(StatusCode::BAD_GATEWAY)
            }
            Err(_) => {
                warn!("Timed out publishing tender");
                Err(StatusCode::GATEWAY_TIMEOUT)
            }
        }
    }
}

async fn get_nodes(
    State(state): State<RestState>,
    _headers: HeaderMap,
) -> Result<axum::response::Response<axum::body::Body>, axum::http::StatusCode> {
    let peers = state.peer_rx.borrow().clone();
    let response = crate::messages::NodesResponse { peers };
    let response_data = bincode::serialize(&response).map_err(|e| {
        error!("get_nodes: failed to serialize response: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(axum::response::Response::builder()
        .status(200)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(response_data))
        .unwrap())
}

async fn get_public_key(State(_state): State<RestState>) -> String {
    if let Some((pk, _)) = crate::network::get_node_keypair() {
        return base64::engine::general_purpose::STANDARD.encode(pk);
    }
    "ERROR: No keypair available".to_string()
}

/// Shared state for all API handlers.
///
/// Provides thread-safe access to:
/// - Peer list updates via watch channel
/// - Network control commands via unbounded channel
/// - Active tender tracking for status queries
#[derive(Clone)]
pub struct RestState {
    /// Receiver for peer list updates from the network layer
    pub peer_rx: watch::Receiver<Vec<String>>,
    /// Sender for network control commands
    pub control_tx: mpsc::UnboundedSender<crate::network::NetworkControl>,
    /// Tracks active tenders for status queries
    pub tender_tracker: Arc<RwLock<HashMap<String, TenderRecord>>>,
    /// This node's identity bytes (for signing/verification)
    pub local_identity_bytes: Vec<u8>,
}

/// Builds the complete API router with all endpoints.
///
/// # Arguments
///
/// * `peer_rx` - Watch receiver for peer list updates
/// * `control_tx` - Unbounded sender for network control commands
/// * `local_identity_bytes` - This node's identity bytes
///
/// # Returns
///
/// A configured Axum router with all BeeMesh and Kubernetes-compatible endpoints.
pub fn build_router(
    peer_rx: watch::Receiver<Vec<String>>,
    control_tx: mpsc::UnboundedSender<crate::network::NetworkControl>,
    local_identity_bytes: Vec<u8>,
) -> Router {
    let state = RestState {
        peer_rx,
        control_tx,
        tender_tracker: Arc::new(RwLock::new(HashMap::new())),
        local_identity_bytes,
    };
    let kube_routes = Router::new()
        .route("/version", get(kube::version))
        .route("/apis", get(kube::api_group_list))
        .nest("/api", kube::core_router())
        .nest("/apis/apps/v1", kube::apps_v1_router());

    Router::new()
        // Defense in depth: explicit body size limit matching MAX_MANIFEST_SIZE
        .layer(DefaultBodyLimit::max(crate::runtime::MAX_MANIFEST_SIZE))
        .route("/health", get(|| async { "ok" }))
        .route("/api/v1/pubkey", get(get_public_key))
        // NOTE: kem_pubkey currently returns Ed25519 signing key, not a KEM key.
        // True KEM (e.g., X25519 or post-quantum) is planned for future implementation.
        .route("/api/v1/kem_pubkey", get(get_public_key))
        .route("/api/v1/signing_pubkey", get(get_public_key))
        .route("/debug/dht/active_announces", get(debug_active_announces))
        .route("/debug/dht/peers", get(debug_dht_peers))
        .route("/debug/peers", get(debug_peers))
        .route("/debug/pods", get(debug_pods))
        .route("/debug/tenders", get(debug_all_tenders))
        .route("/debug/local_identity", get(debug_local_identity))
        .route("/tenders", post(create_tender))
        .route("/tenders/{tender_id}", get(get_tender_status))
        .route("/disposal/{namespace}/{kind}/{name}", delete(delete_tender))
        .route("/tenders/{tender_id}/candidates", post(get_candidates))
        .route(
            "/disposal/{namespace}/{kind}/{name}",
            get(get_disposal_status),
        )
        .route("/nodes", get(get_nodes))
        .merge(kube_routes)
        .with_state(state)
}

pub async fn get_candidates(
    Path(tender_id): Path<String>,
    State(state): State<RestState>,
    _headers: HeaderMap,
    _body: Bytes,
) -> Result<axum::response::Response<axum::body::Body>, axum::http::StatusCode> {
    log::info!(
        "get_candidates: called for tender_id={} (direct delivery mode)",
        tender_id
    );

    let candidates = collect_candidate_pubkeys(&state, &tender_id, 5).await?;
    let response = crate::messages::CandidatesResponse {
        ok: true,
        candidates,
    };
    let response_data = bincode::serialize(&response).map_err(|e| {
        error!("get_candidates: failed to serialize response: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(axum::response::Response::builder()
        .status(200)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(response_data))
        .unwrap())
}

pub(crate) async fn collect_candidate_pubkeys(
    state: &RestState,
    tender_id: &str,
    max_candidates: usize,
) -> Result<Vec<CandidateNode>, axum::http::StatusCode> {
    const PLACEHOLDER_KEM_PUBLIC_KEY: &str = "mock-kem-public-key";

    log::info!(
        "collect_candidate_pubkeys: using peer inventory for tender {} (limit {})",
        tender_id,
        max_candidates
    );

    let peers = state.peer_rx.borrow().clone();
    let mut candidates: Vec<CandidateNode> = Vec::new();

    for identity in peers {
        if candidates.len() >= max_candidates {
            break;
        }

        candidates.push(CandidateNode {
            identity: identity.clone(),
            public_key: PLACEHOLDER_KEM_PUBLIC_KEY.to_string(),
        });
    }

    if candidates.is_empty() {
        log::warn!(
            "collect_candidate_pubkeys: no peers available on topic {}; returning empty candidate list",
            BEEMESH_FABRIC
        );
    }

    Ok(candidates)
}

#[derive(Debug, Clone)]
pub struct KubeResourceRecord {
    pub api_version: String,
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub resource_version: u64,
    pub manifest: Value,
    pub creation_timestamp: std::time::SystemTime,
}

#[derive(Debug, Clone)]
pub struct TenderRecord {
    pub manifest_bytes: Vec<u8>,
    pub created_at: std::time::SystemTime,
    pub manifests_distributed: HashMap<String, String>,
    pub assigned_peers: Option<Vec<String>>,
    pub owner_pubkey: Vec<u8>,
    pub kube: Option<KubeResourceRecord>,
}

pub async fn create_tender(
    State(state): State<RestState>,
    _headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<axum::response::Response<axum::body::Body>, axum::http::StatusCode> {
    debug!("create_tender: parsing payload");

    let payload_bytes_for_parsing = body.to_vec();

    log::info!(
        "create_tender: received payload len={}, first_20_bytes={:02x?}",
        payload_bytes_for_parsing.len(),
        &payload_bytes_for_parsing[..std::cmp::min(20, payload_bytes_for_parsing.len())]
    );

    let tender_id = Uuid::new_v4().to_string();
    log::info!("create_tender: generated tender_id='{}'", tender_id);

    let owner_pubkey = Vec::new();

    let manifest_bytes_to_store = payload_bytes_for_parsing;
    let manifest_str = String::from_utf8_lossy(&manifest_bytes_to_store).to_string();

    register_local_manifest(&tender_id, &manifest_str);

    let rec = TenderRecord {
        manifest_bytes: manifest_bytes_to_store,
        created_at: std::time::SystemTime::now(),
        manifests_distributed: HashMap::new(),
        assigned_peers: None,
        owner_pubkey: owner_pubkey.clone(),
        kube: None,
    };
    {
        let mut tracker = state.tender_tracker.write().await;

        if tracker.len() >= MAX_TRACKED_TENDERS
            && let Some(oldest_key) = tracker
                .iter()
                .min_by_key(|(_, v)| v.created_at)
                .map(|(k, _)| k.clone())
        {
            log::warn!(
                "create_tender: tender_tracker at capacity ({}), evicting oldest tender '{}'",
                MAX_TRACKED_TENDERS,
                oldest_key
            );
            tracker.remove(&oldest_key);
        }

        log::info!(
            "create_tender: tracking tender with tender_id='{}'",
            tender_id
        );
        log::info!(
            "create_tender: tender_tracker had {} tenders before insert",
            tracker.len()
        );
        tracker.insert(tender_id.clone(), rec);
        log::info!(
            "create_tender: tender_tracker now has {} tenders after insert",
            tracker.len()
        );
        log::info!(
            "create_tender: verifying tender_id '{}' exists in tracker: {}",
            tender_id,
            tracker.contains_key(&tender_id)
        );
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .as_millis() as u64;

    let tender = crate::messages::Tender {
        id: tender_id.clone(),
        manifest_digest: tender_id.clone(),
        qos_preemptible: false,
        timestamp,
        nonce: rand::thread_rng().next_u64(),
    };

    let tender_bytes = bincode::serialize(&tender).map_err(|e| {
        error!("create_tender: failed to serialize tender: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel::<Result<(), String>>();
    state
        .control_tx
        .send(NetworkControl::PublishTender {
            payload: tender_bytes,
            reply_tx,
        })
        .map_err(|e| {
            error!(
                "create_tender: failed to dispatch tender publication for tender_id={}: {}",
                tender_id, e
            );
            StatusCode::BAD_GATEWAY
        })?;

    match timeout(Duration::from_secs(10), reply_rx.recv()).await {
        Ok(Some(Ok(_))) => {}
        Ok(Some(Err(e))) => {
            log::warn!(
                "create_tender: tender publication failed for tender_id={}: {}",
                tender_id,
                e
            );
            return Err(StatusCode::BAD_GATEWAY);
        }
        Ok(None) => {
            log::warn!(
                "create_tender: tender publication channel closed for tender_id={}",
                tender_id
            );
            return Err(StatusCode::BAD_GATEWAY);
        }
        Err(_) => {
            log::warn!(
                "create_tender: timed out publishing tender_id={}",
                tender_id
            );
            return Err(StatusCode::GATEWAY_TIMEOUT);
        }
    }

    let response = crate::messages::TenderCreateResponse {
        ok: true,
        tender_id: tender_id.clone(),
        manifest_ref: tender_id.clone(),
        selection_window_ms: DEFAULT_SELECTION_WINDOW_MS,
        message: String::new(),
    };
    let response_data = bincode::serialize(&response).map_err(|e| {
        error!("create_tender: failed to serialize response: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(axum::response::Response::builder()
        .status(200)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(response_data))
        .unwrap())
}

async fn debug_active_announces(State(_state): State<RestState>) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "ok": true,
        "cids": [],
        "message": "provider announcements are low-touch and expire naturally"
    }))
}

async fn debug_local_identity(State(state): State<RestState>) -> axum::Json<serde_json::Value> {
    use tokio::sync::mpsc;
    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();

    let control_msg = crate::network::NetworkControl::GetLocalIdentity { reply_tx };

    if state.control_tx.send(control_msg).is_err() {
        return axum::Json(serde_json::json!({
            "ok": false,
            "error": "Failed to send control message"
        }));
    }

    match tokio::time::timeout(std::time::Duration::from_secs(2), reply_rx.recv()).await {
        Ok(Some(identity)) => axum::Json(serde_json::json!({
            "ok": true,
            "local_identity": identity.to_string()
        })),
        Ok(None) => axum::Json(serde_json::json!({
            "ok": false,
            "error": "Control channel closed"
        })),
        Err(_) => axum::Json(serde_json::json!({
            "ok": false,
            "error": "Timeout waiting for local identity"
        })),
    }
}

async fn debug_dht_peers(State(state): State<RestState>) -> axum::Json<serde_json::Value> {
    use tokio::sync::mpsc;

    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();

    let control_msg = crate::network::NetworkControl::GetConnectedPeers { reply_tx };

    if let Err(e) = state.control_tx.send(control_msg) {
        return axum::Json(serde_json::json!({
            "ok": false,
            "error": format!("Failed to send peers request: {}", e)
        }));
    }

    match tokio::time::timeout(std::time::Duration::from_secs(5), reply_rx.recv()).await {
        Ok(Some(Ok(peer_info))) => axum::Json(serde_json::json!({
            "ok": true,
            "connected_peers": peer_info
        })),
        Ok(Some(Err(e))) => axum::Json(serde_json::json!({
            "ok": false,
            "error": format!("Peers query failed: {}", e)
        })),
        Ok(None) => axum::Json(serde_json::json!({
            "ok": false,
            "error": "Peers channel closed"
        })),
        Err(_) => axum::Json(serde_json::json!({
            "ok": false,
            "error": "Peers request timed out"
        })),
    }
}

async fn debug_peers(State(state): State<RestState>) -> axum::Json<serde_json::Value> {
    let peers: Vec<String> = state.peer_rx.borrow().clone();

    axum::Json(serde_json::json!({
        "ok": true,
        "peers": peers,
        "count": peers.len()
    }))
}

async fn debug_pods(State(state): State<RestState>) -> axum::Json<serde_json::Value> {
    let local_identity = {
        use tokio::sync::mpsc;
        let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
        let control_msg = crate::network::NetworkControl::GetLocalIdentity { reply_tx };

        if state.control_tx.send(control_msg).is_ok() {
            match tokio::time::timeout(std::time::Duration::from_secs(1), reply_rx.recv()).await {
                Ok(Some(identity)) => identity.to_string(),
                _ => "unknown".to_string(),
            }
        } else {
            "unknown".to_string()
        }
    };

    match crate::runtime::list_all_pods().await {
        Ok(pods) => {
            let mut instances = serde_json::Map::new();

            for pod in &pods {
                let pod_id = &pod.id;
                let pod_name = &pod.name;
                let pod_status = match &pod.status {
                    crate::runtimes::PodStatus::Running => "Running",
                    crate::runtimes::PodStatus::Starting => "Starting",
                    crate::runtimes::PodStatus::Stopped => "Stopped",
                    crate::runtimes::PodStatus::Failed(reason) => reason.as_str(),
                    crate::runtimes::PodStatus::Unknown => "Unknown",
                };
                let labels = &pod.metadata;

                if pod_id.is_empty() {
                    continue;
                }

                let created_timestamp = format_debug_timestamp(&pod.created_at);

                let workload_namespace = labels
                    .get("magik.namespace")
                    .cloned()
                    .unwrap_or_else(|| pod.namespace.clone());
                let workload_kind = labels
                    .get("magik.kind")
                    .cloned()
                    .unwrap_or_else(|| pod.kind.clone());
                let workload_name = labels
                    .get("magik.name")
                    .cloned()
                    .unwrap_or_else(|| pod.name.clone());

                let app_name = labels
                    .get("app.kubernetes.io/name")
                    .or_else(|| labels.get("app"))
                    .cloned()
                    .unwrap_or_default();

                let mut metadata = serde_json::Map::new();
                metadata.insert(
                    "name".to_string(),
                    serde_json::Value::String(app_name.clone()),
                );
                for (k, v) in labels {
                    metadata.insert(k.clone(), serde_json::Value::String(v.clone()));
                }

                instances.insert(
                    pod_id.clone(),
                    serde_json::json!({
                        "id": pod_id,
                        "name": pod_name,
                        "status": pod_status,
                        "workload": {
                            "namespace": workload_namespace,
                            "kind": workload_kind,
                            "name": workload_name
                        },
                        "metadata": metadata,
                        "created": created_timestamp,
                        "ports": pod.ports
                    }),
                );
            }

            axum::Json(serde_json::json!({
                "ok": true,
                "local_identity": local_identity,
                "instances": instances,
                "instance_count": instances.len()
            }))
        }
        Err(e) => axum::Json(serde_json::json!({
            "ok": false,
            "error": format!("Failed to list pods: {}", e),
            "local_identity": local_identity,
            "instances": {},
            "instance_count": 0
        })),
    }
}

async fn debug_all_tenders(State(state): State<RestState>) -> axum::Json<serde_json::Value> {
    let tracker = state.tender_tracker.read().await;
    let mut tenders = serde_json::Map::new();

    for (tender_id, record) in tracker.iter() {
        tenders.insert(
            tender_id.clone(),
            serde_json::json!({
                "created_at": record
                    .created_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                "assigned_peers": record.assigned_peers,
                "manifest_bytes_len": record.manifest_bytes.len()
            }),
        );
    }

    axum::Json(serde_json::json!({
        "ok": true,
        "tenders": serde_json::Value::Object(tenders)
    }))
}

pub async fn get_tender_status(
    Path(tender_id): Path<String>,
    State(state): State<RestState>,
    _headers: HeaderMap,
) -> Result<axum::response::Response<axum::body::Body>, axum::http::StatusCode> {
    let maybe = { state.tender_tracker.read().await.get(&tender_id).cloned() };
    if let Some(r) = maybe {
        let assigned = r.assigned_peers.unwrap_or_default();

        let response = crate::messages::TenderStatusResponse {
            tender_id: tender_id.clone(),
            state: "Pending".to_string(),
            assigned_peers: assigned,
        };
        let response_data = bincode::serialize(&response)
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
        return create_response_with_fallback(&response_data).await;
    }
    let error_response = bincode::serialize(&crate::messages::TenderStatusResponse {
        tender_id: String::new(),
        state: "Error".to_string(),
        assigned_peers: vec![],
    })
    .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
    create_response_with_fallback(&error_response).await
}

pub async fn delete_tender(
    Path((namespace, kind, name)): Path<(String, String, String)>,
    State(state): State<RestState>,
    _headers: HeaderMap,
    _body: Bytes,
) -> Result<axum::response::Response<axum::body::Body>, axum::http::StatusCode> {
    let resource_coord = format!("{}/{}/{}", namespace, kind, name);
    info!("delete_tender: resource={}", resource_coord);

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .as_millis() as u64;

    let disposal = crate::messages::Disposal {
        namespace: namespace.clone(),
        kind: kind.clone(),
        name: name.clone(),
        timestamp,
        nonce: rand::thread_rng().next_u64(),
    };

    let disposal_bytes =
        bincode::serialize(&disposal).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (reply_tx, mut reply_rx) = mpsc::unbounded_channel();
    state
        .control_tx
        .send(NetworkControl::PublishDisposal {
            payload: disposal_bytes,
            reply_tx,
        })
        .map_err(|e| {
            error!("delete_tender: failed to send disposal: {}", e);
            StatusCode::BAD_GATEWAY
        })?;

    match timeout(Duration::from_secs(5), reply_rx.recv()).await {
        Ok(Some(Ok(_))) => {
            info!(
                "delete_tender: disposal published for resource={}",
                resource_coord
            );
        }
        Ok(Some(Err(e))) => {
            log::warn!("delete_tender: disposal publish failed: {}", e);
            return Err(StatusCode::BAD_GATEWAY);
        }
        Ok(None) | Err(_) => {
            log::warn!("delete_tender: disposal publish timeout");
            return Err(StatusCode::GATEWAY_TIMEOUT);
        }
    }

    let response = crate::messages::DeleteResponse {
        ok: true,
        operation_id: format!("disposal-{}", timestamp),
        message: "Disposal published to fabric".to_string(),
        resource: resource_coord,
        removed_pods: vec![], // Fire-and-forget - no immediate confirmation
    };
    let response_data =
        bincode::serialize(&response).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    axum::response::Response::builder()
        .status(200)
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(response_data))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn get_disposal_status(
    Path((namespace, kind, name)): Path<(String, String, String)>,
) -> axum::Json<serde_json::Value> {
    let resource_key = format!("{}/{}/{}", namespace, kind, name);
    let disposing = crate::scheduler::is_disposing(&resource_key).await;
    axum::Json(serde_json::json!({
        "resource": resource_key,
        "disposing": disposing
    }))
}
