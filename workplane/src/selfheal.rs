//! # Self-Healing and Health Monitoring
//!
//! This module provides automatic health checking and replica reconciliation
//! for workplane agents. It monitors local pod health and triggers scaling
//! operations when replica counts drift from the desired state.
//!
//! # Features
//!
//! - **Health Probes**: Periodic liveness and readiness checks
//! - **DHT Updates**: Announces health status to the service discovery DHT
//! - **Replica Reconciliation**: Detects under-replication and triggers new deployments
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │                      SelfHealer                             │
//! ├────────────────────────────────────────────────────────────┤
//! │  Health Probe Loop                                          │
//! │    ├── HTTP GET liveness_url                               │
//! │    ├── HTTP GET readiness_url                              │
//! │    └── Update DHT with status                              │
//! │                                                             │
//! │  Replica Reconciliation Loop                                │
//! │    ├── Query DHT for healthy peers                         │
//! │    ├── Compare with desired replica count                  │
//! │    └── Trigger tender if under-replicated                  │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Security Considerations
//!
//! - Health probe timeouts prevent hanging on unresponsive endpoints
//! - Reconciliation is rate-limited to prevent tender storms

use std::collections::HashSet;
use std::process;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use reqwest::Client;
use serde::Serialize;
use serde_json::json;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};
use tokio::{select, sync::watch};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::discovery;
use crate::discovery::ServiceRecord;
use crate::network::Network;
use crate::rpc::RPCRequest;

/// Tender request for triggering new deployments.
///
/// Sent to the machineplane API when replica count is below desired.
#[derive(Debug, Serialize, serde::Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Tender {
    /// Unique tender identifier
    pub tender_id: String,

    /// Workload kind (Deployment, StatefulSet, etc.)
    pub kind: String,

    /// Workload name
    pub name: String,

    /// Target destination (for routing)
    pub destination: String,

    /// Whether this is a clone request
    pub clone_request: bool,

    /// Number of replicas needed
    pub replicas: usize,

    /// Kubernetes namespace
    pub namespace: String,

    /// Optional workload spec for new deployments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<serde_json::Value>,
}

/// Health monitoring and replica reconciliation service.
///
/// Runs background loops for:
/// - Periodic health probes (liveness/readiness)
/// - DHT status updates
/// - Replica count reconciliation
pub struct SelfHealer {
    network: Network,
    config: Config,
    http: Client,
    stop_tx: Option<watch::Sender<bool>>,
    handle: Option<JoinHandle<()>>,
}

impl SelfHealer {
    /// Creates a new self-healer with the given network and configuration.
    pub fn new(network: Network, config: Config) -> Self {
        let timeout = config.health_probe_timeout;
        Self {
            network,
            config,
            http: Client::builder()
                .timeout(timeout)
                .build()
                .expect("reqwest client"),
            stop_tx: None,
            handle: None,
        }
    }

    /// Starts the background health check and reconciliation loops.
    ///
    /// This is idempotent - calling multiple times has no effect.
    pub fn start(&mut self) {
        if self.handle.is_some() {
            return; // Already running
        }
        let (tx, mut rx) = watch::channel(false);
        self.stop_tx = Some(tx.clone());

        let cfg = self.config.clone();
        let network = self.network.clone();
        let client = self.http.clone();

        let handle = tokio::spawn(async move {
            let interval_duration = if cfg.health_probe_interval < cfg.replica_check_interval {
                cfg.health_probe_interval
            } else {
                cfg.replica_check_interval
            };
            let mut ticker = interval(interval_duration);
            ticker.tick().await; // Immediate first tick

            let mut last_health_probe = Instant::now() - cfg.health_probe_interval;
            let mut last_replica_check = Instant::now() - cfg.replica_check_interval;

            loop {
                select! {
                    _ = ticker.tick() => {
                        let now = Instant::now();

                        if now.duration_since(last_health_probe) >= cfg.health_probe_interval {
                            if let Err(err) = update_local_health(&network, &client, &cfg).await {
                                warn!(error = %err, "health probe failed");
                            }
                            last_health_probe = now;
                        }

                        if now.duration_since(last_replica_check) >= cfg.replica_check_interval {
                            match reconcile_replicas(&network, &cfg, &client).await {
                                Ok(()) => {
                                    crate::gauge!("workplane.reconciliation.failures", 0.0);
                                }
                                Err(err) => {
                                    crate::increment_counter!(
                                        "workplane.reconciliation.failures"
                                    );
                                    crate::gauge!("workplane.reconciliation.failures", 1.0);
                                    warn!(error = %err, "replica reconciliation failed");
                                }
                            }
                            last_replica_check = now;
                        }
                    }
                    changed = rx.changed() => {
                        if changed.is_ok() && *rx.borrow() {
                            break;
                        }
                    }
                }
            }
            debug!("self-healer stopped");
        });
        self.handle = Some(handle);
    }

    /// Stops the background loops gracefully.
    pub async fn stop(&mut self) {
        if let Some(tx) = &self.stop_tx {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
        self.stop_tx = None;
    }
}

/// Performs health probes and updates the local DHT record.
async fn update_local_health(network: &Network, client: &Client, cfg: &Config) -> Result<()> {
    let status = perform_local_health_check(client, cfg).await;
    let ready = status.liveness_ok && status.readiness_ok;
    network
        .update_local_status(status.liveness_ok, ready)
        .context("update WDHT with local health")
}

/// Checks replica count and triggers scaling if under-replicated.
async fn reconcile_replicas(network: &Network, cfg: &Config, client: &Client) -> Result<()> {
    crate::gauge!("workplane.reconciliation.scale_up", 0.0);
    crate::gauge!("workplane.reconciliation.scale_down", 0.0);

    if !policy_allows_workload(cfg, &cfg.namespace, &cfg.workload_name) {
        debug!(
            namespace = %cfg.namespace,
            workload = %cfg.workload_name,
            "skipping reconciliation due to workload policy",
        );
        crate::gauge!("workplane.reconciliation.skipped_due_to_policy", 1.0);
        crate::increment_counter!("workplane.reconciliation.skipped_due_to_policy");
        return Ok(());
    }
    crate::gauge!("workplane.reconciliation.skipped_due_to_policy", 0.0);

    let desired_replicas = cfg.replicas.max(1);
    let relevant_records = current_workload_records(network, cfg);

    let mut evaluated_records = Vec::new();
    for mut record in relevant_records {
        if record.ready && record.healthy && !wdht_healthtest(network, &record).await {
            record.ready = false;
            record.healthy = false;
        }
        evaluated_records.push(record);
    }

    let (mut healthy, mut unhealthy): (Vec<ServiceRecord>, Vec<ServiceRecord>) = evaluated_records
        .into_iter()
        .partition(|record| record.ready && record.healthy);

    healthy.sort_by(rank_records);
    unhealthy.sort_by(rank_records);

    info!(
        healthy = healthy.len(),
        unhealthy = unhealthy.len(),
        desired = desired_replicas,
        namespace = %cfg.namespace,
        workload = %cfg.workload_name,
        "replica health evaluated"
    );
    crate::gauge!("workplane.replicas.healthy", healthy.len() as f64);
    crate::gauge!("workplane.replicas.unhealthy", unhealthy.len() as f64);

    let mut duplicates = Vec::new();
    if cfg.is_stateful() {
        let mut seen = HashSet::new();
        let mut deduped = Vec::new();
        for record in healthy.into_iter() {
            if let Some(ord) = record.ordinal
                && !seen.insert(ord)
            {
                duplicates.push(record);
                continue;
            }
            deduped.push(record);
        }
        healthy = deduped;
    }

    if check_disposal_status(client, cfg).await.unwrap_or(false) {
        self_terminate(&cfg.workload_id());
    }

    if healthy.len() < desired_replicas {
        let deficit = desired_replicas - healthy.len();
        crate::gauge!("workplane.reconciliation.scale_up", deficit as f64);
        crate::increment_counter!("workplane.reconciliation.scale_up", deficit);

        if cfg.is_stateful() {
            let missing_ordinals = missing_stateful_ordinals(desired_replicas, &healthy);
            for ordinal in missing_ordinals.into_iter().take(deficit) {
                request_new_replica(cfg, client, Some(ordinal)).await?;
            }
        } else {
            for _ in 0..deficit {
                request_new_replica(cfg, client, None).await?;
            }
        }
    }

    let mut removal_candidates = Vec::new();
    removal_candidates.extend(duplicates);
    removal_candidates.extend(unhealthy);

    let mut excess = healthy.len().saturating_sub(desired_replicas);
    if excess > 0 {
        let mut iter = healthy.into_iter().rev();
        while excess > 0 {
            if let Some(record) = iter.next() {
                removal_candidates.push(record);
            } else {
                break;
            }
            excess -= 1;
        }
    }

    let removal_total = removal_candidates.len();
    crate::gauge!("workplane.reconciliation.scale_down", removal_total as f64);
    if removal_total > 0 {
        crate::increment_counter!("workplane.reconciliation.scale_down", removal_total);
    }

    for record in removal_candidates {
        if let Err(err) = remove_replica(cfg, client, &record).await {
            warn!(
                error = %err,
                peer = %record.peer_id,
                namespace = %record.namespace,
                workload = %record.workload_name,
                "failed to remove replica"
            );
        }
    }

    Ok(())
}

async fn publish_clone_tender(cfg: &Config, client: &Client, ordinal: Option<u32>) -> Result<()> {
    let tender = Tender {
        tender_id: Uuid::new_v4().to_string(),
        kind: cfg.task_kind().to_string(),
        name: cfg.workload_name.clone(),
        destination: "scheduler".to_string(),
        clone_request: true,
        replicas: 1,
        namespace: cfg.namespace.clone(),
        spec: ordinal.map(|ord| json!({ "ordinal": ord })),
    };

    let url = format!("{}/v1/publish_tender", cfg.magik_api.trim_end_matches('/'));
    let resp = client
        .post(url)
        .json(&tender)
        .send()
        .await
        .context("send clone tender")?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "publish clone tender failed with status {}",
            resp.status()
        ));
    }
    Ok(())
}

async fn request_new_replica(cfg: &Config, client: &Client, ordinal: Option<u32>) -> Result<()> {
    let action = || async { publish_clone_tender(cfg, client, ordinal).await };
    retry_with_backoff(action, cfg.replica_check_interval).await
}

#[derive(Serialize)]
struct ReplicaRemovalRequest {
    namespace: String,
    workload: String,
    peer_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pod_name: Option<String>,
}

async fn remove_replica(cfg: &Config, client: &Client, record: &ServiceRecord) -> Result<()> {
    let base = cfg.magik_api.trim_end_matches('/');
    let url = format!("{base}/v1/remove_replica");
    let request = ReplicaRemovalRequest {
        namespace: record.namespace.clone(),
        workload: record.workload_name.clone(),
        peer_id: record.peer_id.clone(),
        pod_name: record.pod_name.clone(),
    };
    let action = || async {
        let resp = client
            .post(url.clone())
            .json(&request)
            .send()
            .await
            .context("send remove replica request")?;
        if resp.status().is_success() {
            discovery::remove(&record.workload_id, &record.peer_id);
            Ok(())
        } else {
            Err(anyhow!(
                "remove replica failed with status {}",
                resp.status()
            ))
        }
    };
    retry_with_backoff(action, cfg.replica_check_interval).await
}

async fn check_disposal_status(client: &Client, cfg: &Config) -> Result<bool> {
    let workload_id = cfg.workload_id();
    let url = format!(
        "{}/disposal/{}",
        cfg.magik_api.trim_end_matches('/'),
        workload_id
    );

    let resp = client
        .get(&url)
        .send()
        .await
        .context("check disposal status")?;

    if !resp.status().is_success() {
        return Ok(false);
    }

    let body: serde_json::Value = resp.json().await.context("parse disposal response")?;
    Ok(body
        .get("disposing")
        .and_then(|v| v.as_bool())
        .unwrap_or(false))
}

fn self_terminate(workload_id: &str) -> ! {
    error!(
        workload_id = %workload_id,
        "Disposal detected - self-terminating workload (SIGTERM)"
    );
    process::exit(143);
}

struct HealthStatus {
    liveness_ok: bool,
    readiness_ok: bool,
}

async fn perform_local_health_check(client: &Client, cfg: &Config) -> HealthStatus {
    let liveness_ok = if cfg.liveness_url.is_empty() {
        true
    } else {
        probe_url(client, &cfg.liveness_url).await
    };

    let readiness_ok = if cfg.readiness_url.is_empty() {
        true
    } else {
        probe_url(client, &cfg.readiness_url).await
    };

    HealthStatus {
        liveness_ok,
        readiness_ok,
    }
}

async fn probe_url(client: &Client, url: &str) -> bool {
    match client.get(url).send().await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

fn missing_stateful_ordinals(desired_replicas: usize, records: &[ServiceRecord]) -> Vec<u32> {
    let mut present = std::collections::HashSet::new();
    for record in records {
        if let Some(ord) = record.ordinal {
            present.insert(ord);
        }
    }
    (0..desired_replicas as u32)
        .filter(|ord| !present.contains(ord))
        .collect()
}

fn current_workload_records(network: &Network, cfg: &Config) -> Vec<ServiceRecord> {
    let workload_id = cfg.workload_id();
    network
        .find_service_peers()
        .into_iter()
        .filter(|record| record.workload_id == workload_id)
        .filter(|record| policy_allows_workload(cfg, &record.namespace, &record.workload_name))
        .collect()
}

fn policy_allows_workload(cfg: &Config, namespace: &str, workload_name: &str) -> bool {
    let workload_id = format!("{namespace}/{workload_name}");

    if cfg
        .denied_workloads
        .iter()
        .any(|denied| denied == &workload_id)
    {
        return false;
    }

    if !cfg.allow_cross_namespace && namespace != cfg.namespace {
        return false;
    }

    if cfg.allowed_workloads.is_empty() {
        return true;
    }

    cfg.allowed_workloads
        .iter()
        .any(|allowed| allowed == &workload_id)
}

fn rank_records(a: &ServiceRecord, b: &ServiceRecord) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let key = |record: &ServiceRecord| {
        (
            record.ready && record.healthy,
            record.ready,
            record.healthy,
            record.ts,
            record.version,
            record.peer_id.clone(),
        )
    };
    match key(b).cmp(&key(a)) {
        Ordering::Equal => b.peer_id.cmp(&a.peer_id),
        other => other,
    }
}

async fn wdht_healthtest(network: &Network, record: &ServiceRecord) -> bool {
    let request = RPCRequest {
        method: "healthz".to_string(),
        body: serde_json::Map::new(),
        leader_only: false,
    };
    match network.send_request(&record.peer_id, request).await {
        Ok(response) => response.ok,
        Err(_) => false,
    }
}

async fn retry_with_backoff<F, Fut>(mut action: F, max_interval: Duration) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut delay = Duration::from_millis(200);
    let max_delay = std::cmp::min(Duration::from_secs(5), max_interval);
    let max_attempts = 5;

    for attempt in 1..=max_attempts {
        match action().await {
            Ok(()) => return Ok(()),
            Err(err) if attempt == max_attempts => return Err(err),
            Err(err) => {
                warn!(attempt, error = %err, "retrying HTTP operation");
                sleep(delay).await;
                delay = std::cmp::min(delay.saturating_mul(2), max_delay);
            }
        }
    }
    Ok(())
}
