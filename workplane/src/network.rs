//! # Mesh Network Layer
//!
//! This module provides the networking layer for workplane agents using the
//! Korium peer-to-peer library. It handles:
//!
//! - **Node Identity**: Each agent has a unique cryptographic identity
//! - **Service Discovery**: Gossipsub-based peer announcements
//! - **RPC Communication**: Request/response messaging between peers
//! - **Policy Enforcement**: Cross-namespace and workload filtering
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │                      Network Layer                          │
//! ├────────────────────────────────────────────────────────────┤
//! │  Heartbeat Loop ─────┬──────▶ Gossipsub (discovery)        │
//! │                      │                                      │
//! │  RPC Requests  ──────┼──────▶ Korium Node ◀───── Peers     │
//! │                      │                                      │
//! │  Policy Engine ──────┴──────▶ Allow/Deny decisions         │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Security Features
//!
//! - Message size limits ([`MAX_RPC_MESSAGE_SIZE`])
//! - Request timeouts ([`RPC_TIMEOUT_SECS`])
//! - Bounded pending request queue ([`MAX_PENDING_REQUESTS`])
//! - Policy-based cross-namespace filtering

use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use korium::Node;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::discovery::{self, ServiceRecord};
use crate::rpc::{RPCRequest, RPCResponse};

/// Maximum size for RPC messages (16 MiB).
const MAX_RPC_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// RPC message prefixes for framing.
#[allow(dead_code)]
const RPC_REQUEST_PREFIX: &[u8] = b"WRQ:";
#[allow(dead_code)]
const RPC_RESPONSE_PREFIX: &[u8] = b"WRS:";

/// Timeout for RPC requests (seconds).
const RPC_TIMEOUT_SECS: u64 = 30;

/// The network layer for a workplane agent.
///
/// Provides methods for:
/// - Sending RPC requests to peers
/// - Updating local health status in the DHT
/// - Dialing bootstrap peers
#[derive(Clone)]
pub struct Network {
    inner: Arc<NetworkInner>,
    cmd_tx: mpsc::Sender<NetworkCommand>,
}

/// Internal network state.
struct NetworkInner {
    cfg: Config,
    local_record: RwLock<ServiceRecord>,
    heartbeat: Mutex<Option<HeartbeatHandle>>,
    policy: PolicyEngine,
    identity: String,
}

/// Handle for the background heartbeat task.
struct HeartbeatHandle {
    stop_tx: watch::Sender<bool>,
    join_handle: JoinHandle<()>,
}

/// Policy engine for filtering cross-namespace and workload access.
#[derive(Clone)]
struct PolicyEngine {
    allow_cross_namespace: bool,
    allowed: HashSet<String>,
    denied: HashSet<String>,
}

/// Internal commands for the network event loop.
enum NetworkCommand {
    /// Publish a service record via Gossipsub
    PublishRecord {
        record: ServiceRecord,
        ttl: Duration,
    },
    /// Send an RPC request to a peer
    SendRequest {
        target: String,
        request: RPCRequest,
        request_id: String,
        reply_tx: oneshot::Sender<Result<RPCResponse>>,
    },
    /// Dial a peer address
    Dial {
        addr: String,
        reply_tx: oneshot::Sender<Result<()>>,
    },
}

impl Network {
    /// Creates a new network layer and starts the background event loop.
    pub async fn new(cfg: &Config) -> Result<Self> {
        if cfg.workload_name.is_empty() || cfg.pod_name.is_empty() {
            return Err(anyhow!("config: workload name and pod name are required"));
        }

        let listen_addr = cfg.listen_addrs.first()
            .map(|s| s.as_str())
            .unwrap_or("0.0.0.0:0");
        
        let node = Node::bind(listen_addr).await
            .context("failed to bind Korium node")?;
        
        let identity = node.identity();

        let mut caps = serde_json::Map::new();
        caps.insert(
            "namespace".into(),
            serde_json::Value::String(cfg.namespace.clone()),
        );
        caps.insert(
            "workload".into(),
            serde_json::Value::String(cfg.workload_name.clone()),
        );
        caps.insert(
            "pod".into(),
            serde_json::Value::String(cfg.pod_name.clone()),
        );
        if let Some(ord) = cfg.ordinal() {
            caps.insert("ordinal".into(), serde_json::json!(ord));
        }

        let record = ServiceRecord {
            workload_id: cfg.workload_id(),
            namespace: cfg.namespace.clone(),
            workload_name: cfg.workload_name.clone(),
            peer_id: identity.clone(),
            pod_name: Some(cfg.pod_name.clone()),
            ordinal: cfg.ordinal(),
            addrs: cfg.listen_addrs.clone(),
            caps,
            version: 1,
            ts: current_millis(),
            nonce: Uuid::new_v4().to_string(),
            ready: false,
            healthy: false,
        };

        // Subscribe to workload-specific Gossipsub topic for peer discovery
        let workload_topic = format!("workplane/{}", cfg.workload_id());
        node.subscribe(&workload_topic).await
            .context("failed to subscribe to workload topic")?;
        
        // Get Gossipsub message receiver for peer announcements
        let mut gossip_rx = node.messages().await
            .context("failed to get gossipsub message receiver")?;
        let dht_ttl = cfg.dht_ttl;
        tokio::spawn(async move {
            while let Some(msg) = gossip_rx.recv().await {
                // Parse incoming ServiceRecord announcements
                match serde_json::from_slice::<ServiceRecord>(&msg.data) {
                    Ok(record) => {
                        debug!(
                            workload_id = %record.workload_id,
                            peer_id = %record.peer_id,
                            from = %msg.from,
                            "Received peer announcement via Gossipsub"
                        );
                        // Validate sender matches record peer_id
                        if record.peer_id == msg.from {
                            discovery::put(record, dht_ttl);
                        } else {
                            warn!(
                                from = %msg.from,
                                claimed_peer_id = %record.peer_id,
                                "Rejecting announcement: sender identity mismatch"
                            );
                        }
                    }
                    Err(e) => {
                        debug!(
                            topic = %msg.topic,
                            from = %msg.from,
                            error = %e,
                            "Failed to parse Gossipsub message as ServiceRecord"
                        );
                    }
                }
            }
        });

        // Get request-response RPC receiver for direct peer communication
        let mut rpc_rx = node.incoming_requests().await
            .context("failed to get RPC request receiver")?;
        
        let (rpc_tx, direct_rx) = mpsc::channel::<(String, Vec<u8>, oneshot::Sender<Vec<u8>>)>(256);
        tokio::spawn(async move {
            while let Some((from, data, reply_tx)) = rpc_rx.recv().await {
                if rpc_tx.send((from, data, reply_tx)).await.is_err() {
                    break; // receiver dropped
                }
            }
        });

        const NETWORK_CMD_CHANNEL_CAPACITY: usize = 256;
        let (cmd_tx, cmd_rx) = mpsc::channel(NETWORK_CMD_CHANNEL_CAPACITY);

        let identity_clone = identity.clone();
        let workload_topic_clone = workload_topic.clone();
        tokio::spawn(network_event_loop(node, cmd_rx, identity_clone, direct_rx, workload_topic_clone));

        Ok(Self {
            inner: Arc::new(NetworkInner {
                cfg: cfg.clone(),
                local_record: RwLock::new(record),
                heartbeat: Mutex::new(None),
                policy: PolicyEngine::from_config(cfg),
                identity,
            }),
            cmd_tx,
        })
    }

    pub fn start(&self) {
        for peer_str in &self.inner.cfg.bootstrap_peer_strings {
            let (tx, _rx) = oneshot::channel();
            let _ = self
                .cmd_tx
                .try_send(NetworkCommand::Dial { 
                    addr: peer_str.clone(), 
                    reply_tx: tx,
                });
        }

        if let Err(err) = self.refresh_heartbeat() {
            warn!(error = %err, "failed to publish initial service record");
        }

        let mut guard = self.inner.heartbeat.lock().expect("heartbeat lock");
        if guard.is_some() {
            return; // Already running
        }

        let (tx, mut rx) = watch::channel(false);
        let interval_duration = self.inner.cfg.heartbeat_interval;
        let network = self.clone();
        let join_handle = tokio::spawn(async move {
            let mut ticker = interval(interval_duration);
            ticker.tick().await; // Skip immediate first tick
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(err) = network.refresh_heartbeat() {
                            warn!(error = %err, "failed to refresh WDHT record");
                        }
                    }
                    changed = rx.changed() => {
                        if changed.is_ok() && *rx.borrow() {
                            break;
                        }
                    }
                }
            }
            debug!("network heartbeat stopped");
        });

        *guard = Some(HeartbeatHandle {
            stop_tx: tx,
            join_handle,
        });
    }

    pub async fn shutdown(&self) {
        if let Some(handle) = self.take_heartbeat() {
            let _ = handle.stop_tx.send(true);
            let _ = handle.join_handle.await;
        }
    }

    pub fn update_local_status(&self, healthy: bool, ready: bool) -> Result<()> {
        let mut changed = false;
        {
            let mut record = self.inner.local_record.write().expect("record write lock");
            if record.healthy != healthy {
                record.healthy = healthy;
                changed = true;
            }
            if record.ready != ready {
                record.ready = ready;
                changed = true;
            }
            if changed {
                record.version = record.version.saturating_add(1);
                record.ts = current_millis();
                record.nonce = Uuid::new_v4().to_string();
            }
        }

        if changed {
            self.publish_current_record()?;
        }
        Ok(())
    }

    pub fn update_leader_endpoint(&self, active: bool, epoch: u64) -> Result<()> {
        let mut should_publish = false;
        {
            let mut record = self.inner.local_record.write().expect("record write lock");
            let mut changed = false;

            if record.ready != active {
                record.ready = active;
                changed = true;
            }

            let leader_value = serde_json::Value::Bool(active);
            if record.caps.get("leader") != Some(&leader_value) {
                record.caps.insert("leader".into(), leader_value);
                changed = true;
            }

            let epoch_value = serde_json::Value::Number(serde_json::Number::from(epoch));
            if record.caps.get("leader_epoch") != Some(&epoch_value) {
                record.caps.insert("leader_epoch".into(), epoch_value);
                changed = true;
            }

            if changed {
                record.version = record.version.saturating_add(1);
                record.ts = current_millis();
                record.nonce = Uuid::new_v4().to_string();
                should_publish = true;
            }
        }

        if should_publish {
            if active {
                info!(epoch, "advertising leader endpoint");
                crate::increment_counter!("workplane.network.leader_advertisements");
            } else {
                info!(epoch, "withdrawing leader endpoint");
                crate::increment_counter!("workplane.network.leader_withdrawals");
            }
            self.publish_current_record()?;
        }
        Ok(())
    }

    pub fn find_service_peers(&self) -> Vec<ServiceRecord> {
        let namespace = self.inner.cfg.namespace.clone();
        let kind = self.inner.cfg.workload_kind.clone();
        let workload = self.inner.cfg.workload_name.clone();
        let local_id = self.inner.cfg.workload_id();

        discovery::list(&namespace, &kind, &workload)
            .into_iter()
            .filter(|record| {
                record.workload_id == local_id || self.inner.policy.allows(&namespace, record)
            })
            .collect()
    }

    pub fn peer_id(&self) -> String {
        self.inner.identity.clone()
    }

    fn refresh_heartbeat(&self) -> Result<()> {
        {
            let mut record = self.inner.local_record.write().expect("record write lock");
            record.version = record.version.saturating_add(1);
            record.ts = current_millis();
            record.nonce = Uuid::new_v4().to_string();
        }
        self.publish_current_record()
    }

    fn publish_current_record(&self) -> Result<()> {
        let record = self.local_record();
        let ttl = self.inner.cfg.dht_ttl;

        let cmd = NetworkCommand::PublishRecord { record, ttl };
        let _ = self.cmd_tx.try_send(cmd);

        Ok(())
    }

    fn local_record(&self) -> ServiceRecord {
        self.inner
            .local_record
            .read()
            .expect("record read lock")
            .clone()
    }

    fn take_heartbeat(&self) -> Option<HeartbeatHandle> {
        self.inner.heartbeat.lock().expect("heartbeat lock").take()
    }

    pub async fn send_request(&self, target_peer_id: &str, req: RPCRequest) -> Result<RPCResponse> {
        let request_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();

        self.cmd_tx
            .send(NetworkCommand::SendRequest {
                target: target_peer_id.to_string(),
                request: req,
                request_id,
                reply_tx: tx,
            })
            .await?;

        match tokio::time::timeout(Duration::from_secs(RPC_TIMEOUT_SECS), rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(anyhow!("RPC request channel closed")),
            Err(_) => Err(anyhow!("RPC request timed out")),
        }
    }
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RpcMessage {
    request_id: String,
    payload: RpcPayload,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum RpcPayload {
    Request(RPCRequest),
    Response(RPCResponse),
}

async fn network_event_loop(
    node: Node,
    mut cmd_rx: mpsc::Receiver<NetworkCommand>,
    _local_identity: String,
    mut direct_rx: tokio::sync::mpsc::Receiver<(String, Vec<u8>, oneshot::Sender<Vec<u8>>)>,
    workload_topic: String,
) {
    loop {
        tokio::select! {
            Some((source_identity, payload, reply_tx)) = direct_rx.recv() => {
                if payload.len() > MAX_RPC_MESSAGE_SIZE {
                    warn!(
                        source = %source_identity,
                        size = payload.len(),
                        "Dropping oversized RPC message"
                    );
                    // Send empty response for oversized messages
                    let _ = reply_tx.send(Vec::new());
                    continue;
                }

                match serde_json::from_slice::<RpcMessage>(&payload) {
                    Ok(msg) => {
                        match msg.payload {
                            RpcPayload::Request(request) => {
                                let response = crate::rpc::handle_request(&source_identity, request).await;
                                
                                let response_msg = RpcMessage {
                                    request_id: msg.request_id,
                                    payload: RpcPayload::Response(response),
                                };
                                if let Ok(data) = serde_json::to_vec(&response_msg) {
                                    let _ = reply_tx.send(data);
                                } else {
                                    let _ = reply_tx.send(Vec::new());
                                }
                            }
                            RpcPayload::Response(_response) => {
                                // Responses come via node.send() return value, not via incoming_requests
                                // Log and ignore if we somehow receive one here
                                debug!(
                                    request_id = %msg.request_id,
                                    "Unexpected response received via incoming_requests"
                                );
                                let _ = reply_tx.send(Vec::new());
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            source = %source_identity,
                            error = %e,
                            "Failed to parse RPC message"
                        );
                        let _ = reply_tx.send(Vec::new());
                    }
                }
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    NetworkCommand::PublishRecord { record, ttl } => {
                        if let Ok(data) = serde_json::to_vec(&record) {
                            // Publish to Gossipsub topic for peer discovery
                            if let Err(e) = node.publish(&workload_topic, data).await {
                                warn!(
                                    workload_id = %record.workload_id,
                                    topic = %workload_topic,
                                    error = %e,
                                    "Failed to publish record via Gossipsub"
                                );
                            }
                            // Always update local cache
                            discovery::put(record.clone(), ttl);
                        }
                    }
                    NetworkCommand::SendRequest { target, request, request_id, reply_tx } => {
                        let msg = RpcMessage {
                            request_id: request_id.clone(),
                            payload: RpcPayload::Request(request),
                        };
                        match serde_json::to_vec(&msg) {
                            Ok(data) => {
                                // Korium's send() is request-response: returns the response directly
                                match node.send(&target, data).await {
                                    Ok(response_data) => {
                                        // Parse the response
                                        match serde_json::from_slice::<RpcMessage>(&response_data) {
                                            Ok(response_msg) => {
                                                if let RpcPayload::Response(response) = response_msg.payload {
                                                    let _ = reply_tx.send(Ok(response));
                                                } else {
                                                    let _ = reply_tx.send(Err(anyhow!(
                                                        "Expected response payload, got request"
                                                    )));
                                                }
                                            }
                                            Err(e) => {
                                                let _ = reply_tx.send(Err(anyhow!(
                                                    "Failed to parse RPC response: {}", e
                                                )));
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let _ = reply_tx.send(Err(anyhow!(
                                            "Failed to send RPC request to {}: {}", target, e
                                        )));
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = reply_tx.send(Err(anyhow!(
                                    "Failed to serialize RPC request: {}", e
                                )));
                            }
                        }
                    }
                    NetworkCommand::Dial { addr, reply_tx } => {
                        if let Some((identity, socket_addr)) = addr.split_once('@') {
                            match node.bootstrap(identity, socket_addr).await {
                                Ok(_) => {
                                    let _ = reply_tx.send(Ok(()));
                                }
                                Err(e) => {
                                    let _ = reply_tx.send(Err(anyhow!("Failed to bootstrap to {}: {}", addr, e)));
                                }
                            }
                        } else {
                            let _ = reply_tx.send(Err(anyhow!(
                                "Invalid bootstrap address '{}': expected format 'identity@host:port'", addr
                            )));
                        }
                    }
                }
            }
        }
    }
}


impl PolicyEngine {
    fn from_config(cfg: &Config) -> Self {
        Self {
            allow_cross_namespace: cfg.allow_cross_namespace,
            allowed: cfg.allowed_workloads.iter().cloned().collect(),
            denied: cfg.denied_workloads.iter().cloned().collect(),
        }
    }

    fn allows(&self, local_namespace: &str, record: &ServiceRecord) -> bool {
        let workload_id = format!("{}/{}", record.namespace, record.workload_name);

        if self.denied.contains(&workload_id) {
            return false;
        }

        if !self.allow_cross_namespace && record.namespace != local_namespace {
            return self.allowed.contains(&workload_id);
        }

        if self.allowed.is_empty() {
            return true;
        }

        self.allowed.contains(&workload_id)
    }
}


fn current_millis() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}
