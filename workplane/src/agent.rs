//! # Agent
//!
//! This module provides the main agent logic for workplane nodes.
//! The agent orchestrates all subsystems (network, Raft, self-healing)
//! and handles the agent lifecycle.
//!
//! # Responsibilities
//!
//! - Initialize network layer and connect to bootstrap peers
//! - Set up RPC handlers for health checks and Raft messages
//! - Run Raft consensus for leader election
//! - Start self-healing loops for health monitoring and replica reconciliation
//!
//! # Lifecycle
//!
//! ```text
//! Agent::new() ──▶ Network::new() ──▶ register_stream_handler()
//!      │
//!      ▼
//! Agent::run() ──▶ start_heartbeat() ──▶ start_selfhealer()
//!      │
//!      ▼
//! Main loop (Raft tick + RPC handling)
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use workplane::{Config, Agent};
//!
//! let config = Config { /* ... */ };
//! let agent = Agent::new(config).await?;
//! agent.run().await?;
//! ```

use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Result, anyhow};
use serde_json::{Map, Value};
use tokio::task::JoinHandle;
use tokio::{select, sync::watch, time::interval};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::discovery::ServiceRecord;
use crate::network::Network;
use crate::raft::{
    Heartbeat, LeadershipUpdate, RaftAction, RaftManager, RaftRole, VoteRequest, VoteResponse,
};
use crate::rpc::{RPCRequest, RPCResponse, register_stream_handler};
use crate::selfheal::SelfHealer;

/// A workplane agent instance.
///
/// Manages the lifecycle of all agent subsystems and coordinates
/// their interaction.
pub struct Agent {
    /// Agent configuration
    cfg: Config,

    /// Network layer for mesh communication
    pub network: Network,

    /// Raft state machine for leader election
    pub raft: Option<Arc<RwLock<RaftManager>>>,

    /// Self-healing service for health monitoring
    selfhealer: Option<SelfHealer>,

    /// Current known leader (shared with RPC handler)
    leader_state: Arc<RwLock<Option<String>>>,

    /// Background task monitoring leader changes
    leader_monitor: Option<LeaderMonitor>,
}

impl Agent {
    /// Creates a new agent with the given configuration.
    ///
    /// Initializes the network layer and registers RPC handlers,
    /// but does not start background tasks yet. Call [`run`](Self::run)
    /// to start the agent.
    pub async fn new(cfg: Config) -> Result<Self> {
        if cfg.workload_name.is_empty() || cfg.pod_name.is_empty() {
            return Err(anyhow!("config: workload and pod name required"));
        }
        let mut cfg = cfg;
        cfg.apply_defaults();

        let network = Network::new(&cfg).await?;
        cfg.peer_id_str = Some(network.peer_id());

        //

        let leader_state: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));
        let handler_network = network.clone();
        let local_peer_id = handler_network.peer_id();
        let leader_state_for_handler = leader_state.clone();

        register_stream_handler(Arc::new(move |remote: &ServiceRecord, req: RPCRequest| {
            let leader_state = leader_state_for_handler.clone();
            let handler_network = handler_network.clone();
            let local_peer_id = local_peer_id.clone();
            let remote_peer_id = remote.peer_id.clone();
            let remote_pod = remote.pod_name.clone();

            Box::pin(async move {
                if req.method.eq_ignore_ascii_case("healthz") {
                    let mut body = Map::new();
                    body.insert("remote".to_string(), Value::String(remote_peer_id));
                    if let Some(pod) = remote_pod {
                        body.insert("pod".to_string(), Value::String(pod));
                    }
                    return RPCResponse {
                        ok: true,
                        error: None,
                        body,
                    };
                }

                //

                let leader = leader_state.read().expect("leader state").clone();
                let leader_matches = leader
                    .as_ref()
                    .map(|leader_id| leader_id == &local_peer_id)
                    .unwrap_or(false);

                if req.leader_only && !leader_matches {
                    if let Some(leader_id) = leader {
                        debug!(target = %leader_id, method = %req.method, "proxying follower request to leader");
                        match handler_network.send_request(&leader_id, req.clone()).await {
                            Ok(resp) => return resp,
                            Err(err) => {
                                warn!(error = %err, method = %req.method, "failed to proxy request to leader");
                                crate::increment_counter!("workplane.raft.follower_rejections");
                                return RPCResponse {
                                    ok: false,
                                    error: Some("leader unreachable".to_string()),
                                    body: Map::new(),
                                };
                            }
                        }
                    }
                    crate::increment_counter!("workplane.raft.follower_rejections");
                    return RPCResponse {
                        ok: false,
                        error: Some("not leader".to_string()),
                        body: Map::new(),
                    };
                }

                let mut body = Map::new();
                body.insert("handled_by".to_string(), Value::String(local_peer_id));
                RPCResponse {
                    ok: true,
                    error: None,
                    body,
                }
            })
        }));

        let mut raft = None;
        if cfg.is_stateful() {
            let peer_id = network.peer_id();
            let workload_id = cfg.workload_id();
            let mut manager = RaftManager::new(peer_id.clone(), workload_id);

            let peer_ids: std::collections::HashSet<String> = network
                .find_service_peers()
                .into_iter()
                .map(|p| p.peer_id)
                .collect();
            manager.update_peers(peer_ids);

            let actions = manager.bootstrap_if_needed();

            let mut new_leader = None;
            for action in &actions {
                if let crate::raft::RaftAction::LeadershipChanged(update) = action {
                    new_leader = update.leader_id.clone();
                }
            }
            *leader_state.write().expect("leader state") = new_leader;
            raft = Some(Arc::new(RwLock::new(manager)));
        } else {
            *leader_state.write().expect("leader state") = Some(network.peer_id());
        }

        let selfhealer = Some(SelfHealer::new(network.clone(), cfg.clone()));

        Ok(Self {
            cfg,
            network,
            raft,
            selfhealer,
            leader_state,
            leader_monitor: None,
        })
    }

    pub fn start(&mut self) {
        self.network.start();
        if let Some(sh) = self.selfhealer.as_mut() {
            sh.start();
        }
        if self.is_stateful() {
            self.start_leader_monitor();
        }
    }

    pub async fn close(&mut self) {
        if let Some(monitor) = self.leader_monitor.take() {
            monitor.stop().await;
        }
        if let Some(sh) = self.selfhealer.as_mut() {
            sh.stop().await;
        }
        self.network.shutdown().await;
        info!("agent shut down");
    }

    pub fn is_stateful(&self) -> bool {
        self.cfg.is_stateful()
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    fn start_leader_monitor(&mut self) {
        if self.leader_monitor.is_some() {
            return; // Already running
        }
        let Some(raft) = self.raft.clone() else {
            return; // No Raft manager
        };

        let monitor = LeaderMonitor::new(
            self.cfg.clone(),
            self.network.clone(),
            raft,
            self.leader_state.clone(),
        );
        self.leader_monitor = Some(monitor);
    }
}

struct LeaderMonitor {
    stop_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl LeaderMonitor {
    fn new(
        cfg: Config,
        network: Network,
        raft: Arc<RwLock<RaftManager>>,
        leader_state: Arc<RwLock<Option<String>>>,
    ) -> Self {
        let (tx, mut rx) = watch::channel(false);

        let raft_tick_interval = Duration::from_millis(50);
        let discovery_interval = cfg.replica_check_interval;

        let handle = tokio::spawn(async move {
            let mut raft_ticker = interval(raft_tick_interval);
            let mut discovery_ticker = interval(discovery_interval);
            raft_ticker.tick().await; // Skip first tick
            discovery_ticker.tick().await;

            loop {
                select! {
                    _ = raft_ticker.tick() => {
                        let actions = {
                            let mut guard = raft.write().expect("raft lock");
                            guard.tick()
                        };

                        for action in actions {
                            match action {
                                RaftAction::SendHeartbeat(hb) => {
                                    LeaderMonitor::broadcast_heartbeat(&network, &cfg, &hb).await;
                                }
                                RaftAction::RequestVotes(req) => {
                                    LeaderMonitor::request_votes(&network, &cfg, &raft, &leader_state, req).await;
                                }
                                RaftAction::LeadershipChanged(update) => {
                                    {
                                        let mut guard = leader_state.write().expect("leader state");
                                        *guard = update.leader_id.clone();
                                    }
                                    LeaderMonitor::apply_network_update(&network, &update);
                                }
                            }
                        }
                    }

                    _ = discovery_ticker.tick() => {
                        let records = network.find_service_peers();
                        let relevant: std::collections::HashSet<String> = records
                            .into_iter()
                            .filter(|record| record.workload_id == cfg.workload_id())
                            .map(|r| r.peer_id)
                            .collect();
                        {
                            let mut guard = raft.write().expect("raft lock");
                            guard.update_peers(relevant);
                        }
                    }

                    changed = rx.changed() => {
                        if changed.is_ok() && *rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        Self {
            stop_tx: tx,
            handle,
        }
    }

    async fn broadcast_heartbeat(network: &Network, cfg: &Config, heartbeat: &Heartbeat) {
        let records = network.find_service_peers();
        let peers: Vec<_> = records
            .into_iter()
            .filter(|r| r.workload_id == cfg.workload_id() && r.peer_id != heartbeat.leader_id)
            .collect();

        for peer in peers {
            let body = match serde_json::to_value(heartbeat) {
                Ok(Value::Object(map)) => map,
                _ => Map::new(),
            };
            let req = RPCRequest {
                method: "raft.heartbeat".to_string(),
                leader_only: false,
                body,
            };

            if let Err(e) = network.send_request(&peer.peer_id, req).await {
                debug!(peer = %peer.peer_id, error = %e, "failed to send heartbeat");
            }
        }
    }

    async fn request_votes(
        network: &Network,
        cfg: &Config,
        raft: &Arc<RwLock<RaftManager>>,
        leader_state: &Arc<RwLock<Option<String>>>,
        vote_request: VoteRequest,
    ) {
        let records = network.find_service_peers();
        let peers: Vec<_> = records
            .into_iter()
            .filter(|r| {
                r.workload_id == cfg.workload_id() && r.peer_id != vote_request.candidate_id
            })
            .collect();

        for peer in peers {
            let body = match serde_json::to_value(&vote_request) {
                Ok(Value::Object(map)) => map,
                _ => Map::new(),
            };
            let req = RPCRequest {
                method: "raft.vote".to_string(),
                leader_only: false,
                body,
            };

            match network.send_request(&peer.peer_id, req).await {
                Ok(resp) if resp.ok => {
                    if let Ok(vote_resp) =
                        serde_json::from_value::<VoteResponse>(serde_json::Value::Object(resp.body))
                    {
                        let actions = {
                            let mut guard = raft.write().expect("raft lock");
                            guard.handle_vote_response(vote_resp)
                        };

                        for action in actions {
                            if let RaftAction::LeadershipChanged(update) = action {
                                {
                                    let mut guard = leader_state.write().expect("leader state");
                                    *guard = update.leader_id.clone();
                                }
                                LeaderMonitor::apply_network_update(network, &update);
                            }
                        }
                    }
                }
                Ok(_) => {
                    debug!(peer = %peer.peer_id, "vote request rejected");
                }
                Err(e) => {
                    debug!(peer = %peer.peer_id, error = %e, "failed to request vote");
                }
            }
        }
    }

    async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = self.handle.await;
    }

    fn apply_network_update(network: &Network, update: &LeadershipUpdate) {
        match update.role {
            RaftRole::Leader => {
                if let Err(err) = network.update_leader_endpoint(true, update.term) {
                    warn!(error = %err, "failed to advertise leader endpoint");
                }
            }
            RaftRole::Follower => {
                if let Err(err) = network.update_leader_endpoint(false, update.term) {
                    warn!(error = %err, "failed to withdraw leader endpoint");
                }
            }
            RaftRole::Detached => {}
        }
    }
}
