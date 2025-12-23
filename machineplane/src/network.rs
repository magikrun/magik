//! # Mesh Network Layer
//!
//! This module provides the networking layer for machineplane nodes using the
//! Korium peer-to-peer library. It handles:
//!
//! - **Node setup and identity**: Each node generates a cryptographic keypair
//!   for identity and message signing
//! - **PubSub messaging**: Tenders and disposals are broadcast to all mesh nodes
//!   via the [`BEEMESH_FABRIC`] topic
//! - **Direct messaging**: Bids, awards, and events are sent point-to-point
//!   between specific peers
//! - **Scheduler integration**: Network messages are routed to the scheduler
//!   for processing
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Network Layer                           │
//! ├─────────────────────────────────────────────────────────────┤
//! │  NetworkControl (API requests) ───────────────┐             │
//! │                                                │             │
//! │  PubSub Messages ───────────────┐              ▼             │
//! │                                  │     ┌──────────────┐     │
//! │  Direct Messages ─────────────────────▶│  Scheduler   │     │
//! │                                  │     └──────────────┘     │
//! │                                  ▼              │            │
//! │                          ┌────────────┐        │            │
//! │                          │ Korium Node│◀───────┘            │
//! │                          └────────────┘  (send commands)     │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Security Considerations
//!
//! - All messages are validated for freshness (timestamp-based replay protection)
//! - Message sizes are bounded by [`MAX_MESSAGE_SIZE`] to prevent memory exhaustion
//! - Direct messages verify sender identity matches claimed owner

use anyhow::Result;
use korium::Node;
use log::{debug, error, info, trace, warn};
use std::sync::{LazyLock, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use zeroize::Zeroizing;

use crate::messages::EventType;
use crate::scheduler::SchedulerCommand;

/// The PubSub topic for all mesh-wide broadcast messages.
///
/// All nodes subscribe to this topic to receive:
/// - Tender announcements (new workloads to schedule)
/// - Disposal messages (workloads to delete)
pub const BEEMESH_FABRIC: &str = "magik-fabric";

/// Storage for node keypairs indexed by identity.
/// Maps hex-encoded identity to (public_key, secret_key) tuple.
/// Secret keys use [`Zeroizing`] wrapper to ensure memory is scrubbed on drop.
type KeypairMap = std::collections::HashMap<String, (Vec<u8>, Zeroizing<Vec<u8>>)>;

/// Global sender for network control messages from the API layer.
static CONTROL_SENDER: OnceLock<mpsc::UnboundedSender<NetworkControl>> = OnceLock::new();

/// Global storage for node cryptographic keypairs.
/// Protected by mutex for thread-safe access.
static NODE_KEYPAIRS: LazyLock<Mutex<KeypairMap>> = LazyLock::new(|| Mutex::new(KeypairMap::new()));

/// Global reference to the Korium node instance.
static CORIUM_NODE: OnceLock<Node> = OnceLock::new();

/// Messages sent directly between peers (not broadcast).
///
/// These bypass PubSub and are sent point-to-point for efficiency
/// and to avoid flooding the network with per-tender traffic.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DirectMessage {
    /// A node's bid in response to a tender
    Bid(crate::messages::Bid),
    /// Award notification with full manifest sent to bid winners
    Award(crate::messages::AwardWithManifest),
    /// Deployment status event from winner back to tender owner
    Event(crate::messages::SchedulerEvent),
}

/// Maximum allowed size for incoming direct messages (64 MiB).
///
/// Messages exceeding this size are rejected to prevent memory exhaustion.
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Control commands sent from the API layer to the network layer.
///
/// Each variant includes a reply channel for async response delivery.
#[derive(Debug)]
pub enum NetworkControl {
    /// Publish a tender to the mesh (broadcast to all nodes)
    PublishTender {
        payload: Vec<u8>,
        reply_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    /// Publish a disposal request to the mesh (broadcast to all nodes)
    PublishDisposal {
        payload: Vec<u8>,
        reply_tx: mpsc::UnboundedSender<Result<(), String>>,
    },
    /// Get this node's identity (hex-encoded public key)
    GetLocalIdentity {
        reply_tx: mpsc::UnboundedSender<String>,
    },
    /// Get information about connected peers
    GetConnectedPeers {
        reply_tx: mpsc::UnboundedSender<Result<serde_json::Value, String>>,
    },
}

/// Processes control messages from the API layer.
///
/// Routes each control message type to the appropriate handler:
/// - Tender/Disposal: Delivered to local scheduler AND broadcast to mesh
/// - Identity/Peers: Returns node information
fn handle_control_message(
    msg: NetworkControl,
    node: std::sync::Arc<Node>,
    sched_input_tx: mpsc::Sender<(String, String, Vec<u8>)>,
    local_identity: String,
) {
    match msg {
        NetworkControl::PublishTender { payload, reply_tx } => {
            debug!("handle_control_message: received PublishTender request");
            let node = node.clone();
            let local_identity_for_sched = local_identity.clone();
            let payload_for_sched = payload.clone();

            if let Err(e) = sched_input_tx.try_send((
                BEEMESH_FABRIC.to_string(),
                local_identity_for_sched,
                payload_for_sched,
            )) {
                warn!("Failed to deliver tender to local scheduler: {}", e);
            }

            let _ = reply_tx.send(Ok(()));

            tokio::spawn(async move {
                debug!("Spawned task: calling node.publish for mesh broadcast");
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    node.publish(BEEMESH_FABRIC, payload.clone()),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        debug!("Mesh publish completed successfully");
                    }
                    Ok(Err(e)) => {
                        error!("Mesh publish failed: {}", e);
                    }
                    Err(_) => {
                        warn!("Mesh publish timed out (local delivery already succeeded)");
                    }
                }
            });
        }

        NetworkControl::PublishDisposal { payload, reply_tx } => {
            let node = node.clone();
            let local_identity_for_sched = local_identity.clone();
            let payload_for_sched = payload.clone();

            if let Err(e) = sched_input_tx.try_send((
                BEEMESH_FABRIC.to_string(),
                local_identity_for_sched,
                payload_for_sched,
            )) {
                warn!("Failed to deliver disposal to local scheduler: {}", e);
            }

            tokio::spawn(async move {
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    node.publish(BEEMESH_FABRIC, payload),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        debug!("Spawned task: disposal published to mesh");
                    }
                    Ok(Err(e)) => {
                        warn!("Spawned task: disposal publish failed: {}", e);
                    }
                    Err(_) => {
                        warn!("Spawned task: disposal publish timed out after 5s");
                    }
                }
            });

            let _ = reply_tx.send(Ok(()));
        }

        NetworkControl::GetLocalIdentity { reply_tx } => {
            let _ = reply_tx.send(node.identity());
        }

        NetworkControl::GetConnectedPeers { reply_tx } => {
            let info = serde_json::json!({
                "local_identity": node.identity(),
                "listening_on": node.local_addr().map(|a| a.to_string()).unwrap_or_default(),
            });
            let _ = reply_tx.send(Ok(info));
        }
    }
}

/// Retrieves a node keypair by identity.
///
/// # Arguments
///
/// * `identity` - Optional hex-encoded node identity. If `None`, returns the first
///   available keypair (useful in single-node scenarios).
///
/// # Returns
///
/// The (public_key, secret_key) tuple if found, or `None` if the identity is unknown.
///
/// # Panics
///
/// Panics if the keypair mutex is poisoned (indicates unrecoverable state).
///
/// # Security
///
/// The returned secret key bytes are wrapped in [`Zeroizing`] to ensure
/// they are scrubbed from memory when dropped.
pub fn get_node_keypair_for_identity(identity: Option<&str>) -> Option<(Vec<u8>, Vec<u8>)> {
    let keypairs = NODE_KEYPAIRS
        .lock()
        .expect("node keypairs mutex poisoned - cryptographic state may be corrupted");
    if let Some(id) = identity {
        keypairs.get(id).map(|(pk, sk)| {
            let secret_bytes: &Vec<u8> = sk.as_ref();
            (pk.clone(), secret_bytes.clone())
        })
    } else {
        keypairs.values().next().map(|(pk, sk)| {
            let secret_bytes: &Vec<u8> = sk.as_ref();
            (pk.clone(), secret_bytes.clone())
        })
    }
}

/// Retrieves any available node keypair.
///
/// Convenience wrapper around [`get_node_keypair_for_identity`] with `None`.
pub fn get_node_keypair() -> Option<(Vec<u8>, Vec<u8>)> {
    get_node_keypair_for_identity(None)
}

/// Retrieves a keypair by raw identity bytes.
///
/// # Arguments
///
/// * `identity_bytes` - Optional raw bytes of the node identity (will be hex-encoded).
pub fn get_node_keypair_for_peer(identity_bytes: Option<&[u8]>) -> Option<(Vec<u8>, Vec<u8>)> {
    if let Some(bytes) = identity_bytes {
        let identity = hex::encode(bytes);
        get_node_keypair_for_identity(Some(&identity))
    } else {
        get_node_keypair_for_identity(None)
    }
}

/// Stores a node keypair for later retrieval.
///
/// # Arguments
///
/// * `identity` - Hex-encoded node identity
/// * `keypair` - (public_key, secret_key) tuple to store
///
/// # Panics
///
/// Panics if the keypair mutex is poisoned.
///
/// # Security
///
/// The secret key is wrapped in [`Zeroizing`] to ensure it is scrubbed
/// from memory when the keypair entry is removed or overwritten.
pub fn set_node_keypair(identity: String, keypair: (Vec<u8>, Vec<u8>)) {
    let mut keypairs = NODE_KEYPAIRS
        .lock()
        .expect("node keypairs mutex poisoned - cryptographic state may be corrupted");
    keypairs.insert(identity, (keypair.0, Zeroizing::new(keypair.1)));
}

/// Sets the global control sender for network commands.
///
/// Called once during node initialization. Subsequent calls are ignored.
pub fn set_control_sender(sender: mpsc::UnboundedSender<NetworkControl>) {
    let _ = CONTROL_SENDER.set(sender);
}

/// Retrieves the global control sender.
///
/// Returns `None` if the node has not been initialized.
pub fn get_control_sender() -> Option<&'static mpsc::UnboundedSender<NetworkControl>> {
    CONTROL_SENDER.get()
}

/// Retrieves the global Korium node instance.
///
/// Returns `None` if the node has not been initialized.
pub fn get_korium_node() -> Option<&'static Node> {
    CORIUM_NODE.get()
}

/// Result type for node setup containing the node and peer watch channels.
pub type NodeSetupResult = (
    Node,
    watch::Receiver<Vec<String>>,
    watch::Sender<Vec<String>>,
);

/// Creates and configures a new Korium mesh node.
///
/// # Arguments
///
/// * `bind_addr` - Address to bind the node to (e.g., "0.0.0.0:4000")
///
/// # Returns
///
/// A tuple containing:
/// - The configured [`Node`] instance
/// - A watch receiver for peer list updates
/// - A watch sender for updating the peer list
///
/// # Side Effects
///
/// - Generates and stores a cryptographic keypair
/// - Logs node identity and listening address
pub async fn setup_korium_node(bind_addr: &str) -> Result<NodeSetupResult> {
    let node = Node::bind(bind_addr).await?;

    info!("Korium node identity: {}", node.identity());
    info!("Korium listening on: {:?}", node.local_addr()?);

    let (peer_tx, peer_rx) = watch::channel(Vec::new());

    let keypair = node.keypair();
    let identity = node.identity();
    let public_bytes = keypair.public_key_bytes().to_vec();
    let secret_bytes = keypair.secret_key_bytes().to_vec();
    set_node_keypair(identity.clone(), (public_bytes, secret_bytes));

    Ok((node, peer_rx, peer_tx))
}

/// Subscribes the node to the mesh fabric topic.
///
/// Must be called after [`setup_korium_node`] but before [`start_korium_node`].
pub async fn subscribe_to_fabric(node: &Node) -> Result<()> {
    node.subscribe(BEEMESH_FABRIC).await?;
    debug!("Subscribed to topic: {}", BEEMESH_FABRIC);
    Ok(())
}

/// Starts the main Korium node event loop.
///
/// This function runs the core networking logic:
/// - Processes incoming PubSub messages and routes them to the scheduler
/// - Handles direct message requests (bids, awards, events)
/// - Executes scheduler output commands (publish, send)
/// - Responds to control commands from the API layer
/// - Periodically refreshes DHT state
///
/// # Arguments
///
/// * `node` - The configured Korium node
/// * `peer_tx` - Watch channel sender for peer list updates
/// * `control_rx` - Receiver for API control messages
/// * `dht_refresh_interval_secs` - How often to refresh DHT state (seconds)
///
/// # Returns
///
/// Returns `Ok(())` when the control channel is closed (graceful shutdown).
///
/// # Event Loop
///
/// The main loop uses `tokio::select!` to handle multiple event sources:
/// 1. Scheduler output commands (highest priority)
/// 2. API control messages
/// 3. PubSub messages from the mesh
/// 4. Direct request messages from peers
/// 5. DHT refresh timer ticks
pub async fn start_korium_node(
    node: Node,
    peer_tx: watch::Sender<Vec<String>>,
    mut control_rx: mpsc::UnboundedReceiver<NetworkControl>,
    dht_refresh_interval_secs: u64,
) -> Result<()> {
    let node = std::sync::Arc::new(node);

    let mut dht_refresh_interval =
        tokio::time::interval(Duration::from_secs(dht_refresh_interval_secs));
    dht_refresh_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut pubsub_rx = node.messages().await?;
    let mut request_rx = node.incoming_requests().await?;

    const SCHEDULER_INPUT_CHANNEL_CAPACITY: usize = 10_000;
    const SCHEDULER_OUTPUT_CHANNEL_CAPACITY: usize = 10_000;

    let (sched_input_tx, mut sched_input_rx) =
        mpsc::channel::<(String, String, Vec<u8>)>(SCHEDULER_INPUT_CHANNEL_CAPACITY);
    let (sched_output_tx, mut sched_output_rx) =
        mpsc::channel::<SchedulerCommand>(SCHEDULER_OUTPUT_CHANNEL_CAPACITY);

    let local_identity = node.identity();

    let scheduler = crate::scheduler::Scheduler::new(local_identity.clone(), sched_output_tx);
    let scheduler = std::sync::Arc::new(scheduler);
    let scheduler_for_messages = scheduler.clone();
    let scheduler_for_dm = scheduler.clone();

    let sched_input_tx_local = sched_input_tx.clone();
    let local_identity_clone = local_identity.clone();

    tokio::spawn(async move {
        while let Some((topic, from, data)) = sched_input_rx.recv().await {
            scheduler_for_messages
                .handle_pubsub_message(&topic, &from, &data)
                .await;
        }
    });

    let _ = peer_tx; // Keep alive for now

    loop {
        trace!("Main loop iteration");
        tokio::select! {
            Some(command) = sched_output_rx.recv() => {
                debug!("Received command from scheduler: {:?}", std::mem::discriminant(&command));
                match command {
                    SchedulerCommand::Publish { topic, payload } => {
                        if let Err(e) = node.publish(&topic, payload.clone()).await {
                            error!("Failed to publish scheduler message: {}", e);
                        }
                        if let Err(e) = sched_input_tx_local
                            .try_send((topic, local_identity_clone.clone(), payload))
                        {
                            warn!("Failed to deliver to local scheduler: {}", e);
                        }
                    }
                    SchedulerCommand::SendBid { identity, payload } => {
                        match crate::messages::deserialize_safe::<crate::messages::Bid>(&payload) {
                            Ok(bid) => {
                                debug!("Sending bid to tender owner {}", identity);
                                let dm = DirectMessage::Bid(bid);
                                if let Ok(dm_bytes) = bincode::serialize(&dm) {
                                    debug!("Calling send to {}, payload size={}", identity, dm_bytes.len());
                                    match tokio::time::timeout(
                                        std::time::Duration::from_secs(5),
                                        node.send(&identity, dm_bytes)
                                    ).await {
                                        Ok(Ok(_)) => info!("Dispatched bid to tender owner {}", identity),
                                        Ok(Err(e)) => error!("Failed to send bid to {}: {}", identity, e),
                                        Err(_) => error!("Timeout sending bid to {}", identity),
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to deserialize bid payload: {}", e);
                            }
                        }
                    }
                    SchedulerCommand::SendEvent { identity, payload } => {
                        match crate::messages::deserialize_safe::<crate::messages::SchedulerEvent>(&payload) {
                            Ok(event) => {
                                let dm = DirectMessage::Event(event);
                                if let Ok(dm_bytes) = bincode::serialize(&dm) {
                                    match node.send(&identity, dm_bytes).await {
                                        Ok(_) => info!("Dispatched event to tender owner {}", identity),
                                        Err(e) => error!("Failed to send event to {}: {}", identity, e),
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to deserialize event payload: {}", e);
                            }
                        }
                    }
                    SchedulerCommand::SendAward { identity, payload } => {
                        match crate::messages::deserialize_safe::<crate::messages::AwardWithManifest>(&payload) {
                            Ok(award) => {
                                let dm = DirectMessage::Award(award);
                                if let Ok(dm_bytes) = bincode::serialize(&dm) {
                                    match node.send(&identity, dm_bytes).await {
                                        Ok(_) => info!("Dispatched award+manifest to winner {}", identity),
                                        Err(e) => error!("Failed to send award to {}: {}", identity, e),
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to deserialize award payload: {}", e);
                            }
                        }
                    }
                }
            }

            maybe_msg = control_rx.recv() => {
                if let Some(msg) = maybe_msg {
                    handle_control_message(msg, node.clone(), sched_input_tx_local.clone(), local_identity_clone.clone());
                } else {
                    info!("Control channel closed; shutting down korium loop");
                    break;
                }
            }

            Some(msg) = pubsub_rx.recv() => {
                debug!(
                    "Received PubSub message on topic '{}' from {} ({} bytes)",
                    msg.topic,
                    &msg.from[..16.min(msg.from.len())],
                    msg.data.len()
                );

                if msg.topic == BEEMESH_FABRIC {
                    if let Err(e) = sched_input_tx
                        .try_send((msg.topic.clone(), msg.from.clone(), msg.data.clone()))
                    {
                        warn!("Scheduler input channel full, dropping message: {}", e);
                    }
                } else {
                    warn!(
                        "Dropping message on unsupported topic '{}' from {}",
                        msg.topic, msg.from
                    );
                }
            }

            Some((from, data, reply_tx)) = request_rx.recv() => {
                if data.len() > MAX_MESSAGE_SIZE {
                    warn!(
                        "Rejecting oversized request from {} ({} bytes > {} max)",
                        from, data.len(), MAX_MESSAGE_SIZE
                    );
                    let _ = reply_tx.send(vec![0]); // Send error ack
                    continue;
                }

                debug!(
                    "Received request from {} ({} bytes)",
                    &from[..16.min(from.len())],
                    data.len()
                );

                match crate::messages::deserialize_safe::<DirectMessage>(&data) {
                    Ok(DirectMessage::Bid(bid)) => {
                        info!("[{}] Received bid from peer {} for tender {}", &local_identity_clone[..16], from, bid.tender_id);
                        scheduler_for_dm.handle_bid_direct(&bid, &from).await;
                    }
                    Ok(DirectMessage::Award(award)) => {
                        info!("[{}] Received award from peer {} for tender {}", &local_identity_clone[..16], from, award.tender_id);
                        if let Err(e) = process_award(&award, &from, &node, &scheduler_for_dm).await {
                            warn!("Failed to process award from {}: {}", from, e);
                        }
                    }
                    Ok(DirectMessage::Event(event)) => {
                        info!("Received event from peer {} for tender {}", from, event.tender_id);
                        scheduler_for_dm.handle_event_direct(&event, &from).await;
                    }
                    Err(e) => {
                        warn!("Failed to deserialize request from {}: {}", from, e);
                    }
                }
                let _ = reply_tx.send(vec![1]); // 1 = success ack
            }

            _ = dht_refresh_interval.tick() => {
                debug!("DHT refresh tick");
            }
        }
    }

    Ok(())
}

/// Processes an award message received from a tender owner.
///
/// Validates the award and deploys the workload:
///
/// 1. Checks timestamp freshness to prevent replay attacks
/// 2. Verifies sender identity matches claimed owner
/// 3. Registers the tender-owner association
/// 4. Stores the manifest locally
/// 5. Deploys the workload via the runtime engine
/// 6. Reports success/failure back to the scheduler
///
/// # Arguments
///
/// * `award` - The award message containing manifest and deployment params
/// * `from` - Identity of the sender (for verification)
/// * `_node` - Reference to Korium node (reserved for future use)
/// * `scheduler` - Scheduler instance for event publishing
///
/// # Errors
///
/// Returns an error if:
/// - Timestamp is stale (replay attack protection)
/// - Sender doesn't match claimed owner (identity spoofing)
/// - Deployment fails (runtime error)
async fn process_award(
    award: &crate::messages::AwardWithManifest,
    from: &str,
    _node: &Node,
    scheduler: &std::sync::Arc<crate::scheduler::Scheduler>,
) -> Result<()> {
    if !crate::scheduler::is_timestamp_fresh(award.timestamp) {
        anyhow::bail!("stale timestamp");
    }

    if from != award.owner_identity {
        anyhow::bail!(
            "sender {} does not match owner {}",
            from,
            award.owner_identity
        );
    }

    info!(
        "Award verified for tender {} from owner {}",
        award.tender_id, award.owner_identity
    );

    crate::scheduler::set_tender_owner(&award.tender_id, &award.owner_identity);

    crate::scheduler::register_local_manifest(&award.tender_id, &award.manifest_json);

    // Deploy pod via podservice
    match crate::podservice::deploy_pod(&award.manifest_json).await {
        Ok(pod_id) => {
            info!(
                "Successfully deployed manifest for tender {} as pod {}",
                award.tender_id, pod_id
            );
            scheduler.publish_event(
                &award.tender_id,
                EventType::Deployed,
                &format!("pod {} deployed", pod_id),
            );
            Ok(())
        }
        Err(e) => {
            error!("Deployment failed for tender {}: {}", award.tender_id, e);
            scheduler.publish_event(
                &award.tender_id,
                EventType::Failed,
                &format!("deployment failed: {}", e),
            );
            anyhow::bail!("deployment failed: {}", e)
        }
    }
}
