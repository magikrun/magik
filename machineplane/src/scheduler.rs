//! # Decentralized Scheduler
//!
//! This module implements the tender/bid/award scheduling protocol for
//! distributing workloads across mesh nodes.
//!
//! # Protocol Overview
//!
//! 1. **Tender Phase**: A node with a deployment request broadcasts a [`Tender`]
//!    to all mesh nodes. The tender contains a manifest digest for verification.
//!
//! 2. **Bid Phase**: Nodes that can run the workload respond with [`Bid`] messages
//!    containing their capability scores. Bids are sent directly to the tender owner.
//!
//! 3. **Award Phase**: After a selection window, the owner picks winners based on
//!    scores and sends [`AwardWithManifest`] messages with the full deployment spec.
//!
//! 4. **Event Phase**: Winners deploy the workload and report status via
//!    [`SchedulerEvent`] messages back to the owner.
//!
//! # Security Features
//!
//! - **Replay Protection**: All messages include timestamps and nonces. Seen messages
//!   are tracked in bounded bloom filters to prevent replay attacks.
//! - **Clock Skew Tolerance**: Timestamps are validated with [`MAX_CLOCK_SKEW_MS`]
//!   tolerance (30 seconds) to handle network delays.
//! - **Resource Limits**: All caches and maps have bounded sizes to prevent OOM.
//!
//! # Disposal Handling
//!
//! When a workload is deleted, a [`Disposal`] message is broadcast. Nodes track
//! active disposals to avoid racing with in-flight deployments.

use crate::messages::{AwardWithManifest, Bid, Disposal, EventType, SchedulerEvent, Tender};
use crate::network::BEEMESH_FABRIC;

/// Default time window for collecting bids before winner selection (milliseconds).
///
/// Can be overridden via `BEEMESH_SELECTION_WINDOW_MS` environment variable.
pub const DEFAULT_SELECTION_WINDOW_MS: u64 = 250;

use indexmap::IndexMap;
use log::{error, info, warn};
use rand::RngCore;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::sleep;

/// Type alias for bid replay filter: maps (tender_id, node_id, nonce) -> first_seen_timestamp
type BidReplayFilter = Arc<Mutex<HashMap<(String, String, u64), u64>>>;

/// Type alias for event replay filter: maps (tender_id, node_id, nonce) -> first_seen_timestamp
type EventReplayFilter = Arc<Mutex<HashMap<(String, String, u64), u64>>>;

/// Maximum clock skew tolerance for timestamp validation (30 seconds).
///
/// Messages with timestamps outside this window from current time are rejected.
pub const MAX_CLOCK_SKEW_MS: u64 = 30_000;

/// Time window for replay detection (5 minutes).
///
/// Messages with matching signatures within this window are considered duplicates.
const REPLAY_WINDOW_MS: u64 = 300_000;

/// Maximum entries in replay filter before eviction starts.
const MAX_REPLAY_FILTER_ENTRIES: usize = 100_000;

/// Maximum cached manifests (for tender owners awaiting bid collection).
const MAX_MANIFEST_CACHE_ENTRIES: usize = 1_000;

/// Maximum tracked tender-owner associations.
const MAX_TENDER_OWNER_ENTRIES: usize = 10_000;

/// Maximum tracked active disposals.
const MAX_DISPOSAL_ENTRIES: usize = 10_000;

/// Cache of manifests indexed by tender ID.
/// Used by tender owners to send full manifests to award winners.
/// Uses [`IndexMap`] to maintain insertion order for proper FIFO eviction.
static LOCAL_MANIFEST_CACHE: LazyLock<Mutex<IndexMap<String, String>>> =
    LazyLock::new(|| Mutex::new(IndexMap::new()));

/// Map of tender IDs to their owner identities.
/// Used to route events back to the correct tender owner.
/// Uses [`IndexMap`] to maintain insertion order for proper FIFO eviction.
static TENDER_OWNERS: LazyLock<Mutex<IndexMap<String, String>>> =
    LazyLock::new(|| Mutex::new(IndexMap::new()));

/// Associates a tender with its owner identity.
///
/// # Arguments
///
/// * `tender_id` - Unique tender identifier
/// * `owner_identity` - Hex-encoded identity of the tender owner
///
/// # Eviction
///
/// If the cache is full, the oldest entry is evicted.
pub fn set_tender_owner(tender_id: &str, owner_identity: &str) {
    let mut owners = TENDER_OWNERS
        .lock()
        .expect("tender owners mutex poisoned - scheduler state corrupted");

    // FIFO eviction: IndexMap maintains insertion order, so shift_remove(0)
    // always removes the oldest entry.
    while owners.len() >= MAX_TENDER_OWNER_ENTRIES {
        if let Some((oldest_key, _)) = owners.shift_remove_index(0) {
            info!("Evicted oldest tender owner entry: {}", oldest_key);
        } else {
            break;
        }
    }

    owners.insert(tender_id.to_string(), owner_identity.to_string());
}

/// Retrieves the owner identity for a tender.
///
/// Returns `None` if the tender is unknown.
pub fn get_tender_owner(tender_id: &str) -> Option<String> {
    let owners = TENDER_OWNERS
        .lock()
        .expect("tender owners mutex poisoned - scheduler state corrupted");
    owners.get(tender_id).cloned()
}

/// How long disposal records are kept before expiring (5 minutes).
const DISPOSAL_TTL: Duration = Duration::from_secs(300);

/// Set of resources currently being disposed.
/// Maps resource_key to expiration instant.
static DISPOSAL_SET: LazyLock<tokio::sync::RwLock<HashMap<String, std::time::Instant>>> =
    LazyLock::new(|| tokio::sync::RwLock::new(HashMap::new()));

/// Checks if a resource is currently being disposed.
///
/// Used to prevent racing between deployments and deletions.
///
/// # Arguments
///
/// * `resource_key` - Resource identifier in format "namespace/kind/name"
pub async fn is_disposing(resource_key: &str) -> bool {
    let set = DISPOSAL_SET.read().await;
    if let Some(expires_at) = set.get(resource_key) {
        if std::time::Instant::now() < *expires_at {
            return true;
        }
    }
    false
}

/// Marks a resource as being disposed.
///
/// The entry expires after [`DISPOSAL_TTL`]. Expired entries are cleaned up
/// on each insertion.
///
/// # Arguments
///
/// * `resource_key` - Resource identifier in format "namespace/kind/name"
pub async fn mark_disposal(resource_key: &str) {
    let mut set = DISPOSAL_SET.write().await;
    let now = std::time::Instant::now();
    let expires_at = now + DISPOSAL_TTL;

    set.retain(|_, exp| *exp > now);

    while set.len() >= MAX_DISPOSAL_ENTRIES {
        if let Some(oldest_key) = set
            .iter()
            .min_by_key(|(_, exp)| *exp)
            .map(|(k, _)| k.clone())
        {
            set.remove(&oldest_key);
        } else {
            break;
        }
    }

    set.insert(resource_key.to_string(), expires_at);
    info!(
        "mark_disposal: resource_key={} expires_in={:?} (cache_size={})",
        resource_key,
        DISPOSAL_TTL,
        set.len()
    );
}

/// Processes a disposal request from the mesh.
///
/// # Actions
///
/// 1. Validates timestamp freshness
/// 2. Marks the resource as disposing
/// 3. Deletes matching pods via the runtime engine
///
/// # Arguments
///
/// * `disposal` - The disposal message with resource coordinates
/// * `source_peer_identity` - Identity of the sender (for logging)
///
/// # Errors
///
/// Returns error if timestamp validation fails.
pub async fn handle_disposal(
    disposal: &Disposal,
    source_peer_identity: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let resource_key = format!("{}/{}/{}", disposal.namespace, disposal.kind, disposal.name);
    info!(
        "handle_disposal: resource={} from={}",
        resource_key, source_peer_identity
    );

    if !is_timestamp_fresh(disposal.timestamp) {
        log::warn!(
            "handle_disposal: rejecting disposal for resource={} - stale timestamp",
            resource_key
        );
        return Err("Stale timestamp".into());
    }

    mark_disposal(&resource_key).await;

    match crate::runtime::delete_pods_by_resource(
        &disposal.namespace,
        &disposal.kind,
        &disposal.name,
    )
    .await
    {
        Ok(deleted) => {
            info!(
                "handle_disposal: deleted {} pods for resource={}",
                deleted.len(),
                resource_key
            );
        }
        Err(e) => {
            log::warn!(
                "handle_disposal: failed to delete pods for resource={}: {}",
                resource_key,
                e
            );
        }
    }

    Ok(())
}

/// The main scheduler state machine.
///
/// Each node runs one scheduler instance that:
/// - Tracks tenders this node owns (initiated deployment requests)
/// - Collects bids for owned tenders
/// - Selects winners and sends awards after the selection window
/// - Handles incoming bids, awards, and events
/// - Maintains replay protection filters
///
/// # Thread Safety
///
/// All internal state is protected by mutexes. The scheduler can be shared
/// across tasks via `Arc<Scheduler>`.
pub struct Scheduler {
    /// This node's identity (hex-encoded public key)
    local_node_id: String,

    /// Tenders initiated by this node, awaiting bid collection
    owned_tenders: Arc<Mutex<HashMap<String, OwnedTenderContext>>>,

    /// Seen tender signatures for replay protection
    seen_tenders: Arc<Mutex<HashMap<(String, u64), u64>>>,

    /// Seen bid signatures for replay protection
    seen_bids: BidReplayFilter,

    /// Seen event signatures for replay protection
    seen_events: EventReplayFilter,

    /// Channel for sending commands to the network layer
    outbound_tx: mpsc::Sender<SchedulerCommand>,
}

/// Context for collecting bids during the selection window.
struct BidContext {
    bids: Vec<BidEntry>,
}

/// A single bid entry with bidder identity and score.
#[derive(Clone)]
struct BidEntry {
    bidder_id: String,
    score: f64,
}

/// Full context for a tender owned by this node.
struct OwnedTenderContext {
    /// Collected bids during selection window
    bid_context: BidContext,
    /// Cached manifest to send to winners
    manifest_json: Option<String>,
}

/// Commands sent from the scheduler to the network layer.
#[derive(Debug, Clone)]
pub enum SchedulerCommand {
    /// Broadcast a message to the mesh fabric topic
    Publish { topic: String, payload: Vec<u8> },

    /// Send a bid directly to a specific peer
    SendBid { identity: String, payload: Vec<u8> },

    /// Send an event directly to a specific peer
    SendEvent { identity: String, payload: Vec<u8> },

    /// Send an award with manifest to a bid winner
    SendAward { identity: String, payload: Vec<u8> },
}

impl Scheduler {
    /// Creates a new scheduler instance.
    ///
    /// # Arguments
    ///
    /// * `local_node_id` - This node's hex-encoded identity
    /// * `outbound_tx` - Channel for sending commands to the network layer
    pub fn new(local_node_id: String, outbound_tx: mpsc::Sender<SchedulerCommand>) -> Self {
        Self {
            local_node_id,
            owned_tenders: Arc::new(Mutex::new(HashMap::new())),
            seen_tenders: Arc::new(Mutex::new(HashMap::new())),
            seen_bids: Arc::new(Mutex::new(HashMap::new())),
            seen_events: Arc::new(Mutex::new(HashMap::new())),
            outbound_tx,
        }
    }

    /// Routes incoming PubSub messages to the appropriate handler.
    ///
    /// Messages on the [`BEEMESH_FABRIC`] topic are parsed as either:
    /// - [`Tender`]: New workload announcement
    /// - [`Disposal`]: Workload deletion request
    pub async fn handle_pubsub_message(&self, topic: &str, from: &str, data: &[u8]) {
        if topic != BEEMESH_FABRIC {
            return;
        }

        if let Ok(tender) = crate::messages::deserialize_safe::<Tender>(data) {
            self.handle_tender(&tender, from).await;
            return;
        }

        if let Ok(disposal) = crate::messages::deserialize_safe::<Disposal>(data) {
            if let Err(e) = handle_disposal(&disposal, from).await {
                error!("Failed to handle disposal: {}", e);
            }
            return;
        }

        error!("Failed to parse PubSub message as Tender or Disposal");
    }

    /// Handles a bid received via direct message.
    pub async fn handle_bid_direct(&self, bid: &Bid, source_peer: &str) {
        self.handle_bid(bid, source_peer).await;
    }

    /// Handles an event received via direct message.
    pub async fn handle_event_direct(&self, event: &SchedulerEvent, source_peer: &str) {
        self.handle_event(event, source_peer).await;
    }

    /// Processes an incoming tender announcement.
    ///
    /// For tenders from other nodes: computes a fitness score and submits a bid.
    /// For tenders from this node: initializes bid collection for winner selection.
    async fn handle_tender(&self, tender: &Tender, source_identity: &str) {
        let tender_id = tender.id.clone();
        info!("Received Tender: {}", tender_id);
        println!("tender:received id={}", tender_id);

        if !is_timestamp_fresh(tender.timestamp) {
            error!(
                "Discarding tender {}: timestamp outside allowed skew",
                tender_id
            );
            eprintln!("tender:error id={} reason=stale_timestamp", tender_id);
            return;
        }

        if !self.mark_tender_seen(&tender_id, tender.nonce) {
            info!("Ignoring replayed tender {}", tender_id);
            eprintln!("tender:replay id={}", tender_id);
            return;
        }

        let is_owner = source_identity == self.local_node_id;
        let manifest_json = if is_owner {
            get_local_manifest(&tender_id)
        } else {
            None
        };

        if is_owner {
            if manifest_json.is_none() {
                error!(
                    "Local node issued tender {} without cached manifest; refusing to proceed",
                    tender_id
                );
                eprintln!("tender:error id={} reason=missing_manifest", tender_id);
                return;
            }

            self.initialize_owned_tender(&tender_id, manifest_json.clone());
            info!(
                "Local node issued tender {}; skipping bid as owner",
                tender_id
            );
            println!("tender:owned id={} action=skip_bid", tender_id);
            return;
        }

        //

        let capacity_score = 1.0;

        let my_score = capacity_score * 0.8 + 0.2;

        let bid = Bid {
            tender_id: tender_id.clone(),
            node_id: self.local_node_id.clone(),
            score: my_score,
            resource_fit_score: capacity_score,
            network_locality_score: 0.5,
            timestamp: now_ms(),
            nonce: rand::thread_rng().next_u64(),
        };

        let bid_bytes = match bincode::serialize(&bid) {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to serialize bid for tender {}: {}", tender_id, e);
                return;
            }
        };

        match self.outbound_tx.try_send(SchedulerCommand::SendBid {
            identity: source_identity.to_string(),
            payload: bid_bytes,
        }) {
            Ok(()) => {
                info!(
                    "Queued Bid for tender {} to owner {} with score {:.2}",
                    tender_id, source_identity, my_score
                );
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                error!(
                    "Failed to queue bid for tender {}: output channel full (backpressure)",
                    tender_id
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!(
                    "Failed to queue bid for tender {}: channel closed",
                    tender_id
                );
            }
        }
    }

    /// Initializes bid collection for a tender owned by this node.
    ///
    /// Spawns a background task that:
    /// 1. Waits for the selection window to close
    /// 2. Selects winners based on collected bids
    /// 3. Sends award messages with full manifest to winners
    fn initialize_owned_tender(&self, tender_id: &str, manifest_json: Option<String>) {
        {
            let mut owned = self
                .owned_tenders
                .lock()
                .expect("owned tenders mutex poisoned - bid collection state corrupted");
            if owned.contains_key(tender_id) {
                return; // Already initialized (duplicate tender receipt)
            }

            owned.insert(
                tender_id.to_string(),
                OwnedTenderContext {
                    bid_context: BidContext { bids: Vec::new() },
                    manifest_json,
                },
            );
        }

        let owned_tenders = self.owned_tenders.clone();
        let tender_id_clone = tender_id.to_string();
        let local_id = self.local_node_id.clone();
        let outbound_tx = self.outbound_tx.clone();

        tokio::spawn(async move {
            let selection_window_ms = std::env::var("BEEMESH_SELECTION_WINDOW_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(DEFAULT_SELECTION_WINDOW_MS);
            sleep(Duration::from_millis(selection_window_ms)).await;

            let (winners, manifest_json_opt) = {
                let mut tenders = owned_tenders
                    .lock()
                    .expect("owned tenders mutex poisoned - bid collection state corrupted");
                if let Some(ctx) = tenders.remove(&tender_id_clone) {
                    (select_winners(&ctx.bid_context), ctx.manifest_json.clone())
                } else {
                    (Vec::new(), None)
                }
            };

            if winners.is_empty() {
                info!("No qualifying bids for tender {}", tender_id_clone);
                return;
            }

            let summary = winners
                .iter()
                .map(|w| format!("{} ({:.2})", w.bidder_id, w.score))
                .collect::<Vec<_>>()
                .join(", ");

            info!(
                "Selected winners for tender {}: {}",
                tender_id_clone, summary
            );

            let winners_list: Vec<String> = winners.iter().map(|w| w.bidder_id.clone()).collect();

            if let Some(manifest_json) = manifest_json_opt
                .clone()
                .or_else(|| get_local_manifest(&tender_id_clone))
            {
                for winner in winners.iter() {
                    let award_with_manifest = AwardWithManifest {
                        tender_id: tender_id_clone.clone(),
                        manifest_json: manifest_json.clone(),
                        owner_identity: local_id.clone(),
                        replicas: 1,
                        timestamp: now_ms(),
                        nonce: rand::thread_rng().next_u64(),
                    };

                    let award_bytes =
                        bincode::serialize(&award_with_manifest).unwrap_or_else(|_| Vec::new());

                    match outbound_tx.try_send(SchedulerCommand::SendAward {
                        identity: winner.bidder_id.clone(),
                        payload: award_bytes,
                    }) {
                        Ok(()) => {
                            info!(
                                "Queued award+manifest for tender {} to winner {}",
                                tender_id_clone, winner.bidder_id
                            );
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            error!(
                                "Failed to queue award for tender {} to {}: output channel full (backpressure)",
                                tender_id_clone, winner.bidder_id
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            error!(
                                "Failed to queue award for tender {} to {}: channel closed",
                                tender_id_clone, winner.bidder_id
                            );
                        }
                    }
                }

                info!(
                    "Sent awards for tender {} to {:?}",
                    tender_id_clone, winners_list
                );
            } else {
                error!(
                    "Unable to load manifest content for tender {}; cannot distribute to winners",
                    tender_id_clone
                );
            }
        });
    }

    /// Marks a tender as seen for replay protection.
    ///
    /// Returns `true` if this is a new tender, `false` if already seen.
    fn mark_tender_seen(&self, tender_id: &str, nonce: u64) -> bool {
        let mut seen = self
            .seen_tenders
            .lock()
            .expect("seen tenders mutex poisoned - replay protection compromised");
        record_replay(&mut seen, (tender_id.to_string(), nonce))
    }

    /// Marks a bid as seen for replay protection.
    fn mark_bid_seen(&self, bid: &Bid) -> bool {
        let mut seen = self
            .seen_bids
            .lock()
            .expect("seen bids mutex poisoned - replay protection compromised");
        record_replay(
            &mut seen,
            (bid.tender_id.clone(), bid.node_id.clone(), bid.nonce),
        )
    }

    /// Marks an event as seen for replay protection.
    fn mark_event_seen(&self, event: &SchedulerEvent) -> bool {
        let mut seen = self
            .seen_events
            .lock()
            .expect("seen events mutex poisoned - replay protection compromised");
        record_replay(
            &mut seen,
            (event.tender_id.clone(), event.node_id.clone(), event.nonce),
        )
    }

    /// Processes an incoming bid for a tender this node owns.
    ///
    /// # Security
    ///
    /// Validates that the sender identity (from transport layer) matches the
    /// claimed node_id in the bid to prevent identity spoofing attacks.
    async fn handle_bid(&self, bid: &Bid, source_identity: &str) {
        // Verify sender identity matches claimed identity to prevent spoofing.
        // A malicious node could otherwise submit bids claiming to be another node.
        if bid.node_id != source_identity {
            warn!(
                "Rejecting bid: sender {} claims to be {} (identity mismatch)",
                source_identity, bid.node_id
            );
            eprintln!(
                "bid:error tender_id={} node_id={} reason=identity_mismatch sender={}",
                bid.tender_id, bid.node_id, source_identity
            );
            return;
        }

        if !is_timestamp_fresh(bid.timestamp) {
            error!(
                "Discarding bid for tender {} from {}: timestamp outside skew",
                bid.tender_id, bid.node_id
            );
            eprintln!(
                "bid:error tender_id={} node_id={} reason=stale_timestamp",
                bid.tender_id, bid.node_id
            );
            return;
        }

        if !self.mark_bid_seen(bid) {
            info!(
                "Ignoring replayed bid {} from {}",
                bid.tender_id, bid.node_id
            );
            eprintln!(
                "bid:replay tender_id={} node_id={}",
                bid.tender_id, bid.node_id
            );
            return;
        }

        let tender_id = bid.tender_id.clone();
        let bidder_id = bid.node_id.clone();
        let score = bid.score;

        if bidder_id == self.local_node_id {
            println!(
                "bid:local tender_id={} node_id={} action=ignore",
                tender_id, bidder_id
            );
            return;
        }

        let mut owned = self
            .owned_tenders
            .lock()
            .expect("owned tenders mutex poisoned - bid collection state corrupted");
        if let Some(ctx) = owned.get_mut(&tender_id) {
            info!(
                "Recorded bid for tender {}: {:.2} from {}",
                tender_id, score, bidder_id
            );
            println!(
                "bid:recorded tender_id={} node_id={} score={:.2}",
                tender_id, bidder_id, score
            );
            ctx.bid_context.bids.push(BidEntry {
                bidder_id: bidder_id.to_string(),
                score,
            });
        } else {
            info!(
                "Ignoring bid for tender {} because this node is not the owner",
                tender_id
            );
            eprintln!(
                "bid:ignored tender_id={} node_id={} reason=not_owner",
                tender_id, bidder_id
            );
        }
    }

    /// Processes an incoming scheduler event (deployment status report).
    ///
    /// # Security
    ///
    /// Validates that the sender identity (from transport layer) matches the
    /// claimed node_id in the event to prevent identity spoofing attacks.
    async fn handle_event(&self, event: &SchedulerEvent, source_identity: &str) {
        let tender_id = event.tender_id.clone();
        info!(
            "Received Scheduler Event for tender {}: {:?}",
            tender_id, event.event_type
        );

        // Verify sender identity matches claimed identity to prevent spoofing.
        // A malicious node could otherwise submit events claiming to be another node.
        if event.node_id != source_identity {
            warn!(
                "Rejecting event: sender {} claims to be {} (identity mismatch)",
                source_identity, event.node_id
            );
            eprintln!(
                "event:error tender_id={} node_id={} reason=identity_mismatch sender={}",
                tender_id, event.node_id, source_identity
            );
            return;
        }

        if !is_timestamp_fresh(event.timestamp) {
            error!(
                "Discarding scheduler event for tender {}: timestamp outside skew",
                tender_id
            );
            eprintln!("event:error tender_id={} reason=stale_timestamp", tender_id);
            return;
        }

        if !self.mark_event_seen(event) {
            info!(
                "Ignoring replayed scheduler event for tender {} from {}",
                tender_id, event.node_id
            );
            eprintln!(
                "event:replay tender_id={} node_id={} nonce={}",
                tender_id, event.node_id, event.nonce
            );
            return;
        }

        if event.node_id == self.local_node_id
            && matches!(
                event.event_type,
                EventType::Cancelled | EventType::Preempted | EventType::Failed
            )
        {
            info!(
                "Received termination event for locally deployed tender {}",
                tender_id
            );
        }
    }

    /// Publishes a scheduler event to the tender owner.
    ///
    /// Called after deployment attempts to report success/failure.
    ///
    /// # Arguments
    ///
    /// * `tender_id` - ID of the tender this event relates to
    /// * `event_type` - Type of event (Deployed, Failed, etc.)
    /// * `reason` - Human-readable explanation
    pub fn publish_event(&self, tender_id: &str, event_type: EventType, reason: &str) {
        let owner_identity = match get_tender_owner(tender_id) {
            Some(identity) => identity,
            None => {
                info!("Unknown tender owner for {}; cannot send event", tender_id);
                return;
            }
        };

        let event = SchedulerEvent {
            tender_id: tender_id.to_string(),
            node_id: self.local_node_id.clone(),
            event_type,
            reason: reason.to_string(),
            timestamp: now_ms(),
            nonce: rand::thread_rng().next_u64(),
        };

        let payload = bincode::serialize(&event).unwrap_or_else(|_| Vec::new());

        match self.outbound_tx.try_send(SchedulerCommand::SendEvent {
            identity: owner_identity.clone(),
            payload,
        }) {
            Ok(()) => {
                info!(
                    "Queued {:?} event for tender {} to owner {}",
                    event.event_type, tender_id, owner_identity
                );
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                error!(
                    "Failed to queue event for tender {} to owner {}: output channel full (backpressure)",
                    tender_id, owner_identity
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!(
                    "Failed to queue event for tender {} to owner {}: channel closed",
                    tender_id, owner_identity
                );
            }
        }
    }
}

/// Outcome of bid selection for a single bidder.
#[derive(Clone)]
struct BidOutcome {
    bidder_id: String,
    score: f64,
}

/// Selects winners from collected bids.
///
/// Algorithm:
/// 1. For each bidder, keep only their highest-scoring bid
/// 2. Sort by score (descending), then by bidder ID (for determinism)
/// 3. Return all bidders in ranked order
///
/// The caller decides how many winners to actually award based on replica count.
fn select_winners(context: &BidContext) -> Vec<BidOutcome> {
    let mut best_by_node: HashMap<String, BidEntry> = HashMap::new();

    for bid in &context.bids {
        let entry = best_by_node
            .entry(bid.bidder_id.clone())
            .or_insert_with(|| bid.clone());

        if bid.score > entry.score {
            *entry = bid.clone();
        }
    }

    let mut bids: Vec<BidEntry> = best_by_node.into_values().collect();
    bids.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.bidder_id.cmp(&b.bidder_id))
    });

    bids.into_iter()
        .map(|bid| BidOutcome {
            bidder_id: bid.bidder_id,
            score: bid.score,
        })
        .collect()
}

/// Caches a manifest for distribution to award winners.
///
/// Called when creating a new tender so the manifest can be sent
/// to winners after the selection window closes.
///
/// # Eviction
///
/// If the cache is full, the oldest entry is evicted (FIFO).
/// Uses [`IndexMap`] to maintain insertion order for deterministic eviction.
pub fn register_local_manifest(tender_id: &str, manifest_json: &str) {
    let mut cache = LOCAL_MANIFEST_CACHE
        .lock()
        .expect("manifest cache mutex poisoned - manifest state corrupted");

    // FIFO eviction: IndexMap maintains insertion order, so shift_remove_index(0)
    // always removes the oldest entry.
    while cache.len() >= MAX_MANIFEST_CACHE_ENTRIES {
        if let Some((oldest_key, _)) = cache.shift_remove_index(0) {
            info!("Evicted oldest manifest cache entry: {}", oldest_key);
        } else {
            break;
        }
    }

    cache.insert(tender_id.to_string(), manifest_json.to_string());
}

/// Retrieves a cached manifest by tender ID.
fn get_local_manifest(tender_id: &str) -> Option<String> {
    LOCAL_MANIFEST_CACHE
        .lock()
        .expect("manifest cache mutex poisoned - manifest state corrupted")
        .get(tender_id)
        .cloned()
}

/// Returns current time as milliseconds since Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Records a message signature in the replay filter.
///
/// Returns `true` if this is a new message, `false` if it's a replay.
///
/// # Eviction
///
/// - Entries older than [`REPLAY_WINDOW_MS`] are cleaned up
/// - If still over capacity, the oldest entry is evicted
fn record_replay<K: Eq + Hash + Clone>(map: &mut HashMap<K, u64>, key: K) -> bool {
    let now = now_ms();

    map.retain(|_, ts| now.saturating_sub(*ts) <= REPLAY_WINDOW_MS);

    while map.len() >= MAX_REPLAY_FILTER_ENTRIES {
        if let Some(oldest_key) = map.iter().min_by_key(|(_, ts)| *ts).map(|(k, _)| k.clone()) {
            map.remove(&oldest_key);
        } else {
            break;
        }
    }

    if let Some(ts) = map.get(&key)
        && now.saturating_sub(*ts) <= REPLAY_WINDOW_MS
    {
        return false;
    }

    map.insert(key, now);
    true
}

/// Validates that a timestamp is within acceptable clock skew bounds.
///
/// Returns `true` if the timestamp is within [`MAX_CLOCK_SKEW_MS`] of current time.
/// Used to reject stale or future-dated messages (replay attack protection).
pub fn is_timestamp_fresh(timestamp_ms: u64) -> bool {
    let now = now_ms();
    let (min, max) = if now > timestamp_ms {
        (timestamp_ms, now)
    } else {
        (now, timestamp_ms)
    };

    max - min <= MAX_CLOCK_SKEW_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_top_unique_winner() {
        let context = BidContext {
            bids: vec![
                BidEntry {
                    bidder_id: "node1".to_string(),
                    score: 0.9,
                },
                BidEntry {
                    bidder_id: "node2".to_string(),
                    score: 0.8,
                },
                BidEntry {
                    bidder_id: "node2".to_string(),
                    score: 0.95,
                },
            ],
        };

        let winners = select_winners(&context);
        assert_eq!(winners.len(), 2);
        assert_eq!(winners[0].bidder_id, "node2");
        assert_eq!(winners[1].bidder_id, "node1");
    }

    #[test]
    fn selects_single_local_winner_when_highest() {
        let context = BidContext {
            bids: vec![
                BidEntry {
                    bidder_id: "local".to_string(),
                    score: 0.99,
                },
                BidEntry {
                    bidder_id: "local".to_string(),
                    score: 0.98,
                },
                BidEntry {
                    bidder_id: "remote".to_string(),
                    score: 0.5,
                },
            ],
        };

        let winners = select_winners(&context);
        assert_eq!(winners.len(), 2);
        assert_eq!(winners[0].bidder_id, "local");
    }
}
