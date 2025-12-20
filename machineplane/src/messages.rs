//! # Protocol Messages
//!
//! This module defines the message types used in the BeeMesh scheduling protocol.
//! The protocol follows a tender/bid/award pattern for decentralized workload placement:
//!
//! 1. **Tender**: A node announces a workload to be scheduled
//! 2. **Bid**: Nodes respond with their capability to run the workload
//! 3. **Award**: The tender owner selects winners and sends deployment instructions
//! 4. **Event**: Winners report deployment status back to the owner
//!
//! All messages include timestamps and nonces for replay protection.

use serde::{Deserialize, Serialize};

/// Request to apply (deploy) a workload manifest to the mesh.
///
/// This is the internal representation after a manifest is received
/// via the Kubernetes-compatible API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplyRequest {
    /// Desired number of replicas to deploy across the mesh
    pub replicas: u32,
    /// Unique identifier for this operation (for tracking/correlation)
    pub operation_id: String,
    /// Base64-encoded or raw YAML manifest content
    pub manifest_json: String,
    /// Identity of the node that originated this request
    pub origin_peer: String,
}

/// Response to an apply request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplyResponse {
    /// Whether the operation was accepted
    pub ok: bool,
    /// Correlation ID matching the request
    pub operation_id: String,
    /// Human-readable status or error message
    pub message: String,
}

/// Request to delete a workload from the mesh.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(Default)]
pub struct DeleteRequest {
    /// Kubernetes namespace of the workload
    pub namespace: String,
    /// Resource kind (e.g., "Deployment", "Pod")
    pub kind: String,
    /// Resource name
    pub name: String,
    /// Unique operation identifier
    pub operation_id: String,
    /// Identity of the requesting node
    pub origin_peer: String,
    /// If true, skip graceful shutdown and force removal
    pub force: bool,
}

/// Response to a delete request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeleteResponse {
    pub ok: bool,
    pub operation_id: String,
    pub message: String,
    /// Full resource identifier that was deleted
    pub resource: String,
    /// List of pod IDs that were removed
    pub removed_pods: Vec<String>,
}

/// Simple health check response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Health {
    pub ok: bool,
    pub status: String,
}

/// Tender message: announces a workload for scheduling.
///
/// Published to the mesh when a node receives a new deployment request.
/// Other nodes will respond with bids if they can run the workload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(Default)]
pub struct Tender {
    /// Unique tender identifier (UUID)
    pub id: String,
    /// SHA-256 digest of the manifest for verification
    pub manifest_digest: String,
    /// If true, this workload can be preempted by higher-priority work
    pub qos_preemptible: bool,
    /// Unix timestamp in milliseconds (for freshness checks)
    pub timestamp: u64,
    /// Random nonce for replay protection
    pub nonce: u64,
}

/// Bid message: a node's offer to run a workload.
///
/// Sent directly to the tender owner in response to a Tender message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Bid {
    /// ID of the tender being responded to
    pub tender_id: String,
    /// Identity of the bidding node
    pub node_id: String,
    /// Overall fitness score (0.0-1.0, higher is better)
    pub score: f64,
    /// Score component: how well resources match requirements
    pub resource_fit_score: f64,
    /// Score component: network proximity to requester
    pub network_locality_score: f64,
    /// Timestamp for freshness validation
    pub timestamp: u64,
    /// Nonce for replay protection
    pub nonce: u64,
}

/// Response acknowledging receipt of a bid.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BidResponse {
    pub tender_id: String,
    pub accepted: bool,
    pub message: String,
}

/// Types of scheduler events that nodes can report.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum EventType {
    /// Workload was successfully deployed
    Deployed = 0,
    /// Deployment failed
    Failed = 1,
    /// Workload was preempted by higher-priority work
    Preempted = 2,
    /// Deployment was cancelled before completion
    Cancelled = 3,
}

/// Event message: status update from a deployment winner.
///
/// Sent to the tender owner after attempting deployment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchedulerEvent {
    pub tender_id: String,
    pub node_id: String,
    pub event_type: EventType,
    /// Human-readable explanation
    pub reason: String,
    pub timestamp: u64,
    pub nonce: u64,
}

/// Acknowledgment of a scheduler event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventResponse {
    pub tender_id: String,
    pub acknowledged: bool,
}

/// Disposal message: request to delete a workload across the mesh.
///
/// Published when a workload is deleted via the API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Disposal {
    pub namespace: String,
    pub kind: String,
    pub name: String,
    pub timestamp: u64,
    pub nonce: u64,
}

/// Award message: deployment instructions sent to bid winners.
///
/// Contains the full manifest and deployment parameters.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AwardWithManifest {
    pub tender_id: String,
    /// Base64-encoded manifest content
    pub manifest_json: String,
    /// Identity of the tender owner (for verification)
    pub owner_identity: String,
    /// Number of replicas this winner should deploy
    pub replicas: u32,
    pub timestamp: u64,
    pub nonce: u64,
}

/// Response acknowledging receipt of an award.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AwardResponse {
    pub tender_id: String,
    pub accepted: bool,
    pub message: String,
}

/// Response after creating a new tender.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TenderCreateResponse {
    pub ok: bool,
    pub tender_id: String,
    /// Reference to stored manifest (for large manifests)
    pub manifest_ref: String,
    /// Time window for collecting bids in milliseconds
    pub selection_window_ms: u64,
    pub message: String,
}

/// Status query response for a tender.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TenderStatusResponse {
    pub tender_id: String,
    /// Current state: "pending", "selecting", "awarded", "complete"
    pub state: String,
    /// List of peer IDs that received awards
    pub assigned_peers: Vec<String>,
}

/// Information about a candidate node for scheduling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandidateNode {
    /// Hex-encoded node identity
    pub identity: String,
    /// Hex-encoded public key for verification
    pub public_key: String,
}

/// Response listing candidate nodes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandidatesResponse {
    pub ok: bool,
    pub candidates: Vec<CandidateNode>,
}

/// Response listing connected peers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodesResponse {
    pub peers: Vec<String>,
}

/// Signature schemes supported for manifest signing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum SignatureScheme {
    /// No signature (testing only)
    None = 0,
    /// Ed25519 signatures (default)
    Ed25519 = 1,
    /// RSA-PSS signatures (future)
    RsaPss = 2,
}

/// Types of operations on manifests.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationType {
    /// Initial deployment
    Apply = 0,
    /// Update existing deployment
    Update = 1,
    /// Remove deployment
    Delete = 2,
}

/// Key-value pair for metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

/// Record of an applied manifest with cryptographic provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppliedManifest {
    /// Unique manifest instance ID
    pub id: String,

    /// Operation correlation ID
    pub operation_id: String,

    /// Identity of the originating peer
    pub origin_peer: String,

    /// Public key of the manifest owner (for verification)
    pub owner_pubkey: Vec<u8>,

    /// Signature scheme used
    pub signature_scheme: SignatureScheme,

    /// Cryptographic signature over manifest content
    pub signature: Vec<u8>,

    /// The manifest content itself
    pub manifest_json: String,

    /// Kubernetes resource kind (e.g., "Deployment", "Pod")
    pub manifest_kind: String,

    /// User-defined labels for filtering and selection
    pub labels: Vec<KeyValue>,

    /// Unix timestamp when manifest was applied
    pub timestamp: u64,

    /// Type of operation performed
    pub operation: OperationType,

    /// Time-to-live in seconds (0 = no expiration)
    pub ttl_secs: u32,

    /// SHA-256 hash of manifest content for deduplication
    pub content_hash: String,
}

impl Default for ApplyRequest {
    fn default() -> Self {
        Self {
            replicas: 1,
            operation_id: String::new(),
            manifest_json: String::new(),
            origin_peer: String::new(),
        }
    }
}



impl Default for AwardWithManifest {
    fn default() -> Self {
        Self {
            tender_id: String::new(),
            manifest_json: String::new(),
            owner_identity: String::new(),
            replicas: 1,
            timestamp: 0,
            nonce: 0,
        }
    }
}

impl Default for AppliedManifest {
    fn default() -> Self {
        Self {
            id: String::new(),
            operation_id: String::new(),
            origin_peer: String::new(),
            owner_pubkey: Vec::new(),
            signature_scheme: SignatureScheme::None,
            signature: Vec::new(),
            manifest_json: String::new(),
            manifest_kind: String::new(),
            labels: Vec::new(),
            timestamp: 0,
            operation: OperationType::Apply,
            ttl_secs: 0,
            content_hash: String::new(),
        }
    }
}

/// Maximum size for bincode deserialization to prevent memory exhaustion attacks.
///
/// Set to 16 MiB - large enough for typical manifests but prevents OOM from
/// malicious or corrupted payloads.
pub const MAX_BINCODE_SIZE: u64 = 16 * 1024 * 1024;

/// Safely deserialize bincode data with size limits.
///
/// Unlike the default bincode deserializer, this function:
/// - Enforces a maximum payload size ([`MAX_BINCODE_SIZE`])
/// - Uses fixed-integer encoding for deterministic sizes
/// - Allows trailing bytes for forward compatibility
///
/// # Arguments
///
/// * `bytes` - Raw bytes to deserialize
///
/// # Returns
///
/// The deserialized value, or a bincode error if:
/// - The payload exceeds the size limit
/// - The data is malformed
/// - Type mismatch occurs
///
/// # Security
///
/// Always use this function instead of raw bincode deserialization
/// for network-received data to prevent DoS attacks.
pub fn deserialize_safe<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
where
    T: serde::Deserialize<'a>,
{
    use bincode::Options;
    
    bincode::DefaultOptions::new()
        .with_limit(MAX_BINCODE_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize(bytes)
}
