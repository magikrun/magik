//! # Service Discovery (DHT)
//!
//! This module provides a distributed hash table (DHT) for service discovery.
//! Each workplane agent announces itself and discovers peers running the same workload.
//!
//! # Architecture
//!
//! The DHT is organized as:
//! ```text
//! workload_id → { peer_id → ServiceRecord }
//! ```
//!
//! Records expire after a configurable TTL and are refreshed via periodic heartbeats.
//!
//! # Security Features
//!
//! - **Timestamp validation**: Records with timestamps outside the clock skew window are rejected
//! - **Bounded storage**: Maximum workloads and peers per workload to prevent OOM
//! - **Version tracking**: Only newer records replace existing ones
//!
//! # Example
//!
//! ```rust,ignore
//! use workplane::discovery::{put, list, ServiceRecord};
//! use std::time::Duration;
//!
//! let record = ServiceRecord {
//!     workload_id: "default/Deployment/my-app".to_string(),
//!     peer_id: "abc123".to_string(),
//!     ready: true,
//!     healthy: true,
//!     // ...
//! };
//!
//! put(record.clone(), Duration::from_secs(15));
//! let peers = list("default", "Deployment", "my-app");
//! ```

use std::collections::{HashMap, hash_map::Entry};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// Maximum allowed clock skew for timestamp validation (30 seconds).
const MAX_CLOCK_SKEW_MS: i64 = 30_000;

/// Maximum number of distinct workloads to track.
const MAX_WORKLOADS: usize = 10_000;

/// Maximum peers per workload to prevent memory exhaustion.
const MAX_PEERS_PER_WORKLOAD: usize = 1_000;

/// A service announcement record in the DHT.
///
/// Represents a single pod instance announcing its availability.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ServiceRecord {
    /// Canonical workload identifier: `{namespace}/{kind}/{name}`
    pub workload_id: String,

    /// Kubernetes namespace
    pub namespace: String,

    /// Workload name
    pub workload_name: String,

    /// Unique peer identifier (hex-encoded)
    pub peer_id: String,

    /// Pod name (for debugging and identification)
    #[serde(default)]
    pub pod_name: Option<String>,

    /// StatefulSet ordinal index (if applicable)
    #[serde(default)]
    pub ordinal: Option<u32>,

    /// Network addresses for this peer
    #[serde(default)]
    pub addrs: Vec<String>,

    /// Capability metadata (custom key-value pairs)
    #[serde(default)]
    pub caps: serde_json::Map<String, serde_json::Value>,

    /// Monotonically increasing version for conflict resolution
    pub version: u64,

    /// Unix timestamp in milliseconds
    pub ts: i64,

    /// Random nonce for uniqueness
    pub nonce: String,

    /// Whether the pod has passed readiness checks
    pub ready: bool,

    /// Whether the pod has passed liveness checks
    pub healthy: bool,
}

/// Internal storage entry with expiration tracking.
struct RecordEntry {
    record: ServiceRecord,
    expires_at: Instant,
}

/// Global DHT storage: workload_id → { peer_id → RecordEntry }
static WDHT: LazyLock<Mutex<HashMap<String, HashMap<String, RecordEntry>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Inserts or updates a service record in the DHT.
///
/// # Arguments
///
/// * `record` - The service record to insert
/// * `ttl` - Time-to-live before the record expires
///
/// # Returns
///
/// `true` if the record was inserted/updated, `false` if rejected.
///
/// # Rejection Reasons
///
/// - Timestamp outside clock skew window
/// - DHT at capacity and no empty workloads to evict
/// - Peers-per-workload limit reached
/// - Existing record has higher version/timestamp
pub fn put(record: ServiceRecord, ttl: Duration) -> bool {
    if !is_fresh(&record) {
        return false;
    }

    let mut dht = WDHT.lock().expect("wdht lock");

    if !dht.contains_key(&record.workload_id) && dht.len() >= MAX_WORKLOADS {
        let empty_keys: Vec<_> = dht
            .iter()
            .filter(|(_, peers)| peers.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        for key in empty_keys {
            dht.remove(&key);
        }
        if dht.len() >= MAX_WORKLOADS {
            return false;
        }
    }

    let namespace_map = dht
        .entry(record.workload_id.clone())
        .or_default();

    purge_expired(namespace_map);

    if !namespace_map.contains_key(&record.peer_id) && namespace_map.len() >= MAX_PEERS_PER_WORKLOAD {
        return false;
    }

    match namespace_map.entry(record.peer_id.clone()) {
        Entry::Occupied(mut occupied) => {
            if should_replace(&occupied.get().record, &record) {
                occupied.insert(RecordEntry {
                    record,
                    expires_at: Instant::now() + ttl,
                });
                true
            } else {
                false
            }
        }
        Entry::Vacant(vacant) => {
            vacant.insert(RecordEntry {
                record,
                expires_at: Instant::now() + ttl,
            });
            true
        }
    }
}

/// Removes a peer's record from the DHT.
///
/// Cleans up the workload entry if no peers remain.
pub fn remove(workload_id: &str, peer_id: &str) {
    let mut dht = WDHT.lock().expect("wdht lock");
    if let Some(map) = dht.get_mut(workload_id) {
        map.remove(peer_id);
        if map.is_empty() {
            dht.remove(workload_id);
        }
    }
}

/// Lists all known peers for a workload.
///
/// Purges expired records before returning. Results are sorted by
/// timestamp (newest first).
///
/// # Arguments
///
/// * `namespace` - Kubernetes namespace
/// * `kind` - Workload kind
/// * `workload_name` - Workload name
pub fn list(namespace: &str, kind: &str, workload_name: &str) -> Vec<ServiceRecord> {
    let workload_id = format!("{namespace}/{kind}/{workload_name}");
    let mut dht = WDHT.lock().expect("wdht lock");
    let Some(map) = dht.get_mut(&workload_id) else {
        return Vec::new();
    };

    purge_expired(map);

    let mut records: Vec<ServiceRecord> = map.values().map(|entry| entry.record.clone()).collect();
    records.sort_by(|a, b| b.ts.cmp(&a.ts));
    records
}

/// Removes expired entries from a peer map.
fn purge_expired(map: &mut HashMap<String, RecordEntry>) {
    let now = Instant::now();
    map.retain(|_, entry| entry.expires_at > now);
}

/// Determines if an incoming record should replace an existing one.
///
/// Comparison order: version > timestamp > peer_id (for determinism).
fn should_replace(existing: &ServiceRecord, incoming: &ServiceRecord) -> bool {
    if incoming.version > existing.version {
        return true;
    }
    if incoming.version < existing.version {
        return false;
    }
    if incoming.ts > existing.ts {
        return true;
    }
    if incoming.ts < existing.ts {
        return false;
    }
    incoming.peer_id > existing.peer_id
}

/// Validates that a record's timestamp is within acceptable clock skew.
fn is_fresh(record: &ServiceRecord) -> bool {
    let now = current_millis();
    (record.ts - now).abs() <= MAX_CLOCK_SKEW_MS
}

/// Returns current time as milliseconds since Unix epoch.
fn current_millis() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_millis() as i64
}
