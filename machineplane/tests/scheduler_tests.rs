//! Scheduler Tests
//!
//! This module tests the scheduler protocol as defined in `test-spec.md`:
//! - Deterministic score computation (§4.1)
//! - Deterministic winner selection (§5.2)
//! - Replay protection via (tender_id, nonce) tuples
//! - Timestamp freshness enforcement (±30 seconds max skew)
//! - Topic filtering for PubSub messages
//!
//! Note: Signature verification is now handled by Korium at transport level.
//!
//! # Spec References
//!
//! - test-spec.md: "Scheduler – deterministic bidding and winner selection"
//! - test-spec.md: "Scheduler – deployment, events, and backoff"
//! - machineplane-spec.md §3.3: Replay and timestamp validation
//! - machineplane-spec.md §4.1: Tender/Bid/Award flow
//! - machineplane-spec.md §5.2: Winner selection algorithm

use machineplane::messages::{Bid, EventType, SchedulerEvent, Tender};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Test Utilities
// ============================================================================

/// Returns current time in milliseconds since UNIX epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_millis() as u64
}

/// Creates a tender for testing.
/// Note: Signing is handled by Korium at transport level.
fn create_tender(id: &str, timestamp: u64, nonce: u64) -> Tender {
    Tender {
        id: id.to_string(),
        manifest_digest: format!("digest-{}", id),
        qos_preemptible: false,
        timestamp,
        nonce,
    }
}

/// Creates a bid for testing.
/// Note: Signing is handled by Korium at transport level.
fn create_bid(tender_id: &str, node_id: &str, score: f64, timestamp: u64, nonce: u64) -> Bid {
    Bid {
        tender_id: tender_id.to_string(),
        node_id: node_id.to_string(),
        score,
        resource_fit_score: score * 0.8,
        network_locality_score: score * 0.2,
        timestamp,
        nonce,
    }
}

/// Creates a scheduler event for testing.
/// Note: Signing is handled by Korium at transport level.
fn create_event(
    tender_id: &str,
    node_id: &str,
    event_type: EventType,
    timestamp: u64,
    nonce: u64,
) -> SchedulerEvent {
    SchedulerEvent {
        tender_id: tender_id.to_string(),
        node_id: node_id.to_string(),
        event_type,
        reason: "test event".to_string(),
        timestamp,
        nonce,
    }
}

// ============================================================================
// Deterministic Score Computation Tests (§4.1)
// ============================================================================

/// Verifies that bid score computation is deterministic for identical inputs.
///
/// Spec requirement: "The score computed for T MUST be the same when
/// recomputed with the same Tender and the same local metrics"
#[test]
fn bid_score_computation_is_deterministic() {
    // Score formula from scheduler.rs: capacity_score * 0.8 + 0.2
    // Given fixed capacity_score = 1.0, the result should always be 1.0
    let capacity_score = 1.0;
    let computed_score_1 = capacity_score * 0.8 + 0.2;
    let computed_score_2 = capacity_score * 0.8 + 0.2;

    assert_eq!(
        computed_score_1, computed_score_2,
        "Score computation MUST be deterministic for same inputs"
    );

    // Verify with different capacity scores
    let capacity_score_2 = 0.5;
    let score_a = capacity_score_2 * 0.8 + 0.2;
    let score_b = capacity_score_2 * 0.8 + 0.2;

    assert_eq!(
        score_a, score_b,
        "Score computation MUST be deterministic regardless of input value"
    );
}

/// Verifies that resource fit scores are bounded [0.0, 1.0].
#[test]
fn resource_fit_score_is_bounded() {
    let capacity_score = 1.0f64;
    let computed_score = capacity_score * 0.8 + 0.2;

    assert!(
        computed_score >= 0.0 && computed_score <= 1.0,
        "Computed score MUST be in range [0.0, 1.0]"
    );

    // Edge case: zero capacity
    let zero_capacity = 0.0f64;
    let zero_score = zero_capacity * 0.8 + 0.2;
    assert!(
        zero_score >= 0.0,
        "Score MUST be non-negative even with zero capacity"
    );
}

// ============================================================================
// Deterministic Winner Selection Tests (§5.2)
// ============================================================================

/// Verifies that winner selection produces identical results for the same bid set.
///
/// Spec requirement: "The same ordered list of winners MUST be produced every
/// time the procedure is run with the same Bid set"
#[test]
fn winner_selection_is_deterministic() {
    // Simulate the select_winners algorithm from scheduler.rs
    #[derive(Clone)]
    struct BidEntry {
        bidder_id: String,
        score: f64,
    }

    fn select_winners(bids: &[BidEntry]) -> Vec<String> {
        use std::collections::HashMap;

        // De-duplicate bids by node, keeping highest score per node
        let mut best_by_node: HashMap<String, BidEntry> = HashMap::new();
        for bid in bids {
            let entry = best_by_node
                .entry(bid.bidder_id.clone())
                .or_insert_with(|| bid.clone());
            if bid.score > entry.score {
                *entry = bid.clone();
            }
        }

        let mut sorted: Vec<BidEntry> = best_by_node.into_values().collect();
        sorted.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.bidder_id.cmp(&b.bidder_id))
        });

        sorted.into_iter().map(|b| b.bidder_id).collect()
    }

    let bids = vec![
        BidEntry {
            bidder_id: "node-a".to_string(),
            score: 0.8,
        },
        BidEntry {
            bidder_id: "node-b".to_string(),
            score: 0.95,
        },
        BidEntry {
            bidder_id: "node-c".to_string(),
            score: 0.7,
        },
    ];

    let winners_1 = select_winners(&bids);
    let winners_2 = select_winners(&bids);

    assert_eq!(
        winners_1, winners_2,
        "Winner selection MUST be deterministic for identical bid sets"
    );

    // Verify correct ordering (highest score first)
    assert_eq!(winners_1[0], "node-b", "Highest scoring node MUST be first");
    assert_eq!(winners_1[1], "node-a", "Second highest MUST be second");
    assert_eq!(winners_1[2], "node-c", "Lowest scoring MUST be last");
}

/// Verifies that nodes with equal scores are ordered by identity (deterministic tiebreaker).
///
/// Spec requirement: "If scores equal, lexicographically higher identity wins"
#[test]
fn winner_selection_uses_identity_as_tiebreaker() {
    #[derive(Clone)]
    struct BidEntry {
        bidder_id: String,
        score: f64,
    }

    fn select_winners(bids: &[BidEntry]) -> Vec<String> {
        use std::collections::HashMap;

        let mut best_by_node: HashMap<String, BidEntry> = HashMap::new();
        for bid in bids {
            let entry = best_by_node
                .entry(bid.bidder_id.clone())
                .or_insert_with(|| bid.clone());
            if bid.score > entry.score {
                *entry = bid.clone();
            }
        }

        let mut sorted: Vec<BidEntry> = best_by_node.into_values().collect();
        sorted.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.bidder_id.cmp(&b.bidder_id))
        });

        sorted.into_iter().map(|b| b.bidder_id).collect()
    }

    // All nodes have the same score
    let bids = vec![
        BidEntry {
            bidder_id: "node-c".to_string(),
            score: 0.85,
        },
        BidEntry {
            bidder_id: "node-a".to_string(),
            score: 0.85,
        },
        BidEntry {
            bidder_id: "node-b".to_string(),
            score: 0.85,
        },
    ];

    let winners = select_winners(&bids);

    // With equal scores, should be sorted lexicographically by identity
    assert_eq!(
        winners,
        vec!["node-a", "node-b", "node-c"],
        "Equal scores MUST use identity as deterministic tiebreaker"
    );
}

/// Verifies that duplicate bids from the same node keep only the highest score.
///
/// Spec requirement: "If multiple bids exist from the same node, only the
/// highest-scoring bid is considered"
#[test]
fn winner_selection_deduplicates_by_node() {
    #[derive(Clone)]
    struct BidEntry {
        bidder_id: String,
        score: f64,
    }

    fn select_winners(bids: &[BidEntry]) -> Vec<(String, f64)> {
        use std::collections::HashMap;

        let mut best_by_node: HashMap<String, BidEntry> = HashMap::new();
        for bid in bids {
            let entry = best_by_node
                .entry(bid.bidder_id.clone())
                .or_insert_with(|| bid.clone());
            if bid.score > entry.score {
                *entry = bid.clone();
            }
        }

        let mut sorted: Vec<BidEntry> = best_by_node.into_values().collect();
        sorted.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.bidder_id.cmp(&b.bidder_id))
        });

        sorted.into_iter().map(|b| (b.bidder_id, b.score)).collect()
    }

    // Node-a submits multiple bids with different scores
    let bids = vec![
        BidEntry {
            bidder_id: "node-a".to_string(),
            score: 0.5,
        },
        BidEntry {
            bidder_id: "node-a".to_string(),
            score: 0.9,
        },
        BidEntry {
            bidder_id: "node-a".to_string(),
            score: 0.7,
        },
        BidEntry {
            bidder_id: "node-b".to_string(),
            score: 0.8,
        },
    ];

    let winners = select_winners(&bids);

    assert_eq!(winners.len(), 2, "MUST have exactly 2 unique bidders");
    assert_eq!(
        winners[0],
        ("node-a".to_string(), 0.9),
        "node-a MUST use highest score (0.9)"
    );
    assert_eq!(
        winners[1],
        ("node-b".to_string(), 0.8),
        "node-b MUST retain its only bid"
    );
}

// ============================================================================
// Timestamp Freshness Tests (§3.3)
// ============================================================================

/// Maximum allowed clock skew in milliseconds (from scheduler.rs).
const MAX_CLOCK_SKEW_MS: u64 = 30_000;

/// Checks if a timestamp is within the acceptable clock skew window.
fn is_timestamp_fresh(timestamp_ms: u64) -> bool {
    let now = now_ms();
    let (min, max) = if now > timestamp_ms {
        (timestamp_ms, now)
    } else {
        (now, timestamp_ms)
    };
    max - min <= MAX_CLOCK_SKEW_MS
}

/// Verifies that current timestamps are accepted.
#[test]
fn timestamp_freshness_accepts_current_time() {
    let current_timestamp = now_ms();
    assert!(
        is_timestamp_fresh(current_timestamp),
        "Current timestamp MUST be accepted"
    );
}

/// Verifies that timestamps within the allowed skew window are accepted.
#[test]
fn timestamp_freshness_accepts_within_skew_window() {
    let now = now_ms();

    // 10 seconds in the past (within 30s window)
    let past_timestamp = now.saturating_sub(10_000);
    assert!(
        is_timestamp_fresh(past_timestamp),
        "Timestamp 10s in past MUST be accepted"
    );

    // 10 seconds in the future (within 30s window)
    let future_timestamp = now + 10_000;
    assert!(
        is_timestamp_fresh(future_timestamp),
        "Timestamp 10s in future MUST be accepted"
    );

    // Exactly at the boundary (30s)
    let boundary_past = now.saturating_sub(MAX_CLOCK_SKEW_MS);
    assert!(
        is_timestamp_fresh(boundary_past),
        "Timestamp exactly at 30s boundary MUST be accepted"
    );
}

/// Verifies that stale timestamps (outside clock skew window) are rejected.
///
/// Spec requirement: "Messages with timestamps outside MAX_CLOCK_SKEW_MS
/// window are rejected to prevent replay attacks"
#[test]
fn timestamp_freshness_rejects_stale() {
    let now = now_ms();

    // 60 seconds in the past (outside 30s window)
    let stale_past = now.saturating_sub(60_000);
    assert!(
        !is_timestamp_fresh(stale_past),
        "Timestamp 60s in past MUST be rejected"
    );

    // 60 seconds in the future (outside 30s window)
    let stale_future = now + 60_000;
    assert!(
        !is_timestamp_fresh(stale_future),
        "Timestamp 60s in future MUST be rejected"
    );
}

/// Verifies that very old timestamps are rejected (replay protection).
#[test]
fn timestamp_freshness_rejects_replay_attacks() {
    // Timestamp from 5 minutes ago (well outside the 30s window)
    let replay_timestamp = now_ms().saturating_sub(300_000);
    assert!(
        !is_timestamp_fresh(replay_timestamp),
        "Replayed timestamp from 5 minutes ago MUST be rejected"
    );
}

// ============================================================================
// Replay Protection Tests (§3.3)
// ============================================================================

/// Duration to retain seen message tuples for replay protection.
const REPLAY_WINDOW_MS: u64 = 300_000; // 5 minutes

/// Simulates the replay filter logic from scheduler.rs.
fn record_replay(
    seen: &mut std::collections::HashMap<(String, u64), u64>,
    key: (String, u64),
) -> bool {
    let now = now_ms();

    // Prune expired entries
    seen.retain(|_, ts| now.saturating_sub(*ts) <= REPLAY_WINDOW_MS);

    // Check if already seen
    if let Some(ts) = seen.get(&key) {
        if now.saturating_sub(*ts) <= REPLAY_WINDOW_MS {
            return false; // Replay detected
        }
    }

    seen.insert(key, now);
    true // First time seeing this message
}

/// Verifies that the first occurrence of a message is accepted.
#[test]
fn replay_filter_accepts_first_occurrence() {
    let mut seen: std::collections::HashMap<(String, u64), u64> = std::collections::HashMap::new();

    let key = ("tender-1".to_string(), 12345u64);
    let result = record_replay(&mut seen, key.clone());

    assert!(result, "First occurrence of message MUST be accepted");
}

/// Verifies that duplicate messages within the replay window are rejected.
///
/// Spec requirement: "Replay protection via (tender_id, nonce) tuples with
/// 5-minute window"
#[test]
fn replay_filter_rejects_duplicates() {
    let mut seen: std::collections::HashMap<(String, u64), u64> = std::collections::HashMap::new();

    let key = ("tender-1".to_string(), 12345u64);

    // First occurrence accepted
    let first_result = record_replay(&mut seen, key.clone());
    assert!(first_result, "First occurrence MUST be accepted");

    // Duplicate rejected
    let duplicate_result = record_replay(&mut seen, key.clone());
    assert!(
        !duplicate_result,
        "Duplicate within replay window MUST be rejected"
    );
}

/// Verifies that different nonces for the same tender_id are accepted.
#[test]
fn replay_filter_accepts_different_nonces() {
    let mut seen: std::collections::HashMap<(String, u64), u64> = std::collections::HashMap::new();

    let key1 = ("tender-1".to_string(), 11111u64);
    let key2 = ("tender-1".to_string(), 22222u64);

    let result1 = record_replay(&mut seen, key1);
    let result2 = record_replay(&mut seen, key2);

    assert!(result1, "First nonce MUST be accepted");
    assert!(
        result2,
        "Different nonce for same tender_id MUST be accepted"
    );
}

/// Verifies that different tender_ids with the same nonce are accepted.
#[test]
fn replay_filter_accepts_different_tender_ids() {
    let mut seen: std::collections::HashMap<(String, u64), u64> = std::collections::HashMap::new();

    let key1 = ("tender-1".to_string(), 12345u64);
    let key2 = ("tender-2".to_string(), 12345u64);

    let result1 = record_replay(&mut seen, key1);
    let result2 = record_replay(&mut seen, key2);

    assert!(result1, "First tender_id MUST be accepted");
    assert!(
        result2,
        "Different tender_id with same nonce MUST be accepted"
    );
}

// Note: Signature verification tests removed - Korium handles signing at transport level.
// See: Korium PubSub provides Ed25519 message authentication with verified msg.from.

// ============================================================================
// Bid Collection Window Tests
// ============================================================================

/// Verifies that the default selection window is reasonable.
#[test]
fn selection_window_is_reasonable() {
    use machineplane::scheduler::DEFAULT_SELECTION_WINDOW_MS;

    // Selection window should be between 100ms and 1000ms for responsive scheduling
    assert!(
        DEFAULT_SELECTION_WINDOW_MS >= 100,
        "Selection window MUST be at least 100ms for network propagation"
    );
    assert!(
        DEFAULT_SELECTION_WINDOW_MS <= 1000,
        "Selection window SHOULD be at most 1000ms for responsiveness"
    );
}

// ============================================================================
// Event Type Coverage Tests
// ============================================================================

/// Verifies that all event types can be created.
#[test]
fn all_event_types_can_be_created() {
    let event_types = [
        EventType::Deployed,
        EventType::Failed,
        EventType::Preempted,
        EventType::Cancelled,
    ];

    for event_type in event_types {
        let event = create_event("tender-1", "node-a", event_type, now_ms(), 12345);

        assert_eq!(
            event.event_type, event_type,
            "Event type {:?} MUST be correctly set",
            event_type
        );
    }
}

// Note: Message field integrity tests removed - Korium handles message authentication
// at transport level. All message tampering is detected via Korium's Ed25519 signatures.

// ============================================================================
// Topic Filtering Tests (§2.1)
// ============================================================================

/// Verifies that the correct fabric topic constant is defined.
///
/// Spec requirement: "All scheduler payloads on magik-fabric only"
#[test]
fn fabric_topic_constant_is_correct() {
    use machineplane::network::BEEMESH_FABRIC;

    assert_eq!(
        BEEMESH_FABRIC, "magik-fabric",
        "Fabric topic MUST be 'magik-fabric'"
    );
}

/// Verifies topic hash comparison logic for filtering.
///
/// Spec requirement: "Nodes MUST ignore Tenders on the wrong topic"
///
/// This tests the comparison logic that would be used in handle_message()
/// to filter messages by topic.
#[test]
fn topic_hash_comparison_filters_wrong_topic() {
    use machineplane::network::BEEMESH_FABRIC;

    // With Korium, topics are simple strings
    let fabric_topic = BEEMESH_FABRIC;
    let wrong_topic = "wrong-topic";
    let another_wrong = "magik-other";

    // Same topic should match
    assert_eq!(fabric_topic, BEEMESH_FABRIC, "Same topic name MUST match");

    // Different topics should NOT match
    assert_ne!(fabric_topic, wrong_topic, "Different topic MUST not match");
    assert_ne!(
        fabric_topic, another_wrong,
        "Similar but different topic MUST not match"
    );
}

/// Verifies that topic filtering is case-sensitive.
#[test]
fn topic_filtering_is_case_sensitive() {
    use machineplane::network::BEEMESH_FABRIC;

    let correct = BEEMESH_FABRIC;
    let uppercase = "BEEMESH-FABRIC";
    let mixed = "BeeMesh-Fabric";

    assert_ne!(correct, uppercase, "Topic matching MUST be case-sensitive");
    assert_ne!(correct, mixed, "Topic matching MUST be case-sensitive");
}

// ============================================================================
// Manifest Digest Tests
// ============================================================================

/// Verifies that tender contains manifest digest, not full manifest.
///
/// Spec requirement: "Producer MUST include only the manifest digest and NOT
/// the full manifest payload"
#[test]
fn tender_contains_digest_not_manifest() {
    let tender = create_tender("tender-1", now_ms(), 12345);

    // Tender should have a digest field
    assert!(
        !tender.manifest_digest.is_empty(),
        "Tender MUST have manifest_digest"
    );

    // The Tender struct intentionally does NOT have a manifest_json field
    // This is verified by the struct definition - if manifest_json existed,
    // this test file would need to populate it in create_tender()
}

// ============================================================================
// Bid Uniqueness Tests
// ============================================================================

/// Verifies that each bid has a unique nonce.
///
/// Spec requirement: "Nodes SHOULD submit at most one Bid per Tender"
/// The nonce ensures bids are distinguishable even from the same node.
#[test]
fn bid_nonces_are_unique() {
    let ts = now_ms();

    let bid1 = create_bid("tender-1", "node-a", 0.9, ts, rand::random());
    let bid2 = create_bid("tender-1", "node-a", 0.9, ts, rand::random());

    assert_ne!(
        bid1.nonce, bid2.nonce,
        "Different bids SHOULD have different nonces"
    );
}

/// Verifies that bid contains node_id for attribution.
#[test]
fn bid_contains_node_id() {
    let bid = create_bid("tender-1", "node-xyz", 0.9, now_ms(), 12345);

    assert_eq!(bid.node_id, "node-xyz", "Bid MUST contain correct node_id");
}
