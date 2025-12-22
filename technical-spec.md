# Magik Technical Specification

> **Version:** 0.4.0  
> **Last Updated:** December 2025  
> **Status:** Implementation Reference  
> **Security Review:** December 2025

This document provides a comprehensive technical specification for the Magik distributed workload orchestration system, derived from the codebase and inline documentation.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Machineplane](#3-machineplane)
4. [Workplane](#4-workplane)
5. [Wire Protocols](#5-wire-protocols)
6. [Security Model](#6-security-model)
7. [Data Structures](#7-data-structures)
8. [API Reference](#8-api-reference)
9. [Configuration](#9-configuration)
10. [Failure Handling](#10-failure-handling)
11. [Appendix A: Security Audit Findings](#appendix-a-security-audit-findings-december-2025)

---

## 1. Overview

### 1.1 Purpose

Magik is a decentralized, scale-out workload orchestration fabric that eliminates centralized control planes. It consists of two primary components:

| Component | Purpose | CAP Trade-off |
|-----------|---------|---------------|
| **Machineplane** | Infrastructure layer: node discovery, scheduling, deployment | A/P (Availability + Partition Tolerance) |
| **Workplane** | Workload layer: service discovery, health monitoring, self-healing | C/P (Consistency + Partition Tolerance) |

### 1.2 Design Principles

1. **Statelessness**: Machineplane persists nothing; all state is derived from:
   - **Runtime state**: Container runtime provides the source of truth for running workloads
   - **Ephemeral tracking**: `tender_tracker` holds transient operation state (not persisted)
   - **K8s API**: All responses are reconstructed from container runtime pod labels
2. **Separation of Concerns**: Machine and workload identities are cryptographically disjoint
3. **Consumer-Scoped Consistency**: Each workload manages its own consensus (Raft for stateful)
4. **Ephemeral Scheduling**: Tender→Bid→Award protocol without global state
5. **Fire-and-Forget Mutations**: CREATE/DELETE operations publish tenders and return immediately
6. **Direct Communication (v0.3)**: Only Tenders are broadcast; Bids, Awards, Events use direct request-response

### 1.3 Technology Stack

```toml
[workspace.dependencies]
korium = "0.4.6"       # P2P mesh networking (QUIC, Kademlia, Gossipsub, Request-Response)
tokio = "1"            # Async runtime
serde = "1.0"          # Serialization
bincode = "1.3"        # Binary encoding for messages
axum = "0.8"           # REST API framework
ed25519-dalek = "2"    # Ed25519 signatures (Identity = Public Key)
blake3 = "1"           # Content-addressed hashing
sha2 = "0.10"          # SHA-256 for message digests
zeroize = "1"          # Secure memory scrubbing
krun-sys = "1.10"      # libkrun FFI bindings (optional, for microVM isolation)
```

---

## 2. Architecture

### 2.1 High-Level Components

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              BEEMESH FABRIC                                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         MACHINEPLANE                                │   │
│  │                                                                     │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │  Machine A   │  │  Machine B   │  │  Machine C   │               │   │
│  │  │  (Daemon)    │◄─┤  (Daemon)    │◄─┤  (Daemon)    │               │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘               │   │
│  │         │                 │                 │                       │   │
│  │         └────────────┬────┴────────────┬────┘                       │   │
│  │                      │                 │                            │   │
│  │               ┌──────▼──────┐   ┌──────▼──────┐                     │   │
│  │               │    MDHT     │   │  Gossipsub  │                     │   │
│  │               │ (Kademlia)  │   │ (Pub/Sub)   │                     │   │
│  │               └─────────────┘   └─────────────┘                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                          WORKPLANE                                  │   │
│  │                                                                     │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │  Workload A  │  │  Workload B  │  │  Workload C  │               │   │
│  │  │  (Agent)     │◄─┤  (Agent)     │◄─┤  (Agent)     │               │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘               │   │
│  │         │                 │                 │                       │   │
│  │         └────────────┬────┴────────────┬────┘                       │   │
│  │                      │                 │                            │   │
│  │               ┌──────▼──────┐   ┌──────▼──────┐                     │   │
│  │               │    WDHT     │   │  RPC Streams│                     │   │
│  │               │ (Kademlia)  │   │ (Req/Resp)  │                     │   │
│  │               └─────────────┘   └─────────────┘                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Module Structure

```
magik/
├── machineplane/
│   ├── src/
│   │   ├── lib.rs          # Entry point, CLI config, daemon startup
│   │   ├── main.rs         # Binary entry point
│   │   ├── messages.rs     # Wire protocol message types (Tender, Bid, Award, Event, Disposal)
│   │   ├── runtime.rs      # Runtime integration: workplane embedding, deployment orchestration
│   │   ├── scheduler.rs    # Tender/Bid/Award scheduling logic, disposal handling
│   │   ├── api.rs          # REST API (Kubernetes-compatible endpoints, tender tracking)
│   │   ├── network.rs      # Korium mesh networking (DHT, Gossipsub, Request-Response)
│   │   └── runtimes/       # Runtime engine adapters
│   │       ├── mod.rs      # RuntimeEngine trait, registry, IsolationMode
│   │       ├── crun.rs     # CrunEngine - container isolation via crun
│   │       └── krun.rs     # KrunEngine - microVM isolation via libkrun (optional)
│   ├── build.rs            # Workplane binary detection and embedding
│   └── tests/              # Integration tests
│
└── workplane/
    ├── src/
    │   ├── lib.rs          # Crate root, module exports
    │   ├── main.rs         # Binary entry point
    │   ├── config.rs       # Agent configuration (CLI + env)
    │   ├── agent.rs        # Main orchestrator (Agent struct)
    │   ├── network.rs      # Korium networking (WDHT + RPC)
    │   ├── discovery.rs    # In-memory WDHT cache with TTL
    │   ├── raft.rs         # Leader election (stateful workloads)
    │   ├── selfheal.rs     # Replica management, health probing, disposal check
    │   ├── rpc.rs          # RPC request/response types
    │   └── metrics.rs      # Internal metrics macros
    └── tests/
```

---

## 3. Machineplane

### 3.1 Overview

The Machineplane daemon (`machineplane`) runs on each node and provides:

- **Node Discovery**: Kademlia DHT for peer discovery
- **Decentralized Scheduling**: Tender→Bid→Award protocol via Gossipsub
- **Workload Deployment**: Container or microVM lifecycle via runtime engines
- **Isolation Modes**: MicroVM (default, via libkrun) or Container (via crun)
- **REST API**: Kubernetes-compatible endpoints

### 3.2 Startup Sequence

```rust
pub async fn start_machineplane(cli: DaemonConfig) -> Result<Vec<JoinHandle<()>>> {
    // 1. Initialize logging
    env_logger::Builder::from_env(...).try_init();

    // 2. Configure container runtime
    let runtime = resolve_and_configure_runtime(cli.runtime_socket)?;

    // 3. Set up Korium mesh node (QUIC transport, Kademlia DHT, Gossipsub, Request-Response)
    let (node, peer_rx, peer_tx) = network::setup_korium_node(&bind_addr).await?;

    // 4. Store node keypair for message signing
    let keypair = node.keypair();
    let identity = node.identity();
    network::set_node_keypair(identity.clone(), (keypair.public_key_bytes(), keypair.secret_key_bytes()));

    // 5. Dial bootstrap peers
    for peer_spec in cli.bootstrap_peer {
        node.bootstrap(&identity, &addr).await?;
    }

    // 6. Create control channel for REST→Korium communication
    let (control_tx, control_rx) = mpsc::unbounded_channel();

    // 7. Spawn Korium event loop
    tokio::spawn(network::start_korium_node(node, peer_tx, control_rx, dht_refresh_interval));

    // 8. Initialize runtime registry (crun + krun)
    scheduler::initialize_runtime_manager().await?;

    // 9. Start REST API server
    let app = api::build_router(peer_rx, control_tx, public_bytes);
    axum::serve(listener, app).await;
}
```

### 3.3 Scheduler Module

The scheduler implements the **Tender→Bid→Award** protocol:

```
State Machine (per tender):

[Idle] ──TenderReceived──► [Bidding] ──AwardReceived──► [Deploying] ──DeployOK──► [Running]
              │                 │                              │
              └──NotEligible──► [Idle]                         └──DeployFail──► [Idle]
```

#### 3.3.1 Message Types (v0.3)

```rust
// Tender: broadcast to all nodes via gossipsub (bincode-encoded directly)
pub struct Tender {
    pub id: String,              // ULID, globally unique
    pub manifest_digest: String, // SHA256 of manifest (content stays with owner)
    pub qos_preemptible: bool,   // Can be evicted
    pub timestamp: u64,          // ms since epoch
    pub nonce: u64,              // Replay protection
    pub signature: Vec<u8>,      // Ed25519 signature
    // Note: tender owner identity comes from gossipsub message.from field
}

// Bid: sent directly to tender owner via request-response
pub struct Bid {
    pub tender_id: String,
    pub node_id: String,         // Hex-encoded node identity
    pub score: f64,              // Composite score (0.0-1.0)
    pub resource_fit_score: f64,
    pub network_locality_score: f64,
    pub timestamp: u64,
    pub nonce: u64,
    pub signature: Vec<u8>,
}

// AwardWithManifest: sent directly to each winner via request-response
// Includes manifest inline (no separate fetch required)
// Integrity is verified via signature over manifest_json
pub struct AwardWithManifest {
    pub tender_id: String,
    pub manifest_json: String,   // Full manifest content
    pub owner_identity: String,  // Hex-encoded identity for event responses
    pub owner_pubkey: Vec<u8>,    // For authorization
    pub replicas: u32,
    pub timestamp: u64,
    pub nonce: u64,
    pub signature: Vec<u8>,      // Covers manifest_json for implicit integrity
}

// SchedulerEvent: sent directly to tender owner after deployment attempt
pub struct SchedulerEvent {
    pub tender_id: String,
    pub node_id: String,
    pub event_type: EventType,   // Deployed, Failed, Preempted, Cancelled
    pub reason: String,
    pub timestamp: u64,
    pub nonce: u64,
    pub signature: Vec<u8>,
}
```

#### 3.3.2 Message Flow (v0.3)

| Message | Transport | Protocol/Topic |
|---------|-----------|----------------|
| **Tender** | Gossipsub (broadcast) | `magik-fabric` topic |
| **Bid** | Request-Response (direct) | `/magik/bid/1.0.0` |
| **Award** | Request-Response (direct) | `/magik/award/1.0.0` |
| **Event** | Request-Response (direct) | `/magik/event/1.0.0` |

#### 3.3.3 Scheduling Flow

```rust
// Tender owner workflow (v0.3)
fn initialize_owned_tender(&self, tender_id: &str, ...) {
    // 1. Store in owned_tenders map
    // 2. Spawn async task for selection window
    tokio::spawn(async move {
        // Wait for bid collection window (~250ms ± jitter)
        sleep(Duration::from_millis(DEFAULT_SELECTION_WINDOW_MS)).await;

        // Select winners deterministically
        let winners = select_winners(&bid_context);

        // Send AwardWithManifest directly to each winner via request-response
        // Award includes manifest inline - no separate fetch required
        // Local node receives award via same path as remote nodes for consistency
        for winner in winners {
            outbound_tx.send(SchedulerCommand::SendAward {
                identity: winner.clone(),
                payload: award_with_manifest_bytes,
            });
        }
    });
}
```

#### 3.3.4 Replay Protection

```rust
const MAX_CLOCK_SKEW_MS: u64 = 30_000;    // ±30 seconds
const REPLAY_WINDOW_MS: u64 = 300_000;    // 5 minutes
const DISPOSAL_TTL: Duration = Duration::from_secs(300);  // 5 minutes

// Replay filter: (message_type, timestamp, nonce) tuple
fn record_replay(&mut self, key: (String, u64, u64)) -> bool {
    // Returns true if this is a new message, false if replay
    self.replay_set.insert(key)
}

fn is_timestamp_fresh(timestamp_ms: u64) -> bool {
    let now = now_ms();
    let lower = now.saturating_sub(REPLAY_WINDOW_MS);
    let upper = now.saturating_add(MAX_CLOCK_SKEW_MS);
    timestamp_ms >= lower && timestamp_ms <= upper
}
```

### 3.4 Network Module

#### 3.4.1 Korium Node Architecture

Korium provides an integrated P2P networking layer with:
- **QUIC Transport**: Encrypted, multiplexed connections over UDP
- **Kademlia DHT**: Peer discovery and routing
- **Gossipsub**: Pub/Sub for mesh-wide broadcasts
- **Request-Response**: Direct peer-to-peer messaging

```rust
/// Network control commands from API layer to Korium event loop
pub enum NetworkControl {
    /// Publish a tender to the mesh (broadcast to all nodes)
    PublishTender { payload: Vec<u8>, reply_tx: UnboundedSender<Result<(), String>> },
    /// Publish a disposal request to the mesh (broadcast to all nodes)
    PublishDisposal { payload: Vec<u8>, reply_tx: UnboundedSender<Result<(), String>> },
    /// Get this node's identity (hex-encoded public key)
    GetLocalIdentity { reply_tx: UnboundedSender<String> },
    /// Get information about connected peers
    GetConnectedPeers { reply_tx: UnboundedSender<Result<Value, String>> },
}

/// Direct messages sent point-to-point between peers
pub enum DirectMessage {
    Bid(Bid),
    Award(AwardWithManifest),
    Event(SchedulerEvent),
}
```

#### 3.4.2 Pub/Sub Configuration

```rust
const BEEMESH_FABRIC: &str = "magik-fabric";

// Messages broadcast via Gossipsub (magik-fabric topic):
// - Tender: broadcast to initiate scheduling
// - Disposal: broadcast to trigger fabric-wide deletion

// Messages sent via Request-Response (direct):
// - Bid: bidder → tender owner
// - Award: tender owner → winner
// - Event: winner → tender owner
```

#### 3.4.3 Event Loop

```rust
pub async fn start_korium_node(
    node: Node,
    peer_tx: watch::Sender<Vec<String>>,
    mut control_rx: UnboundedReceiver<NetworkControl>,
    dht_refresh_interval_secs: u64,
) -> Result<()> {
    let node = Arc::new(node);
    let mut pubsub_rx = node.messages().await?;
    let mut request_rx = node.incoming_requests().await?;
    let mut dht_refresh_interval = interval(Duration::from_secs(dht_refresh_interval_secs));

    loop {
        tokio::select! {
            // Handle scheduler output commands (publish, send)
            Some(command) = sched_output_rx.recv() => { ... }

            // Handle API control messages
            Some(msg) = control_rx.recv() => { ... }

            // Handle PubSub messages from mesh
            Some(msg) = pubsub_rx.recv() => {
                if msg.topic == BEEMESH_FABRIC {
                    sched_input_tx.send((msg.topic, msg.from, msg.data))?;
                }
            }

            // Handle direct request messages (bids, awards, events)
            Some((from, data, reply_tx)) = request_rx.recv() => {
                if data.len() > MAX_MESSAGE_SIZE {
                    reply_tx.send(vec![0]); // Reject oversized
                    continue;
                }
                match deserialize_safe::<DirectMessage>(&data)? {
                    DirectMessage::Bid(bid) => scheduler.handle_bid_direct(&bid, &from).await,
                    DirectMessage::Award(award) => process_award(&award, &from).await?,
                    DirectMessage::Event(event) => scheduler.handle_event_direct(&event, &from).await,
                }
                reply_tx.send(vec![1]); // Success ack
            }

            // Periodic DHT refresh
            _ = dht_refresh_interval.tick() => { ... }
        }
    }
}
```

### 3.5 Runtime Adapter

```rust
/// Runtime engine trait for container lifecycle management
#[async_trait]
pub trait RuntimeEngine: Send + Sync {
    fn name(&self) -> &str;
    async fn is_available(&self) -> bool;
    async fn apply(&self, manifest_content: &[u8], config: &DeploymentConfig) -> Result<PodInfo>;
    async fn get_status(&self, pod_id: &str) -> Result<PodInfo>;
    async fn list(&self) -> Result<Vec<PodInfo>>;
    async fn delete(&self, pod_id: &str) -> Result<()>;
    async fn logs(&self, pod_id: &str, tail: Option<usize>) -> Result<String>;
}

/// Crun implementation
impl RuntimeEngine for CrunEngine {
    async fn apply(&self, manifest_content: &[u8], config: &DeploymentConfig) -> Result<PodInfo> {
        // 1. Write manifest to temp file
        // 2. Deploy via crun runtime
        // 3. Parse output for container IDs
        // 4. Return PodInfo with status
    }
}
```

#### Pod Naming Convention

Pod names are generated using **UUID v4** format to ensure uniqueness while respecting DNS hostname
limits (63 characters per RFC 1123):

```rust
/// Generate a unique pod ID using UUID v4
/// Returns a UUID v4 string (36 chars). The runtime may add `-pod` suffix internally,
/// resulting in max 40 chars total—well under the 63-char DNS hostname limit.
fn generate_pod_id() -> String {
    Uuid::new_v4().to_string()
}
```

#### Pod Labels Injected During Deployment

When deploying a manifest, Magik injects the following labels for tracking and K8s API compatibility:

| Label | Description | Value Format |
|-------|-------------|--------------|
| `magik.pod_id` | Unique pod identifier | UUID v4 (36 chars) |
| `magik.local_identity` | Identity of the node running this pod | Hex-encoded Korium identity |
| `magik.kind` | Kubernetes resource kind | "Deployment", "StatefulSet", etc. |
| `magik.name` | Kubernetes resource name | From manifest metadata.name |
| `io.kubernetes.pod.namespace` | K8s namespace (extracted from manifest) | Defaults to "default" |
| `app.kubernetes.io/name` | App name (extracted from manifest metadata.name) | From manifest |

### 3.6 Kubernetes API (Stateless)

The K8s API module (`kube.rs`) provides kubectl-compatible endpoints while maintaining Magik's
stateless architecture. **No K8s resource state is stored in memory**—all responses are derived
from the container runtime.

#### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    kubectl / K8s Client                          │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Machineplane REST API                         │
│              /apis/apps/v1/namespaces/{ns}/deployments           │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              │                               │
         GET/LIST                       POST/DELETE
              │                               │
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│   Container Runtime     │     │   Gossipsub Tender      │
│   (local pods only)     │     │   (fire-and-forget)     │
└─────────────────────────┘     └─────────────────────────┘
```

#### Workload Aggregation

Deployments and StatefulSets are reconstructed by aggregating pods with matching labels:

```rust
// Pods with these labels are grouped into workloads:
// - app.kubernetes.io/name → workload name
// - io.kubernetes.pod.namespace → namespace filtering
// - magik.io/workload-kind → "Deployment" or "StatefulSet"

fn aggregate_workloads(pods: &[PodListEntry], namespace: &str, kind: Option<&str>) -> Vec<WorkloadInfo> {
    // Group pods by (namespace, kind, app_name)
    // Count replicas and ready_replicas
    // Return K8s-formatted responses
}
```

#### Limitations

| Limitation | Reason |
|------------|--------|
| Local node only | No mesh-wide aggregation; each node shows only its own pods |
| No UPDATE/PATCH | Only CREATE and DELETE supported (republish for updates) |
| Best-effort status | Status reflects runtime state, may have propagation delay |

---

## 4. Workplane

### 4.1 Overview

The Workplane agent (`workplane`) runs as the **infra container** in each pod, replacing the standard pause container. This design:

- Eliminates the need for a separate sidecar container
- Owns the pod's network namespace from the start
- Provides mesh networking capabilities without additional overhead
- Enables direct access to workload health endpoints via localhost

**Capabilities:**

- **Service Discovery**: WDHT (Workload DHT) for replica discovery
- **Health Monitoring**: HTTP liveness/readiness probes
- **Self-Healing**: Automatic scale-up/down via machineplane API
- **Leader Election**: Raft consensus for stateful workloads
- **RPC Streams**: Inter-replica communication

### 4.1.1 Workplane Binary Embedding

The workplane agent is statically linked for Linux (musl) and embedded into the machineplane binary at compile time:

```bash
# Build workplane for musl target (static binary)
cargo build -p workplane --release --target x86_64-unknown-linux-musl

# Rebuild machineplane (embeds workplane binary)
cargo build -p machineplane --release
```

At runtime, machineplane:
1. Extracts the embedded binary
2. Creates a minimal OCI image (`localhost/magik/workplane:latest`)
3. Imports it into the container runtime
4. Uses it as the `infra_image` for all pods

### 4.2 Agent Lifecycle

```rust
impl Agent {
    pub fn new(config: Config) -> Result<Self> {
        // 1. Validate required config (private_key, workload_name, pod_name)
        // 2. Initialize Network with Korium node
        // 3. Register RPC stream handler
        // 4. For stateful workloads, create RaftManager
        // 5. Create SelfHealer for replica management
    }

    pub fn start(&mut self) {
        // 1. Start network (listening, bootstrap, heartbeat publishing)
        self.network.start();

        // 2. Start self-healer (health probes + reconciliation)
        if let Some(sh) = self.selfhealer.as_mut() {
            sh.start();
        }

        // 3. For stateful workloads, start LeaderMonitor (50ms tick)
        if self.is_stateful() {
            self.start_leader_monitor();
        }
    }
}
```

### 4.3 Service Discovery (WDHT)

#### 4.3.1 ServiceRecord

```rust
/// A service record published to the WDHT
#[derive(Serialize, Deserialize)]
pub struct ServiceRecord {
    pub workload_id: String,     // "{namespace}/{kind}/{workload_name}"
    pub namespace: String,
    pub workload_kind: String,
    pub workload_name: String,
    pub peer_id: String,         // Hex-encoded Korium identity
    pub pod_name: Option<String>,
    pub ordinal: Option<u32>,    // For StatefulSets
    pub addrs: Vec<String>,      // Network addresses
    pub caps: Map<String, Value>, // Capabilities (leader, ordinal, etc.)
    pub version: u64,            // Monotonic, for conflict resolution
    pub ts: i64,                 // Unix ms timestamp
    pub nonce: String,           // UUID for uniqueness
    pub ready: bool,             // Readiness probe result
    pub healthy: bool,           // Liveness probe result
}
```

#### 4.3.2 Conflict Resolution

```rust
/// Order: version > timestamp > peer_id (lexicographic)
fn should_replace(existing: &ServiceRecord, incoming: &ServiceRecord) -> bool {
    if incoming.version > existing.version { return true; }
    if incoming.version < existing.version { return false; }
    if incoming.ts > existing.ts { return true; }
    if incoming.ts < existing.ts { return false; }
    incoming.peer_id > existing.peer_id  // Deterministic tiebreaker
}
```

#### 4.3.3 TTL-Based Expiration

```rust
const MAX_CLOCK_SKEW_MS: i64 = 30_000;  // ±30 seconds freshness check

pub fn put(record: ServiceRecord, ttl: Duration) -> bool {
    // 1. Reject stale/future timestamps
    if !is_fresh(&record) { return false; }

    // 2. Purge expired entries
    purge_expired(namespace_map);

    // 3. Conflict resolution
    if should_replace(&existing, &record) {
        // Insert with expiration time
        entries.insert(peer_id, RecordEntry {
            record,
            expires_at: Instant::now() + ttl,
        });
    }
}
```

### 4.4 Raft Consensus (Stateful Workloads)

Simplified Raft implementation for leader election only (no log replication):

```rust
/// Election timeout range (randomized to prevent split votes)
const ELECTION_TIMEOUT_MIN_MS: u64 = 150;
const ELECTION_TIMEOUT_MAX_MS: u64 = 300;

/// Heartbeat interval for leader
const HEARTBEAT_INTERVAL_MS: u64 = 50;

pub struct RaftManager {
    node_id: String,
    workload_id: String,
    term: u64,
    state: RaftState,        // Follower | Candidate | Leader
    voted_for: Option<String>,
    votes_received: HashSet<String>,
    peers: HashSet<String>,
    leader_id: Option<String>,
    last_heartbeat: Instant,
    election_timeout: Duration,  // Random 150-300ms
    last_role: RaftRole,         // For change detection
}

impl RaftManager {
    /// Bootstrap: if alone, become leader immediately
    pub fn bootstrap_if_needed(&mut self) -> Vec<RaftAction> {
        if self.peers.is_empty() && self.state != RaftState::Leader {
            self.state = RaftState::Leader;
            self.leader_id = Some(self.node_id.clone());
            self.term = 1;
            // Return LeadershipChanged action
        }
    }

    /// Periodic tick - check timeouts, send heartbeats
    pub fn tick(&mut self) -> Vec<RaftAction> {
        match self.state {
            RaftState::Leader => {
                // Send heartbeat every 50ms
                if elapsed >= Duration::from_millis(HEARTBEAT_INTERVAL_MS) {
                    actions.push(RaftAction::SendHeartbeat(...));
                }
            }
            RaftState::Follower | RaftState::Candidate => {
                // Check election timeout (150-300ms random)
                if elapsed >= self.election_timeout {
                    self.start_election(&mut actions);
                }
            }
        }
    }

    /// Handle incoming heartbeat from leader
    pub fn handle_heartbeat(&mut self, heartbeat: Heartbeat) -> Vec<RaftAction> {
        if heartbeat.term >= self.term {
            self.term = heartbeat.term;
            self.state = RaftState::Follower;
            self.leader_id = Some(heartbeat.leader_id);
            self.last_heartbeat = Instant::now();
            self.voted_for = None;  // Clear vote for next election
        }
    }
}
```

### 4.5 Self-Healing

```rust
pub struct SelfHealer {
    network: Network,
    config: Config,
    http: Client,  // For HTTP health probes
    stop_tx: Option<watch::Sender<bool>>,
    handle: Option<JoinHandle<()>>,
}

impl SelfHealer {
    pub fn start(&mut self) {
        tokio::spawn(async move {
            let mut ticker = interval(min(health_interval, replica_interval));
            loop {
                // Health probe cycle (default 10s)
                if now.duration_since(last_health) >= health_interval {
                    update_local_health(&network, &client, &cfg).await?;
                }

                // Replica reconciliation cycle (default 30s)
                if now.duration_since(last_replica) >= replica_interval {
                    reconcile_replicas(&network, &client, &cfg).await?;
                }
            }
        });
    }
}

async fn reconcile_replicas(network: &Network, client: &Client, cfg: &Config) -> Result<()> {
    // 1. Query WDHT for all workload replicas
    let records = network.find_service_peers();

    // 2. Verify health via RPC healthz calls
    let (healthy, unhealthy) = verify_health(network, &records).await;

    // 3. Check disposal status before scaling up
    if check_disposal_status(client, cfg).await? {
        // Self-terminate with SIGTERM (exit code 143)
        self_terminate(&cfg.workload_id());
    }

    // 4. Scale up if needed
    if healthy.len() < cfg.replicas {
        let deficit = cfg.replicas - healthy.len();
        for _ in 0..deficit {
            request_new_replica(cfg, client, ordinal).await?;
        }
    }

    // 5. Scale down if needed (prioritize: duplicates > unhealthy > excess healthy)
    let removal_candidates = build_removal_list(duplicates, unhealthy, excess_healthy);
    for record in removal_candidates {
        remove_replica(cfg, client, &record).await?;
    }
}
```

### 4.6 RPC Streams

```rust
/// RPC request between workload replicas
#[derive(Serialize, Deserialize)]
pub struct RPCRequest {
    pub method: String,       // "healthz", "leader", custom
    pub body: Map<String, Value>,
    pub leader_only: bool,    // Proxy to leader if true
}

#[derive(Serialize, Deserialize)]
pub struct RPCResponse {
    pub ok: bool,
    pub error: Option<String>,
    pub body: Map<String, Value>,
}

// Built-in method handlers
async fn handle_rpc(peer: &ServiceRecord, request: RPCRequest) -> RPCResponse {
    match request.method.as_str() {
        "healthz" => RPCResponse { ok: true, ..Default::default() },
        "leader" => {
            let leader_id = raft_manager.leader_id();
            RPCResponse {
                ok: true,
                body: json!({ "leader_id": leader_id }),
            }
        }
        _ if request.leader_only && !raft_manager.is_leader() => {
            // Proxy to leader
            network.send_request(&leader_id, request).await
        }
        _ => custom_handler(peer, request).await,
    }
}
```

---

## 5. Wire Protocols

### 5.1 Transport Layer

| Protocol | Port | Description |
|----------|------|-------------|
| QUIC/UDP | Dynamic | Korium transport (encrypted, multiplexed) |
| HTTP | 3000 (default) | REST API (Kubernetes-compatible) |

### 5.2 Korium Protocols

| Message Type | Transport | Topic/Protocol | Encoding |
|--------------|-----------|----------------|----------|
| Tender | Gossipsub (broadcast) | `magik-fabric` | Bincode |
| Disposal | Gossipsub (broadcast) | `magik-fabric` | Bincode |
| Bid | Request-Response (direct) | Point-to-point | Bincode |
| Award | Request-Response (direct) | Point-to-point | Bincode |
| Event | Request-Response (direct) | Point-to-point | Bincode |
| Workplane RPC | Request-Response (direct) | Point-to-point | JSON |

#### DirectMessage Wrapper

All direct messages (Bid, Award, Event) are wrapped in a `DirectMessage` enum for unified handling:

```rust
#[derive(Serialize, Deserialize)]
pub enum DirectMessage {
    Bid(Bid),
    Award(AwardWithManifest),
    Event(SchedulerEvent),
}
```

### 5.3 Message Encoding

```rust
// All scheduler messages use bincode
fn serialize<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serialize(value).expect("serialize")
}

fn deserialize<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> bincode::Result<T> {
    bincode::deserialize(bytes)
}

// Workplane RPC uses JSON over request/response streams
async fn write_request(&mut self, io: &mut T, req: RPCRequest) -> io::Result<()> {
    let data = serde_json::to_vec(&req)?;
    io.write_all(&data).await
}
```

### 5.4 Gossipsub Topic (v0.4)

**Tender** and **Disposal** messages are broadcast via gossipsub:

```rust
const BEEMESH_FABRIC: &str = "magik/fabric";

// Tender: broadcast to initiate scheduling
// Disposal: broadcast to trigger fabric-wide deletion (fire-and-forget)
// Bid, Award, Event use direct request-response

// Message source identified from message.source (MessageAuthenticity::Signed)
```

### 5.5 Request-Response Protocols (v0.4)

Direct communication between specific peers:

```rust
// Bid: bidder → tender owner
// Protocol: /magik/bid/1.0.0
// Sent in response to receiving a Tender broadcast

// Award: tender owner → winner
// Protocol: /magik/award/1.0.0
// Contains AwardWithManifest (manifest included inline)

// Event: winner → tender owner
// Protocol: /magik/event/1.0.0
// Reports deployment success/failure (Started, Failed, HealthCheck)
```

### 5.6 Scheduler Flows

#### Deploy Flow (Tender→Bid→Award)

| Step | Message | From | To | Transport | Description |
|------|---------|------|-----|-----------|-------------|
| 1 | Tender | Requester | All nodes | Gossipsub (`magik-fabric`) | Broadcast manifest digest; owner identity from `message.from` |
| 2 | Bid | Each eligible node | Tender owner | Request-Response (`/magik/bid/1.0.0`) | Score + signature within selection window (~250ms) |
| 3 | Award | Tender owner | Each winner | Request-Response (`/magik/award/1.0.0`) | Full manifest inline (`AwardWithManifest`) |
| 4 | Event | Winner | Tender owner | Request-Response (`/magik/event/1.0.0`) | `Started` or `Failed` status |

#### Delete Flow (DISPOSAL)

| Step | Message | From | To | Transport | Description |
|------|---------|------|-----|-----------|-------------|
| 1 | Disposal | Requester | All nodes | Gossipsub (`magik/fabric`) | Fire-and-forget; namespace/kind/name + timestamp + nonce + signature |
| 2 | (local) | Each node | Runtime | — | Verify signature, add to disposal_map (5m TTL), delete matching pods |

#### State Transitions

| Current State | Event | Next State | Action |
|---------------|-------|------------|--------|
| Idle | TenderReceived | Bidding | Compute score, send Bid if eligible |
| Bidding | NotEligible | Idle | Skip tender |
| Bidding | AwardReceived | Deploying | Deploy manifest via runtime |
| Deploying | DeployOK | Running | Send `Started` event |
| Deploying | DeployFail | Idle | Send `Failed` event |
| Running | DisposalReceived | Idle | Delete pods, add to disposal_map |

---

## 6. Security Model

### 6.1 Identity Separation

| Entity | Identity Type | Key Storage | Scope |
|--------|--------------|-------------|-------|
| Machine | Machine Identity (Ed25519) | Ephemeral (per-run) | Infrastructure only |
| Workload | Workload Identity (Ed25519) | Per-replica | Workload only |

**Critical**: Machine and workload identities are cryptographically disjoint. Machine credentials cannot access workload streams, and vice versa.

### 6.2 Security Constants

| Constant | Value | Purpose | Location |
|----------|-------|---------|----------|
| `MAX_CLOCK_SKEW_MS` | 30,000 ms | Timestamp freshness validation | `scheduler.rs` |
| `REPLAY_WINDOW_MS` | 300,000 ms | Replay detection window | `scheduler.rs` |
| `MAX_REPLAY_FILTER_ENTRIES` | 100,000 | Bounded replay filter size | `scheduler.rs` |
| `MAX_MANIFEST_CACHE_ENTRIES` | 1,000 | Bounded manifest cache | `scheduler.rs` |
| `MAX_TENDER_OWNER_ENTRIES` | 10,000 | Bounded tender-owner map | `scheduler.rs` |
| `MAX_DISPOSAL_ENTRIES` | 10,000 | Bounded disposal tracking | `scheduler.rs` |
| `DISPOSAL_TTL` | 300s (5 min) | Disposal record expiration | `scheduler.rs` |
| `MAX_MESSAGE_SIZE` | 64 MiB | Network message size limit | `network.rs` |
| `MAX_BINCODE_SIZE` | 16 MiB | Bincode deserialization limit | `messages.rs` |
| `MAX_MANIFEST_SIZE` | 1 MiB | YAML manifest size limit | `runtime.rs` |
| `MAX_WORKLOADS` | 10,000 | WDHT workload limit | `discovery.rs` |
| `MAX_PEERS_PER_WORKLOAD` | 1,000 | WDHT peers per workload limit | `discovery.rs` |
| `MAX_RPC_MESSAGE_SIZE` | 16 MiB | Workplane RPC limit | `network.rs` |
| `RPC_TIMEOUT_SECS` | 30s | RPC request timeout | `network.rs` |
| `MAX_PENDING_REQUESTS` | 10,000 | Bounded RPC queue | `network.rs` |

### 6.3 Message Authentication

All scheduler messages use Ed25519 signing with SHA-256 digest:

```rust
// Signature scheme:
// 1. Create "view" struct (all fields except signature)
// 2. Serialize view using deterministic bincode (little-endian, fixint)
// 3. Compute SHA-256 hash of serialized view
// 4. Sign 32-byte digest with Ed25519 private key

fn serialize_view<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .serialize(value)?)
}

pub fn sign_tender(tender: &mut Tender, keypair: &Keypair) -> Result<()> {
    let view = TenderView { /* all fields except signature */ };
    let view_bytes = serialize_view(&view)?;
    let digest: [u8; 32] = Sha256::digest(&view_bytes).into();
    tender.signature = keypair.sign(&digest)?;
    Ok(())
}

pub fn verify_sign_tender(tender: &Tender, public_key: &PublicKey) -> bool {
    let view = TenderView { /* all fields except signature */ };
    let view_bytes = match serialize_view(&view) { Ok(b) => b, Err(_) => return false };
    let digest: [u8; 32] = Sha256::digest(&view_bytes).into();
    public_key.verify(&digest, &tender.signature)
}
```

**Signed Message Types:**
- Tender, Bid, AwardWithManifest, SchedulerEvent
- Disposal
- ApplyRequest, ApplyResponse

### 6.4 Replay Protection

```rust
// Two-tier protection:
// 1. Timestamp freshness (±30s window via MAX_CLOCK_SKEW_MS)
// 2. (tender_id, nonce) tuple deduplication (5-minute window via REPLAY_WINDOW_MS)

// Replay filters use IndexMap for O(1) FIFO eviction when at capacity
fn record_replay<K: Eq + Hash + Clone>(map: &mut HashMap<K, u64>, key: K) -> bool {
    // 1. Expire entries older than REPLAY_WINDOW_MS
    map.retain(|_, ts| now.saturating_sub(*ts) <= REPLAY_WINDOW_MS);
    
    // 2. FIFO eviction if at MAX_REPLAY_FILTER_ENTRIES
    while map.len() >= MAX_REPLAY_FILTER_ENTRIES {
        // Remove oldest entry
    }
    
    // 3. Check if already seen within window
    if map.contains(&key) { return false; }
    
    map.insert(key, now);
    true
}

fn is_timestamp_fresh(timestamp_ms: u64) -> bool {
    let now = now_ms();
    let (min, max) = if now > timestamp_ms {
        (timestamp_ms, now)
    } else {
        (now, timestamp_ms)
    };
    max - min <= MAX_CLOCK_SKEW_MS
}
```

### 6.5 Identity Verification

Bid and Event messages undergo sender identity verification:

```rust
// Verify sender identity matches claimed identity to prevent spoofing
async fn handle_bid(&self, bid: &Bid, source_identity: &str) {
    if bid.node_id != source_identity {
        warn!("Rejecting bid: sender {} claims to be {} (identity mismatch)",
              source_identity, bid.node_id);
        return;
    }
    // ... process bid
}

async fn handle_event(&self, event: &SchedulerEvent, source_identity: &str) {
    if event.node_id != source_identity {
        warn!("Rejecting event: sender {} claims to be {} (identity mismatch)",
              source_identity, event.node_id);
        return;
    }
    // ... process event
}
```

### 6.6 Safe Deserialization

All network-received bincode data uses size-limited deserialization:

```rust
pub const MAX_BINCODE_SIZE: u64 = 16 * 1024 * 1024;  // 16 MiB

pub fn deserialize_safe<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
where
    T: serde::Deserialize<'a>,
{
    bincode::DefaultOptions::new()
        .with_limit(MAX_BINCODE_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize(bytes)
}
```

### 6.7 Secret Key Handling

Ed25519 secret keys are wrapped in `Zeroizing<Vec<u8>>` to ensure memory scrubbing on drop:

```rust
type KeypairMap = HashMap<String, (Vec<u8>, Zeroizing<Vec<u8>>)>;

static NODE_KEYPAIRS: LazyLock<Mutex<KeypairMap>> = LazyLock::new(|| Mutex::new(KeypairMap::new()));

pub fn set_node_keypair(identity: String, keypair: (Vec<u8>, Vec<u8>)) {
    let mut keypairs = NODE_KEYPAIRS.lock().expect("keypairs mutex");
    keypairs.insert(identity, (keypair.0, Zeroizing::new(keypair.1)));
}
```

### 6.8 Cross-Workload Isolation (Raft)

Raft vote requests and heartbeats are validated to prevent cross-workload confusion:

```rust
pub fn handle_vote_request(&mut self, request: VoteRequest) -> VoteResponse {
    // Reject vote requests for different workloads
    if request.workload_id != self.workload_id {
        return VoteResponse {
            term: self.term,
            vote_granted: false,
            voter_id: self.node_id.clone(),
            workload_id: self.workload_id.clone(),
        };
    }
    // ... process vote
}
```

### 6.9 Bounded Data Structures

All caches and maps use bounded sizes with FIFO eviction to prevent OOM:

| Data Structure | Location | Max Size | Eviction Strategy |
|----------------|----------|----------|-------------------|
| `LOCAL_MANIFEST_CACHE` | `scheduler.rs` | 1,000 | FIFO (IndexMap) |
| `TENDER_OWNERS` | `scheduler.rs` | 10,000 | FIFO (IndexMap) |
| `DISPOSAL_SET` | `scheduler.rs` | 10,000 | TTL + oldest-first |
| `seen_tenders` | `Scheduler` | 100,000 | TTL + oldest-first |
| `seen_bids` | `Scheduler` | 100,000 | TTL + oldest-first |
| `seen_events` | `Scheduler` | 100,000 | TTL + oldest-first |
| `WDHT` | `discovery.rs` | 10,000 workloads × 1,000 peers | TTL + FIFO |

### 6.10 Transport Encryption

All Korium connections use **QUIC with TLS 1.3** for:
- Mutual authentication (both peers verify each other via Ed25519 identities)
- Encryption (all data encrypted in transit)
- Perfect forward secrecy
- Multiplexed streams over single connection

### 6.11 Known Limitations and Mitigations

| Risk | Current Status | Mitigation |
|------|----------------|------------|
| **Self-reported bid scores** | Scores are not verified | Future: historical performance tracking |
| **Clock skew exploitation** | ±30s tolerance | Future: use vector clocks or HLC |
| **Tender storm amplification** | Leader-only reconciliation recommended | Rate limiting in self-heal loop |
| **REST API TLS** | Not built-in | Deploy behind TLS-terminating proxy |
| **Runtime socket injection** | Env var controlled | Validate socket paths against allowed prefixes |

---

## 7. Data Structures

### 7.1 Tender (v0.3)

```rust
pub struct Tender {
    pub id: String,                   // ULID (unique, sortable)
    pub manifest_digest: String,      // SHA256 hash (base64)
    pub qos_preemptible: bool,        // Can be evicted
    pub timestamp: u64,               // ms since epoch
    pub nonce: u64,                   // Random for replay protection
    pub signature: Vec<u8>,           // Ed25519 signature
    // Note: tender owner identity comes from gossipsub message.from field
}
```

### 7.2 Bid

```rust
pub struct Bid {
    pub tender_id: String,
    pub node_id: String,              // Hex-encoded node identity
    pub score: f64,                   // Composite score (0.0-1.0)
    pub resource_fit_score: f64,
    pub network_locality_score: f64,
    pub timestamp: u64,
    pub nonce: u64,
    pub signature: Vec<u8>,
}
```

### 7.3 AwardWithManifest (v0.3)

```rust
// Integrity verified via signature over manifest_json
pub struct AwardWithManifest {
    pub tender_id: String,
    pub manifest_json: String,        // Full manifest content (inline)
    pub owner_identity: String,       // Hex-encoded identity for event responses
    pub owner_pubkey: Vec<u8>,        // For authorization
    pub replicas: u32,
    pub timestamp: u64,
    pub nonce: u64,
    pub signature: Vec<u8>,           // Covers manifest_json
}
```

### 7.4 SchedulerEvent

```rust
pub struct SchedulerEvent {
    pub tender_id: String,
    pub node_id: String,
    pub event_type: EventType,        // Started, Failed, HealthCheck
    pub reason: String,
    pub timestamp: u64,
    pub nonce: u64,
    pub signature: Vec<u8>,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub enum EventType {
    Started,
    Failed,
    HealthCheck,
}
```

### 7.5 Disposal

```rust
/// Disposal message for workload deletion (published via Gossipsub)
///
/// Fire-and-forget message that instructs all nodes with matching resources
/// to delete their local pods. Broadcast to `magik-fabric` topic.
pub struct Disposal {
    pub namespace: String,            // Kubernetes namespace (e.g., "default")
    pub kind: String,                 // Resource kind (e.g., "Deployment", "Pod")
    pub name: String,                 // Resource name (from metadata.name)
    pub timestamp: u64,               // ms since UNIX epoch
    pub nonce: u64,                   // Random for replay protection
    pub signature: Vec<u8>,           // Ed25519 signature over (namespace || kind || name || timestamp || nonce)
}
```

Each node that receives a DISPOSAL:
1. Verifies signature and timestamp freshness (±30s)
2. Adds resource coordinates (`namespace/kind/name`) to `disposal_map` (5-minute TTL)
3. Deletes local pods matching the resource coordinates
4. Workplane self-healer checks disposal status before scaling up

The `disposal_map` prevents the self-healer from recovering pods that are intentionally being deleted during the TTL window.

### 7.6 ApplyRequest (REST API)

```rust
pub struct ApplyRequest {
    pub replicas: u32,
    pub operation_id: String,         // Idempotency key
    pub manifest_json: String,        // Full manifest content
    pub origin_peer: String,          // Requesting node
    pub signature: Vec<u8>,
}
```

### 7.7 ServiceRecord (WDHT)

```rust
pub struct ServiceRecord {
    pub workload_id: String,          // "{namespace}/{kind}/{workload_name}"
    pub namespace: String,
    pub workload_kind: String,
    pub workload_name: String,
    pub peer_id: String,
    pub pod_name: Option<String>,
    pub ordinal: Option<u32>,
    pub addrs: Vec<String>,
    pub caps: Map<String, Value>,
    pub version: u64,
    pub ts: i64,
    pub nonce: String,
    pub ready: bool,
    pub healthy: bool,
}
```

---

## 8. API Reference

### 8.1 Machineplane REST API

#### Kubernetes API

```mermaid
classDiagram

    %% Core Concepts
    class Cluster {
        +API Server
        +etcd
        +Controller Manager
        +Scheduler
    }

    class Namespace {
        +Logical isolation
    }

    class Node {
        +Kubelet
        +Kube-proxy
        +Pods
    }

    class Pod {
        +Containers
        +Volumes
        +Networking
    }

    class Container {
        +Image
        +Environment
        +Ports
    }

    %% Workloads
    class Deployment {
        +ReplicaSets
        +Scaling
        +Rolling Updates
    }

    class ReplicaSet {
        +Ensures Pod count
    }

    class StatefulSet {
        +Stable network identity
        +Persistent volumes
    }

    class DaemonSet {
        +One Pod per Node
    }

    class Job {
        +Run to completion
    }

    class CronJob {
        +Scheduled jobs
    }

    %% Services & Networking
    class Service {
        +Stable DNS/ClusterIP
        +Load balancing
    }

    class Ingress {
        +HTTP routing
    }

    %% Config & Storage
    class ConfigMap {
        +Key-value configuration
    }

    class Secret {
        +Sensitive data
    }

    class Volume {
        +Ephemeral or Persistent
    }

    class PersistentVolume {
        +Cluster-wide storage
    }

    class PersistentVolumeClaim {
        +Pod storage request
    }

    %% Relationships

    %% Cluster & Namespaces
    Cluster --> Namespace : manages
    Cluster --> Node : manages
    Cluster --> PersistentVolume : provides

    %% Namespaced resources
    Namespace --> Pod : contains
    Namespace --> Service : contains
    Namespace --> Deployment : contains
    Namespace --> ReplicaSet : contains
    Namespace --> StatefulSet : contains
    Namespace --> DaemonSet : contains
    Namespace --> Job : contains
    Namespace --> CronJob : contains
    Namespace --> Ingress : contains
    Namespace --> ConfigMap : contains
    Namespace --> Secret : contains
    Namespace --> PersistentVolumeClaim : contains

    %% Workload relationships
    Node --> Pod : runs
    Deployment --> ReplicaSet : controls
    ReplicaSet --> Pod : manages
    StatefulSet --> Pod : manages
    DaemonSet --> Pod : schedules
    Job --> Pod : runs
    CronJob --> Job : schedules

    %% Networking
    Ingress --> Service : routes to
    Service --> Pod : targets

    %% Pod internals & config
    Pod --> Container : contains
    Pod --> ConfigMap : consumes
    Pod --> Secret : consumes
    Pod --> Volume : mounts

    %% Storage chain
    Volume --> PersistentVolumeClaim : references
    PersistentVolumeClaim --> PersistentVolume : binds
```

#### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/version` | GET | API version info |
| `/apis` | GET | API group list |
| `/health` | GET | Health check |
| `/api/v1/pubkey` | GET | Get node public key |
| `/nodes` | GET | List cluster nodes |
| `/disposal/{ns}/{kind}/{name}` | GET | Check if resource is marked for disposal |
| `/disposal/{ns}/{kind}/{name}` | DELETE | Delete workload (broadcasts DISPOSAL) |

The K8s API is **stateless by design**: all read operations derive state from the local container runtime,
and all write operations publish tenders via Gossipsub (fire-and-forget).

| Endpoint | Method | Description | Source |
|----------|--------|-------------|--------|
| `/api/v1/namespaces/{ns}/pods` | GET | List pods | Container runtime |
| `/api/v1/namespaces/{ns}/pods/{name}` | GET | Get pod | Container runtime |
| `/api/v1/namespaces/{ns}/pods/{name}/log` | GET | Get pod logs | Container runtime |
| `/apis/apps/v1/namespaces/{ns}/deployments` | GET | List deployments | Runtime pods (aggregated) |
| `/apis/apps/v1/namespaces/{ns}/deployments/{name}` | GET | Get deployment | Runtime pods (aggregated) |
| `/apis/apps/v1/namespaces/{ns}/deployments` | POST | Create deployment | Gossipsub tender |
| `/apis/apps/v1/namespaces/{ns}/deployments/{name}` | DELETE | Delete deployment | Gossipsub tender |
| `/apis/apps/v1/namespaces/{ns}/statefulsets` | GET | List statefulsets | Runtime pods (aggregated) |
| `/apis/apps/v1/namespaces/{ns}/statefulsets/{name}` | GET | Get statefulset | Runtime pods (aggregated) |
| `/apis/apps/v1/namespaces/{ns}/statefulsets` | POST | Create statefulset | Gossipsub tender |
| `/apis/apps/v1/namespaces/{ns}/statefulsets/{name}` | DELETE | Delete statefulset | Gossipsub tender |
| `/apis/apps/v1/namespaces/{ns}/replicasets` | GET | List replicasets | Derived from deployments |
| `/apis/apps/v1/namespaces/{ns}/replicasets/{name}` | GET | Get replicaset | Derived from deployments |

**Note**: K8s API responses show only workloads running on the **local node**. Mesh-wide aggregation
requires querying multiple nodes or using DHT-based discovery.

#### Tender Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/tenders` | POST | Create tender |
| `/tenders/{tender_id}` | GET | Get tender status |
| `/tenders/{tender_id}/candidates` | POST | Get scheduling candidates |

#### Debug Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/debug/peers` | GET | List connected peers |
| `/debug/tenders` | GET | List tracked tenders |
| `/debug/local_identity` | GET | Get local node identity |
| `/debug/dht/peers` | GET | List DHT peers |
| `/debug/pods` | GET | List local pods from runtime (stateless) |

### 8.2 Workplane RPC Methods

| Method | Description | Leader Only |
|--------|-------------|-------------|
| `healthz` | Health check | No |
| `leader` | Query current Raft leader | No |
| Custom | Application-defined | Configurable |

---

## 9. Configuration

### 9.1 Machineplane CLI

```rust
pub struct Cli {
    /// Host address for REST API
    #[arg(long, default_value = "0.0.0.0")]
    pub rest_api_host: String,

    /// Port for REST API
    #[arg(long, default_value = "3000")]
    pub rest_api_port: u16,

    /// Runtime socket path
    #[arg(long, env = "CONTAINER_HOST")]
    pub runtime_socket: Option<String>,

    /// Bootstrap peer addresses in format: `<identity_hex>@<ip:port>`
    #[arg(long)]
    pub bootstrap_peer: Vec<String>,

    /// Port for Korium mesh networking (0 = auto-assign)
    #[arg(long, default_value = "0")]
    pub korium_port: u16,

    /// Host address for Korium mesh networking
    #[arg(long, default_value = "0.0.0.0")]
    pub korium_host: String,

    /// Use ephemeral Ed25519 signing keys
    #[arg(long, default_value_t = false)]
    pub signing_ephemeral: bool,

    /// Interval in seconds between DHT routing table refreshes
    #[arg(long, default_value = "60")]
    pub dht_refresh_interval_secs: u64,
}
```

**Runtime Socket Resolution Order:**
1. `--runtime-socket` CLI argument
2. `CONTAINER_HOST` environment variable
3. Auto-detection via `RuntimeEngine::detect_socket`
4. Default system socket path

### 9.2 Workplane Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BEE_PRIVATE_KEY` | Ed25519 private key (base64) | **Required** |
| `BEE_WORKLOAD_NAME` | Workload name | **Required** |
| `BEE_POD_NAME` | Pod name | **Required** |
| `BEE_NAMESPACE` | Kubernetes namespace | `default` |
| `BEE_WORKLOAD_KIND` | `Deployment` or `StatefulSet` | `Deployment` |
| `BEE_REPLICAS` | Desired replica count | `1` |
| `BEE_LIVENESS_URL` | HTTP liveness probe URL | None |
| `BEE_READINESS_URL` | HTTP readiness probe URL | None |
| `BEE_BEEMESH_API` | Machineplane API URL | `http://127.0.0.1:8080` |
| `BEE_HEALTH_PROBE_INTERVAL` | Health check interval | `10s` |
| `BEE_HEALTH_PROBE_TIMEOUT` | Health check timeout | `5s` |
| `BEE_REPLICA_CHECK_INTERVAL` | Reconciliation interval | `30s` |

---

## 10. Failure Handling

### 10.1 Network Partition

| Scenario | Machineplane Behavior | Workplane Behavior |
|----------|----------------------|-------------------|
| Partial partition | Nodes continue bidding locally | WDHT records expire; replicas re-resolve |
| Full partition | No global coordination; local deploys continue | Minority partition refuses writes (Raft) |
| Partition heals | Conflicting awards tolerated; freshest wins | Raft leader re-elected; records refresh |

### 10.2 Node Failure

| Scenario | Detection | Recovery |
|----------|-----------|----------|
| Machine crash | MDHT record expires | Workplane requests new replica |
| Workplane crash | WDHT record expires | SelfHealer on other replicas requests replacement |
| Runtime crash | Container runtime monitoring | Daemon restarts container or requests replacement |

### 10.3 Deployment Failure

```rust
// Machineplane emits SchedulerEvent on deployment attempt
match process_manifest_deployment(...).await {
    Ok(pod_id) => {
        scheduler.publish_event(&tender_id, EventType::Started, "deployed successfully");
    }
    Err(e) => {
        scheduler.publish_event(&tender_id, EventType::Failed, &e.to_string());
        // Other nodes may re-evaluate tender after backoff
    }
}
```

### 10.4 Consistency Guarantees

| Workload Type | Write Consistency | Read Consistency |
|---------------|------------------|------------------|
| Stateless | None (all replicas equal) | Eventual (WDHT TTL) |
| Stateful | Leader-only (Raft quorum) | Read-your-writes (via leader) |

---

## Appendix A: Security Audit Findings (December 2025)

### A.1 Summary

| Severity | Count | Status |
|----------|-------|--------|
| **Critical** | 0 | — |
| **High** | 4 | Open |
| **Medium** | 8 | Open |
| **Low** | 5 | Open |

### A.2 High Severity Findings

#### A.2.1 Clock Skew / Replay Window Mismatch

* **Location:** `scheduler.rs` lines 52-62
* **Issue:** `REPLAY_WINDOW_MS` (5 min) is 10× larger than `MAX_CLOCK_SKEW_MS` (30s). Attackers can replay messages by timing delivery.
* **Remediation:** Align constants or implement monotonic message IDs alongside timestamps.

#### A.2.2 Tender Storm Amplification

* **Location:** `selfheal.rs` lines 145-160
* **Issue:** No rate limiting on tender creation during reconciliation. Multiple nodes detecting under-replication simultaneously cause broadcast storms.
* **Remediation:** Implement leader-only reconciliation or add global tender budget per cycle.

#### A.2.3 Deploy/Delete Race Condition

* **Location:** `scheduler.rs` lines 124-155
* **Issue:** `is_disposing()` check and `mark_disposal()` are not atomic. Deployments can race with deletions causing workload flapping.
* **Remediation:** Use atomic compare-and-swap or hold write lock during entire deployment transaction.

#### A.2.4 O(n) Replay Filter Eviction

* **Location:** `scheduler.rs` lines 935-960
* **Issue:** Linear scan to find oldest entry on every insertion at capacity (100,000 entries). CPU DoS vector.
* **Remediation:** Replace `HashMap` with `IndexMap` (already used elsewhere) or LRU cache.

### A.3 Medium Severity Findings

| ID | Issue | Location | Remediation |
|----|-------|----------|-------------|
| M.1 | Unsafe env var mutation | `lib.rs:133-138` | Pass socket config through type system |
| M.2 | Missing REST body size limit | `api.rs:446-453` | Add `DefaultBodyLimit::max(MAX_MANIFEST_SIZE)` |
| M.3 | Message size limit mismatch | `network.rs` vs `messages.rs` | Align `MAX_MESSAGE_SIZE = MAX_BINCODE_SIZE` |
| M.4 | Missing award replay protection | `network.rs:602-625` | Add `seen_awards` filter keyed on `(tender_id, owner, nonce)` |
| M.5 | Mutex poisoning panic strategy | Multiple locations | Propagate errors or ensure panic causes shutdown |
| M.6 | DHT record namespace bypass | `discovery.rs:113-145` | Validate `workload_id` matches record fields |
| M.7 | Selection window timing attack | `scheduler.rs:507-515` | Add ±10% randomized jitter |
| M.8 | Runtime socket path injection | `crun.rs:54-70` | Validate against allowed path prefixes |

### A.4 Low Severity Findings

| ID | Issue | Location | Remediation |
|----|-------|----------|-------------|
| L.1 | Non-cryptographic nonce source | `scheduler.rs:448` | Document entropy requirements |
| L.2 | Self-reported bid scores | `scheduler.rs:439-445` | Implement verifiable capacity proofs |
| L.3 | No built-in TLS for REST API | `api.rs` | Document TLS proxy requirement |
| L.4 | Internal state in error messages | Multiple | Sanitize errors returned to callers |
| L.5 | Hardcoded Raft timeouts | `raft.rs:30-45` | Document WAN tuning in ops guide |

### A.5 Remediation Roadmap

#### Immediate (Pre-Production Blockers)
1. Replace `HashMap` with `IndexMap` in replay filters for O(1) eviction
2. Add award replay protection filter
3. Align `MAX_MESSAGE_SIZE = MAX_BINCODE_SIZE = 16 MiB`

#### Short-term (Hardening)
4. Add `DefaultBodyLimit` layer to REST API routes
5. Implement leader-only reconciliation to prevent tender storms
6. Validate DHT record `workload_id` field consistency
7. Remove `unsafe` env var mutation; use typed configuration

#### Long-term (Systemic)
8. Implement verifiable capacity scoring
9. Add native TLS termination option
10. Replace ad-hoc timestamp checks with vector clocks or HLC
11. Add property-based fuzz testing for message parsing

---

## References

- [Magik README](README.md) - Project overview
- [Machineplane Spec](machineplane/machineplane-spec.md) - Detailed machineplane specification
- [Workplane Spec](workplane/workplane-spec.md) - Detailed workplane specification
- [Korium Documentation](https://github.com/magik/korium) - P2P mesh networking library
- [Raft Consensus Algorithm](https://raft.github.io/) - Leader election reference
