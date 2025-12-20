//! # Workplane - BeeMesh Worker Agent
//!
//! Workplane is the distributed worker agent component of the BeeMesh mesh.
//! Each node in the mesh runs a workplane agent that participates in:
//!
//! - **Service Discovery**: DHT-based service registration and lookup
//! - **Raft Consensus**: Distributed state machine for cluster coordination
//! - **Self-Healing**: Automatic health monitoring and recovery
//! - **RPC Communication**: Request/response messaging between nodes
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │                        Workplane Agent                        │
//! ├──────────────────────────────────────────────────────────────┤
//! │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │
//! │  │  Discovery  │  │    Raft     │  │     Self-Heal       │   │
//! │  │    (DHT)    │  │ (Consensus) │  │   (Health Check)    │   │
//! │  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘   │
//! │         │                │                    │               │
//! │         └────────────────┴────────────────────┘               │
//! │                          │                                    │
//! │                    ┌─────┴─────┐                             │
//! │                    │  Network  │                             │
//! │                    │ (Korium)  │                             │
//! │                    └───────────┘                             │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Modules
//!
//! - [`config`]: Configuration structures and loading
//! - [`discovery`]: DHT-based service discovery
//! - [`raft`]: Raft consensus protocol implementation
//! - [`network`]: Korium mesh networking
//! - [`selfheal`]: Health checking and automatic recovery
//! - [`rpc`]: RPC message types and handlers
//! - [`agent`]: Main agent logic and lifecycle
//!
//! # Example
//!
//! ```rust,ignore
//! use workplane::{Config, Agent};
//!
//! let config = Config::load()?;
//! let agent = Agent::new(config).await?;
//! agent.run().await?;
//! ```

pub mod config;
pub mod discovery;
pub mod raft;
pub mod network;
pub mod selfheal;
pub mod rpc;
pub mod agent;
pub mod supervisor;

mod metrics;

pub use config::Config;
pub use agent::Agent;
pub use supervisor::run_supervisor;
