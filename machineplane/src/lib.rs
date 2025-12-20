//! # Machineplane
//!
//! Machineplane is the core of Magik, responsible for orchestrating
//! microVM workloads across a distributed mesh network. It provides:
//!
//! - **Kubernetes-compatible API**: Accepts manifests via a kubectl-compatible REST API
//! - **Decentralized Scheduling**: Uses a tender/bid/award protocol for workload placement
//! - **MicroVM Isolation**: Deploys workloads in hardware-isolated microVMs via libkrun
//! - **Mesh Networking**: Leverages Korium for peer-to-peer communication and DHT-based discovery
//!
//! ## Architecture
//!
//! The machineplane daemon consists of several subsystems:
//! - [`api`]: REST API handlers for Kubernetes-compatible endpoints
//! - [`network`]: Korium-based mesh networking and message routing
//! - [`scheduler`]: Tender/bid/award scheduling protocol implementation
//! - [`runtime`]: Runtime engine abstraction and manifest processing
//! - [`runtimes`]: MicroVM runtime implementation (KrunEngine)
//! - [`messages`]: Protocol message types for inter-node communication

use clap::Parser;
use env_logger::Env;

pub mod api;
pub mod messages;
pub mod network;
pub mod runtime;
pub mod runtimes;
pub mod scheduler;

/// Command-line interface configuration for the machineplane daemon.
///
/// All options can also be set via environment variables. The daemon binds
/// to both a REST API port (for kubectl compatibility) and a Korium port
/// (for mesh networking).
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Host address for the REST API server (e.g., "0.0.0.0" for all interfaces)
    #[arg(long, default_value = "0.0.0.0")]
    pub rest_api_host: String,

    /// Port for the REST API server (kubectl-compatible endpoints)
    #[arg(long, default_value = "3000")]
    pub rest_api_port: u16,

    /// Optional human-readable node name for identification
    #[arg(long)]
    pub node_name: Option<String>,

    /// Use ephemeral Ed25519 signing keys (regenerated on each startup)
    #[arg(long, default_value_t = false)]
    pub signing_ephemeral: bool,

    /// Use ephemeral KEM (Key Encapsulation Mechanism) keys for encryption
    #[arg(long, default_value_t = false)]
    pub kem_ephemeral: bool,

    /// Shorthand for both --signing-ephemeral and --kem-ephemeral
    #[arg(long, default_value_t = false)]
    pub ephemeral_keys: bool,

    /// Bootstrap peer addresses in format: `<identity_hex>@<ip:port>`
    /// Multiple peers can be specified by repeating the flag.
    #[arg(long)]
    pub bootstrap_peer: Vec<String>,

    /// Port for Korium mesh networking (0 = auto-assign)
    #[arg(long, default_value = "0")]
    pub korium_port: u16,

    /// Host address for Korium mesh networking
    #[arg(long, default_value = "0.0.0.0")]
    pub korium_host: String,

    /// Interval in seconds between DHT routing table refreshes
    #[arg(long, default_value = "60")]
    pub dht_refresh_interval_secs: u64,
}

/// Type alias for daemon configuration (same as CLI arguments)
pub type DaemonConfig = Cli;

impl Default for Cli {
    fn default() -> Self {
        Self {
            rest_api_host: "127.0.0.1".to_string(),
            rest_api_port: 3000,
            node_name: None,
            signing_ephemeral: false,
            kem_ephemeral: false,
            ephemeral_keys: false,
            bootstrap_peer: Vec::new(),
            korium_port: 0,
            korium_host: "0.0.0.0".to_string(),
            dht_refresh_interval_secs: 60,
        }
    }
}

/// Starts the machineplane daemon with the given configuration.
///
/// This is the main entry point that initializes all subsystems:
/// 1. Configures logging
/// 2. Starts the Korium mesh node and joins bootstrap peers
/// 3. Subscribes to the Magik fabric topic for tender messages
/// 4. Initializes the runtime registry for microVM deployment
/// 5. Starts the REST API server for kubectl compatibility
///
/// # Arguments
/// * `cli` - Daemon configuration from CLI arguments or programmatic setup
///
/// # Returns
/// * `Ok(handles)` - Vector of tokio task handles for background processes
/// * `Err(_)` - If initialization fails (e.g., libkrun not available)
///
/// # Example
/// ```ignore
/// let config = DaemonConfig::default();
/// let handles = start_machineplane(config).await?;
/// futures::future::join_all(handles).await;
/// ```
pub async fn start_machineplane(
    cli: DaemonConfig,
) -> anyhow::Result<Vec<tokio::task::JoinHandle<()>>> {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();

    let bind_addr = format!("{}:{}", cli.korium_host, cli.korium_port);
    let (node, peer_rx, peer_tx) = network::setup_korium_node(&bind_addr).await?;

    let local_identity = node.identity();
    log::info!("Local Identity: {}", local_identity);

    let mut connected_peers: Vec<String> = Vec::new();
    for peer_spec in &cli.bootstrap_peer {
        let parts: Vec<&str> = peer_spec.split('@').collect();
        if parts.len() == 2 {
            let identity = parts[0];
            let addr = parts[1];
            match node.bootstrap(identity, addr).await {
                Ok(_) => {
                    log::info!("Bootstrapped from peer: {}", peer_spec);
                    connected_peers.push(identity.to_string());
                }
                Err(e) => log::warn!("Failed to bootstrap from {}: {}", peer_spec, e),
            }
        } else {
            log::warn!(
                "Invalid bootstrap peer format '{}'. Expected: <identity_hex>@<ip:port>",
                peer_spec
            );
        }
    }
    if !connected_peers.is_empty() {
        let _ = peer_tx.send(connected_peers);
    }

    network::subscribe_to_fabric(&node).await?;

    let local_addr = node.local_addr()?;
    let advertise_addr = if local_addr.ip().is_unspecified() {
        format!("127.0.0.1:{}", local_addr.port())
    } else {
        local_addr.to_string()
    };
    node.publish_address(vec![advertise_addr]).await?;
    log::debug!("Published address to DHT for direct messaging");

    let (control_tx, control_rx) =
        tokio::sync::mpsc::unbounded_channel::<network::NetworkControl>();

    network::set_control_sender(control_tx.clone());

    let control_tx_for_korium = control_tx.clone();
    let dht_refresh_interval = cli.dht_refresh_interval_secs;
    let korium_handle = tokio::spawn(async move {
        let _keeper = control_tx_for_korium;
        if let Err(e) =
            network::start_korium_node(node, peer_tx, control_rx, dht_refresh_interval).await
        {
            log::error!("Korium node error: {}", e);
        }
    });

    log::info!("Initializing runtime registry (microVM via libkrun)...");
    if let Err(e) = runtime::initialize_runtime().await {
        log::error!(
            "Failed to initialize runtime: {}. libkrun is required for Magik.",
            e
        );
        return Err(anyhow::anyhow!("libkrun initialization failed: {}", e));
    }
    log::info!("Runtime registry initialized successfully");

    let mut handles = Vec::new();

    let local_identity_bytes = hex::decode(&local_identity)
        .map_err(|e| anyhow::anyhow!("local_identity should be valid hex: {}", e))?;
    let app = api::build_router(peer_rx, control_tx.clone(), local_identity_bytes);

    let bind_addr = format!("{}:{}", cli.rest_api_host, cli.rest_api_port);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    log::info!("REST API listening on {}", listener.local_addr()?);
    handles.push(tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app.clone().into_make_service()).await {
            log::error!("axum server error: {}", e);
        }
    }));

    let mut all = vec![korium_handle];
    all.extend(handles);
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_cli_config() {
        let cli = Cli::default();
        assert_eq!(cli.rest_api_host, "127.0.0.1");
        assert_eq!(cli.rest_api_port, 3000);
        assert!(cli.bootstrap_peer.is_empty());
    }
}
