//! # Workplane CLI
//!
//! Command-line entry point for the BeeMesh workplane agent.
//!
//! # Usage
//!
//! ```bash
//! workplane --namespace default --workload my-app --pod my-app-abc123
//! ```
//!
//! # Environment Variables
//!
//! All CLI arguments can also be set via environment variables:
//!
//! - `BEE_NAMESPACE`: Kubernetes namespace
//! - `BEE_WORKLOAD_NAME`: Workload name (Deployment, StatefulSet, etc.)
//! - `BEE_POD_NAME`: Pod name
//! - `BEE_REPLICAS`: Desired replica count
//! - `BEE_MACHINE_API`: URL of the machineplane API
//! - `BEE_PRIVATE_KEY_B64`: Base64-encoded private key for identity
//!
//! See `--help` for the full list of options.

use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as Base64;
use clap::Parser;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use workplane::{Agent, Config, run_supervisor};

/// Command-line arguments for the workplane agent.
#[derive(Parser, Debug)]
#[command(name = "workplane", author, version, about = "Magik Workplane agent")]
struct Args {
    /// Kubernetes namespace for service discovery
    #[arg(long, env = "BEE_NAMESPACE")]
    namespace: Option<String>,

    /// Name of the workload (Deployment, StatefulSet, etc.)
    #[arg(long = "workload", env = "BEE_WORKLOAD_NAME")]
    workload_name: Option<String>,

    /// Name of this pod instance
    #[arg(long = "pod", env = "BEE_POD_NAME")]
    pod_name: Option<String>,

    /// Desired number of replicas
    #[arg(long, env = "BEE_REPLICAS", default_value_t = 1)]
    replicas: usize,

    /// Workload kind (Deployment, StatefulSet, DaemonSet)
    #[arg(
        long = "workload-kind",
        env = "BEE_WORKLOAD_KIND",
        default_value = "Deployment"
    )]
    workload_kind: String,

    /// HTTP endpoint for liveness probes
    #[arg(long = "liveness-url", env = "BEE_LIVENESS_URL", default_value = "")]
    liveness_url: String,

    /// HTTP endpoint for readiness probes
    #[arg(long = "readiness-url", env = "BEE_READINESS_URL", default_value = "")]
    readiness_url: String,

    /// Interval for checking replica count (seconds)
    #[arg(long, env = "BEE_REPLICA_CHECK_INTERVAL_SECS", default_value_t = 30)]
    replica_check_interval_secs: u64,

    /// URL of the machineplane API
    #[arg(
        long = "machine-api",
        env = "BEE_MACHINE_API",
        default_value = "http://localhost:8080"
    )]
    machine_api: String,

    /// Base64-encoded private key for identity
    #[arg(long = "privkey", env = "BEE_PRIVATE_KEY_B64")]
    private_key_b64: Option<String>,

    /// Comma-separated list of bootstrap peer addresses
    #[arg(long, env = "BEE_BOOTSTRAP_PEERS")]
    bootstrap_peers: Option<String>,

    /// Comma-separated list of listen addresses
    #[arg(long, env = "BEE_LISTEN_ADDRS")]
    listen_addrs: Option<String>,

    /// Comma-separated list of allowed workload patterns
    #[arg(long, env = "BEE_ALLOW_WORKLOADS")]
    allow_workloads: Option<String>,

    /// Comma-separated list of denied workload patterns
    #[arg(long, env = "BEE_DENY_WORKLOADS")]
    deny_workloads: Option<String>,

    /// Allow cross-namespace service discovery
    #[arg(long, env = "BEE_ALLOW_CROSS_NAMESPACE", default_value_t = false)]
    allow_cross_namespace: bool,

    /// TTL for DHT records (seconds)
    #[arg(long, env = "BEE_DHT_TTL_SECS", default_value_t = 15)]
    dht_ttl_secs: u64,

    /// Interval for DHT heartbeat announcements (seconds)
    #[arg(long, env = "BEE_DHT_HEARTBEAT_SECS", default_value_t = 5)]
    dht_heartbeat_secs: u64,

    /// Interval for health checks (seconds)
    #[arg(long, env = "BEE_HEALTH_INTERVAL_SECS", default_value_t = 10)]
    health_interval_secs: u64,

    /// Timeout for health check requests (seconds)
    #[arg(long, env = "BEE_HEALTH_TIMEOUT_SECS", default_value_t = 5)]
    health_timeout_secs: u64,

    /// Command to execute as a child process (init supervisor mode).
    /// When specified, workplane runs as PID 1 and spawns this command.
    /// The command string is split on whitespace for execution.
    /// Example: --exec "nginx -g 'daemon off;'"
    #[arg(long, env = "BEE_EXEC")]
    exec: Option<String>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        error!(error = %err, "workplane failed");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    init_tracing();

    let args = Args::parse();

    let pod_name = args
        .pod_name
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("pod name is required"))?;

    // Parse pod name format: workload.namespace.uuid
    // Extract namespace and workload if not explicitly provided
    let (parsed_workload, parsed_namespace) = parse_pod_name(&pod_name);

    let namespace = args
        .namespace
        .filter(|s| !s.is_empty())
        .or(parsed_namespace)
        .ok_or_else(|| anyhow::anyhow!("namespace is required (provide --namespace or use pod name format: workload.namespace.uuid)"))?;
    let workload_name = args
        .workload_name
        .filter(|s| !s.is_empty())
        .or(parsed_workload)
        .ok_or_else(|| anyhow::anyhow!("workload name is required (provide --workload or use pod name format: workload.namespace.uuid)"))?;

    let private_key = args
        .private_key_b64
        .filter(|s| !s.is_empty())
        .map(|k| Base64.decode(k.trim()))
        .transpose()
        .context("decode private key")?
        .unwrap_or_default();

    let bootstrap_peers = split_csv(args.bootstrap_peers);

    let mut cfg = Config {
        peer_id_str: None,
        private_key,
        namespace,
        workload_name: workload_name.clone(),
        pod_name: pod_name.clone(),
        replicas: args.replicas,
        workload_kind: args.workload_kind,
        liveness_url: args.liveness_url,
        readiness_url: args.readiness_url,
        bootstrap_peer_strings: bootstrap_peers,
        magik_api: args.machine_api,
        replica_check_interval: std::time::Duration::from_secs(args.replica_check_interval_secs),
        dht_ttl: std::time::Duration::from_secs(args.dht_ttl_secs),
        heartbeat_interval: std::time::Duration::from_secs(args.dht_heartbeat_secs),
        health_probe_interval: std::time::Duration::from_secs(args.health_interval_secs),
        health_probe_timeout: std::time::Duration::from_secs(args.health_timeout_secs),
        allow_cross_namespace: args.allow_cross_namespace,
        allowed_workloads: split_csv(args.allow_workloads),
        denied_workloads: split_csv(args.deny_workloads),
        listen_addrs: split_csv(args.listen_addrs),
        exec_command: args.exec,
    };
    cfg.apply_defaults();

    let mut agent = Agent::new(cfg.clone()).await?;
    agent.start();

    info!(%workload_name, %pod_name, "workplane started");

    // Check if we're in init supervisor mode (--exec specified)
    if let Some(ref exec_cmd) = cfg.exec_command {
        info!(exec = %exec_cmd, "Running in init supervisor mode");

        // Run the supervisor which spawns the workload
        // The supervisor runs concurrently with the mesh agent
        let supervisor_handle = tokio::spawn({
            let exec_cmd = exec_cmd.clone();
            async move { run_supervisor(&exec_cmd).await }
        });

        // Wait for either supervisor exit or ctrl+c
        tokio::select! {
            result = supervisor_handle => {
                // Supervisor exited (child process terminated)
                agent.close().await;
                match result {
                    Ok(Ok(code)) => {
                        info!(code = code, "Workload exited, shutting down");
                        std::process::exit(code);
                    }
                    Ok(Err(e)) => {
                        error!(error = %e, "Supervisor error");
                        std::process::exit(1);
                    }
                    Err(e) => {
                        error!(error = %e, "Supervisor task failed");
                        std::process::exit(1);
                    }
                }
            }
            _ = signal::ctrl_c() => {
                info!("Received ctrl+c, shutting down");
                agent.close().await;
                std::process::exit(0);
            }
        }
    } else {
        // Standard agent mode (infra container)
        signal::ctrl_c().await.context("wait for ctrl+c")?;
        info!("shutting down agent");
        agent.close().await;
    }

    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .try_init();
}

fn split_csv(input: Option<String>) -> Vec<String> {
    input
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Parses pod name in format: workload.namespace.uuid
/// Returns (workload, namespace) if parseable, (None, None) otherwise.
fn parse_pod_name(pod_name: &str) -> (Option<String>, Option<String>) {
    let parts: Vec<&str> = pod_name.splitn(3, '.').collect();
    if parts.len() >= 2 {
        let workload = parts[0].to_string();
        let namespace = parts[1].to_string();
        if !workload.is_empty() && !namespace.is_empty() {
            return (Some(workload), Some(namespace));
        }
    }
    (None, None)
}
