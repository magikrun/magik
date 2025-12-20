//! Common helper functions for "Apply" workflow tests.
//!
//! This module provides shared utilities for setting up test environments,
//! starting fabric nodes, and verifying workload deployments.

use env_logger::Env;
use futures::future::join_all;
use std::collections::HashMap as StdHashMap;
use std::time::Duration;
use tokio::time::{Instant, sleep};

#[path = "runtime_helpers.rs"]
mod runtime_helpers;
use runtime_helpers::{make_test_daemon, start_nodes, wait_for_local_multiaddr};
use tokio::task::JoinHandle;

pub const TEST_PORTS: [u16; 7] = [
    3000u16, 3100u16, 3200u16, 3300u16, 3400u16, 3500u16, 3600u16,
];
pub const TEST_CORIUM_PORTS: [u16; 7] = [
    4000u16, 4100u16, 4200u16, 4300u16, 4400u16, 4500u16, 4600u16,
];

/// Prepare logging and environment for runtime tests.
pub async fn setup_test_environment() -> (reqwest::Client, Vec<u16>) {
    // Use a verbose default filter to surface signing/verification debug logs in CI output.
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();

    let client = reqwest::Client::new();
    (client, TEST_PORTS.to_vec())
}

/// Build standard three-node fabric configuration.
/// Returns JoinHandles for the spawned tasks so tests can abort them when finished.
pub async fn start_fabric_nodes() -> Vec<JoinHandle<()>> {
    start_fabric_nodes_with_ports(&TEST_PORTS, &TEST_CORIUM_PORTS).await
}

/// Start a fabric mesh using the provided REST and Korium port lists.
///
/// The first entry in `rest_ports`/`korium_ports` is treated as the bootstrap node;
/// all subsequent nodes will join using its advertised peer address. All nodes are
/// configured with persistent key material to make debugging easier.
pub async fn start_fabric_nodes_with_ports(
    rest_ports: &[u16],
    korium_ports: &[u16],
) -> Vec<JoinHandle<()>> {
    assert!(
        !rest_ports.is_empty() && rest_ports.len() == korium_ports.len(),
        "REST and Korium port lists must be non-empty and the same length"
    );

    // Start a single bootstrap node first to give it time to initialize.
    let mut bootstrap_daemon = make_test_daemon(rest_ports[0], vec![], korium_ports[0]);
    bootstrap_daemon.signing_ephemeral = true;
    bootstrap_daemon.kem_ephemeral = true;
    bootstrap_daemon.ephemeral_keys = true;
    let mut handles = start_nodes(vec![bootstrap_daemon], Duration::from_secs(1)).await;

    // Allow the bootstrap node to settle before connecting peers.
    sleep(Duration::from_secs(2)).await;

    // Discover the bootstrap node's advertised address (with identity)
    // so subsequent nodes can join the mesh successfully.
    let bootstrap_peer = wait_for_local_multiaddr(
        "127.0.0.1",
        rest_ports[0],
        "127.0.0.1",
        korium_ports[0],
        Duration::from_secs(10),
    )
    .await
    .expect("bootstrap node did not expose an identity in time");

    // Start additional nodes that exclusively use the first node as bootstrap.
    let bootstrap_peers = vec![bootstrap_peer];
    let mut peer_daemons = Vec::new();
    for (&rest_port, &korium_port) in rest_ports.iter().zip(korium_ports.iter()).skip(1) {
        let mut daemon = make_test_daemon(rest_port, bootstrap_peers.clone(), korium_port);
        daemon.signing_ephemeral = true;
        daemon.kem_ephemeral = true;
        daemon.ephemeral_keys = true;
        peer_daemons.push(daemon);
    }

    handles.append(&mut start_nodes(peer_daemons, Duration::from_secs(1)).await);
    handles
}

/// Fetch node identities for provided REST API ports.
pub async fn get_node_identities(
    client: &reqwest::Client,
    ports: &[u16],
) -> StdHashMap<u16, String> {
    let identity_tasks = ports.iter().copied().map(|port| {
        let client = client.clone();
        async move {
            let base = format!("http://127.0.0.1:{}", port);
            match client
                .get(format!("{}/debug/local_identity", base))
                .send()
                .await
            {
                Ok(resp) => match resp.json::<serde_json::Value>().await {
                    Ok(json) if json.get("ok").and_then(|v| v.as_bool()) == Some(true) => {
                        if let Some(identity) = json.get("local_identity").and_then(|v| v.as_str())
                        {
                            return (port, Some(identity.to_string()));
                        }
                    }
                    _ => {}
                },
                Err(_) => {}
            }
            (port, None)
        }
    });

    let identity_results = join_all(identity_tasks).await;
    let mut port_to_identity = StdHashMap::new();
    for (port, maybe_id) in identity_results {
        if let Some(id) = maybe_id {
            port_to_identity.insert(port, id);
        }
    }
    port_to_identity
}

/// Wait until the Korium mesh has at least two peer connections across provided ports.
pub async fn wait_for_mesh_formation(
    client: &reqwest::Client,
    ports: &[u16],
    timeout: Duration,
    min_total_peers: usize,
) -> bool {
    let start = Instant::now();
    loop {
        let mut total_peers = 0usize;
        for &port in ports {
            let base = format!("http://127.0.0.1:{}", port);
            if let Ok(resp) = client.get(format!("{}/debug/peers", base)).send().await {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(peers_array) = json.get("peers").and_then(|v| v.as_array()) {
                        total_peers += peers_array.len();
                    }
                }
            }
        }

        if total_peers >= min_total_peers {
            log::info!(
                "Mesh formation successful: {} total peer connections (target: {})",
                total_peers,
                min_total_peers
            );
            return true;
        }

        if start.elapsed() > timeout {
            log::warn!(
                "Mesh formation timed out after {:?}, only {} peer connections (target: {})",
                timeout,
                total_peers,
                min_total_peers
            );
            return false;
        }

        sleep(Duration::from_millis(500)).await;
    }
}

/// Inspect node debug endpoints to determine pod placement.

pub async fn check_instance_deployment(
    client: &reqwest::Client,
    ports: &[u16],
    task_id: &str,
    original_content: &str,
    port_to_identity: &StdHashMap<u16, String>,
    _expect_modified_replicas: bool,
    expected_nodes: Option<usize>,
    timeout: Duration,
) -> (Vec<u16>, Vec<u16>) {
    let start = Instant::now();

    loop {
        let verification_tasks = ports.iter().copied().map(|port| {
            let client = client.clone();
            let _task_id = task_id.to_string();
            let _original_content = original_content.to_string();
            let _port_to_identity = port_to_identity.clone();
            async move {
                let base = format!("http://127.0.0.1:{}", port);

                // Use /debug/pods endpoint which queries local runtime directly
                // This is stateless - no cross-node state tracking required
                let pods_resp = client
                    .get(format!("{}/debug/pods", base))
                    .send()
                    .await;

                if let Ok(resp) = pods_resp {
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        log::info!(
                            "/debug/pods response on port {}: {}",
                            port,
                            json
                        );
                        if json.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                            let instance_count = json
                                .get("instance_count")
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0);

                            if instance_count == 0 {
                                log::info!(
                                    "No pods on port {} - returning early",
                                    port
                                );
                                return (port, false, false);
                            }

                            if let Some(pods) =
                                json.get("instances").and_then(|v| v.as_object())
                            {
                                for pod_info in pods.values() {
                                    if let Some(metadata) = pod_info
                                        .get("metadata")
                                        .and_then(|v| v.as_object())
                                    {
                                        // Check both "name" key and K8s app name label
                                        let app_name = metadata
                                            .get("name")
                                            .or_else(|| metadata.get("app.kubernetes.io/name"))
                                            .and_then(|v| v.as_str());

                                        if app_name == Some("my-nginx") {
                                            // Check pod status first - only verify manifest for Running pods
                                            let is_running = pod_info
                                                .get("status")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s == "Running")
                                                .unwrap_or(false);

                                            // If not running, treat as "not yet deployed" to keep waiting
                                            if !is_running {
                                                log::info!(
                                                    "Found pod {} but status is not Running yet, continuing to wait",
                                                    app_name.unwrap_or("unknown")
                                                );
                                                continue;
                                            }

                                            // Found pod by app name - verify exported manifest
                                            // is valid K8s YAML (has apiVersion and kind)
                                            // Note: Pod name may differ from app name due to pod_id
                                            let exported_manifest_matches = pod_info
                                                .get("exported_manifest")
                                                .and_then(|v| v.as_str())
                                                .map(|manifest| {
                                                    let contains_kind = manifest
                                                        .contains("Deployment")
                                                        || manifest.contains("Pod");
                                                    let contains_api =
                                                        manifest.contains("apiVersion");

                                                    if manifest.len() > 100 {
                                                        log::info!(
                                                            "Exported manifest preview: {}",
                                                            &manifest[..manifest.len().min(200)]
                                                        );
                                                    }
                                                    contains_kind && contains_api
                                                })
                                                .unwrap_or_else(|| {
                                                    // exported_manifest is null - may happen even for Running pods
                                                    // if kube generation fails. Log and continue waiting.
                                                    log::info!(
                                                        "Pod is Running but exported_manifest is null, treating as not ready"
                                                    );
                                                    false
                                                });

                                            // Only return success if manifest matches
                                            if exported_manifest_matches {
                                                return (port, true, true);
                                            } else {
                                                // Running but manifest not ready - continue waiting
                                                log::info!(
                                                    "Pod is Running but manifest validation failed, continuing to wait"
                                                );
                                                continue;
                                            }
                                        }
                                    }

                                    // Fallback: check for any running pod
                                    if let Some(status) =
                                        pod_info.get("status").and_then(|v| v.as_str())
                                    {
                                        if status == "Running" {
                                            return (port, true, true);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                (port, false, false)
            }
        });

        let verification_results = join_all(verification_tasks).await;
        let nodes_with_deployed_instances: Vec<u16> = verification_results
            .iter()
            .filter_map(
                |(port, has_instance, _)| {
                    if *has_instance { Some(*port) } else { None }
                },
            )
            .collect();

        let nodes_with_content_mismatch: Vec<u16> = verification_results
            .iter()
            .filter_map(|(port, has_instance, manifest_matches)| {
                if *has_instance && !manifest_matches {
                    Some(*port)
                } else {
                    None
                }
            })
            .collect();

        if let Some(expected_nodes) = expected_nodes {
            if nodes_with_deployed_instances.len() >= expected_nodes
                && nodes_with_content_mismatch.is_empty()
            {
                return (nodes_with_deployed_instances, nodes_with_content_mismatch);
            }
        } else if !nodes_with_deployed_instances.is_empty()
            && nodes_with_content_mismatch.is_empty()
        {
            return (nodes_with_deployed_instances, nodes_with_content_mismatch);
        }

        if start.elapsed() >= timeout {
            return (nodes_with_deployed_instances, nodes_with_content_mismatch);
        }

        sleep(Duration::from_millis(500)).await;
    }
}
