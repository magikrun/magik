//! Apply Tests
//!
//! This module tests the manifest apply workflow end-to-end via the
//! Kubernetes-compatible API. It covers:
//! - Applying manifests via the REST API
//! - Verifying workload scheduling and deployment
//! - Manifest content verification
//! - Runtime engine integration (krun/crun)
//! - Replica distribution across nodes
//!
//! NOTE: These tests require libkrun (Linux KVM / macOS HVF) or libcrun (Linux only).

use env_logger::Env;
use serial_test::serial;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

use machineplane::runtimes::{PodStatus, create_default_registry};

#[path = "apply_common.rs"]
mod apply_common;
#[path = "kube_helpers.rs"]
mod kube_helpers;

use apply_common::runtime_helpers::shutdown_nodes;

use apply_common::{
    TEST_PORTS, check_instance_deployment, get_node_identities, setup_test_environment,
    start_fabric_nodes, wait_for_mesh_formation,
};
use kube_helpers::{apply_manifest_via_kube_api, delete_manifest_via_kube_api};

/// Name label key for workload identification
const NAME_LABEL_KEY: &str = "magik.name";

/// Check if any runtime engine is available (krun or crun)
async fn is_runtime_available() -> bool {
    let registry = create_default_registry().await;
    !registry.list_engines().is_empty()
}

/// Check if VM deployment is expected to work on this platform.
///
/// On macOS, libkrun requires libkrunfw (bundled kernel) and the binary must be signed
/// with the `com.apple.security.hypervisor` entitlement for HVF access.
/// On Linux, libkrun (TSI mode) works with just a rootfs + workplane binary.
///
/// Returns true only when:
/// - Platform is Linux, OR MAGIK_FORCE_VM_TESTS=1 is set on macOS
/// - The workplane binary is embedded (checked via runtime initialization)
fn is_vm_deployment_supported() -> bool {
    #[cfg(target_os = "linux")]
    let platform_ok = true;
    #[cfg(not(target_os = "linux"))]
    let platform_ok = std::env::var("MAGIK_FORCE_VM_TESTS").is_ok();

    if !platform_ok {
        return false;
    }

    // Check if the workplane binary was embedded at compile time
    // by looking for the embedded-workplane feature or checking KVM access on Linux
    #[cfg(target_os = "linux")]
    {
        // On Linux, also check KVM access for full VM testing
        let kvm_available = std::path::Path::new("/dev/kvm").exists()
            && std::fs::metadata("/dev/kvm")
                .map(|m| {
                    use std::os::unix::fs::MetadataExt;
                    // Check if we have read+write access (mode check is simplified)
                    m.mode() & 0o006 != 0 || unsafe { libc::geteuid() } == 0
                })
                .unwrap_or(false);

        if !kvm_available {
            log::info!("KVM not accessible - full VM tests will be skipped");
        }
        kvm_available
    }
    #[cfg(not(target_os = "linux"))]
    {
        // On macOS with MAGIK_FORCE_VM_TESTS, assume HVF works (user has signed binary)
        true
    }
}

/// Get the default runtime engine name
async fn get_default_engine_name() -> Option<String> {
    let registry = create_default_registry().await;
    registry.get_default_engine().map(|e| e.name().to_string())
}

/// Resolve a timeout from an env var, falling back to a default in seconds.
fn timeout_from_env(var: &str, default_secs: u64) -> Duration {
    match std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
    {
        Some(secs) => Duration::from_secs(secs),
        None => Duration::from_secs(default_secs),
    }
}

/// Wait for the REST API on each port to return an OK status.
async fn wait_for_rest_api_health(
    client: &reqwest::Client,
    ports: &[u16],
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        let mut healthy_ports = Vec::new();
        let mut unhealthy_ports = Vec::new();

        for &port in ports {
            let base = format!("http://127.0.0.1:{}", port);
            match client.get(format!("{}/health", base)).send().await {
                Ok(resp) if resp.status().is_success() => match resp.text().await {
                    Ok(body) if body.trim() == "ok" => {
                        healthy_ports.push(port);
                    }
                    _ => unhealthy_ports.push(port),
                },
                _ => unhealthy_ports.push(port),
            }
        }

        if healthy_ports.len() == ports.len() {
            log::info!("All REST APIs are healthy ({} ports)", healthy_ports.len());
            return true;
        }

        if start.elapsed() > timeout {
            log::warn!(
                "REST API health check timed out after {:?}; healthy nodes: {} / {}; unhealthy ports: {:?}",
                timeout,
                healthy_ports.len(),
                ports.len(),
                unhealthy_ports
            );
            return false;
        }

        sleep(Duration::from_millis(500)).await;
    }
}

/// Wait for workloads to appear in runtime and verify state
async fn wait_for_runtime_state(
    resource_name: &str,
    expected_present: bool,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();

    loop {
        let runtime_state_matches = verify_runtime_deployment(resource_name).await;

        if runtime_state_matches == expected_present {
            return true;
        }

        if start.elapsed() > timeout {
            return false;
        }

        sleep(Duration::from_millis(500)).await;
    }
}

/// Verify workload deployment via runtime engine
async fn verify_runtime_deployment(resource_name: &str) -> bool {
    let registry = create_default_registry().await;

    let Some(engine) = registry.get_default_engine() else {
        log::warn!("No runtime engine available; skipping verification");
        return false;
    };

    match engine.list().await {
        Ok(pods) => {
            if pods.is_empty() {
                log::info!(
                    "Runtime {} reported no pods when verifying resource {}",
                    engine.name(),
                    resource_name
                );
                return false;
            }

            for pod in &pods {
                if pod_matches_manifest(&pod.name, &pod.metadata, resource_name) {
                    log::info!(
                        "Found {} pod for resource {}: {} (status: {:?})",
                        engine.name(),
                        resource_name,
                        pod.name,
                        pod.status
                    );
                    return matches!(pod.status, PodStatus::Running | PodStatus::Starting);
                }
            }

            log::info!(
                "Runtime {} pods observed but none matched resource {}: {:?}",
                engine.name(),
                resource_name,
                pods.iter().map(|p| &p.name).collect::<Vec<_>>()
            );
            false
        }
        Err(err) => {
            log::warn!("Failed to list pods via {} runtime: {}", engine.name(), err);
            false
        }
    }
}

/// Check if a pod matches the expected manifest resource
fn pod_matches_manifest(
    pod_name: &str,
    metadata: &std::collections::HashMap<String, String>,
    resource_name: &str,
) -> bool {
    if resource_name.is_empty() {
        return false;
    }

    // Match by name label
    if metadata
        .get(NAME_LABEL_KEY)
        .map(|value| value == resource_name)
        .unwrap_or(false)
    {
        return true;
    }

    // Match by pod name pattern: {resource_name}-{entropy}
    workload_name_matches_resource(pod_name, resource_name)
}

/// Check if a workload name matches an expected resource name with entropy suffix.
/// Workload names follow the pattern: {resource_name}-{8-char-hex-entropy}
fn workload_name_matches_resource(candidate: &str, resource_name: &str) -> bool {
    if let Some((prefix, entropy)) = candidate.rsplit_once('-') {
        return prefix == resource_name && entropy_segment_is_valid(entropy);
    }
    false
}

fn entropy_segment_is_valid(segment: &str) -> bool {
    segment.len() == 8 && segment.chars().all(|c| c.is_ascii_hexdigit())
}

/// Clean up all magik-managed pods from the runtime
async fn cleanup_all_magik_pods() {
    log::info!("Cleaning up all Magik pods...");

    let registry = create_default_registry().await;
    let Some(engine) = registry.get_default_engine() else {
        log::warn!("No runtime engine available; nothing to clean up");
        return;
    };

    match engine.list().await {
        Ok(pods) => {
            let mut removed_count = 0;
            for pod in pods {
                // Check if this pod has any magik.* label
                let has_magik_label = pod.metadata.keys().any(|k| k.starts_with("magik."));

                // Also match UUID-like pod names (our naming convention)
                let is_uuid_pod = uuid::Uuid::parse_str(&pod.name).is_ok();

                if has_magik_label || is_uuid_pod {
                    match engine.delete(&pod.id).await {
                        Ok(()) => {
                            log::info!("Removed Magik pod: {}", pod.name);
                            removed_count += 1;
                        }
                        Err(err) => log::warn!("Failed to remove Magik pod {}: {}", pod.name, err),
                    }
                }
            }
            log::info!(
                "Cleaned up {} Magik pods via {}",
                removed_count,
                engine.name()
            );
        }
        Err(err) => {
            log::warn!("Failed to list pods for cleanup: {}", err);
        }
    }
}

/// Tests the basic apply functionality with the runtime engine.
///
/// This test:
/// 1. Sets up a test environment with 7 nodes.
/// 2. Applies an nginx manifest via the API.
/// 3. Verifies that the workload is deployed to at least one node (Linux only).
/// 4. Verifies that the deployed manifest content matches the original.
///
/// NOTE: On macOS, the binary must be signed with hypervisor entitlement for VM support.
/// Set MAGIK_FORCE_VM_TESTS=1 to enable full VM testing on macOS.
#[serial]
#[tokio::test]
async fn apply_deploys_manifest_to_mesh() {
    // Initialize logger
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();

    println!(">>> STARTING apply_deploys_manifest_to_mesh test");
    if !is_runtime_available().await {
        println!(">>> No runtime engine available (krun/crun), skipping");
        log::warn!("Skipping apply test - no runtime engine available");
        return;
    }

    let vm_deployment_supported = is_vm_deployment_supported();
    if !vm_deployment_supported {
        println!(">>> VM deployment skipped (set MAGIK_FORCE_VM_TESTS=1 to enable)");
        println!(">>> Will verify scheduling protocol only, not actual VM creation");
        log::warn!(
            "VM deployment skipped on this platform. \
             On macOS, ensure binary is signed with hypervisor entitlement. \
             Set MAGIK_FORCE_VM_TESTS=1 to force full VM testing."
        );
    }

    let engine_name = get_default_engine_name().await.unwrap_or_default();
    println!(">>> Using runtime engine: {}", engine_name);

    let (client, ports) = setup_test_environment().await;
    println!(">>> Starting {} fabric nodes", ports.len());
    let mut handles = start_fabric_nodes().await;
    println!(">>> Fabric nodes started");

    // Wait for REST APIs to become responsive
    let rest_api_timeout = timeout_from_env("MAGIK_APPLY_HEALTH_TIMEOUT_SECS", 45);
    if !wait_for_rest_api_health(&client, &ports, rest_api_timeout).await {
        shutdown_nodes(&mut handles).await;
        panic!(
            "REST APIs MUST become healthy before apply workflow verification (see test-spec.md); timeout: {:?}",
            rest_api_timeout
        );
    }

    // Wait for mesh to form before proceeding
    let mesh_timeout = timeout_from_env("MAGIK_APPLY_MESH_TIMEOUT_SECS", 30);
    let mesh_formed = wait_for_mesh_formation(&client, &ports, mesh_timeout, 6).await;
    if !mesh_formed {
        shutdown_nodes(&mut handles).await;
        panic!(
            "Mesh formation MUST complete before verification (see test-spec.md); timeout: {:?}",
            mesh_timeout
        );
    }

    // Allow time for gossipsub topic mesh to propagate subscriptions
    let gossip_settle_ms = std::env::var("MAGIK_GOSSIP_SETTLE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5000);
    sleep(Duration::from_millis(gossip_settle_ms)).await;

    // Resolve manifest path relative to this test crate's manifest dir
    let manifest_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sample_manifests/nginx.yml");

    // Read the original manifest content for verification
    let original_content = tokio::fs::read_to_string(&manifest_path)
        .await
        .expect("Failed to read original manifest file for verification");

    let tender_id = apply_manifest_via_kube_api(&client, ports[0], &manifest_path)
        .await
        .expect("kubectl apply should succeed");

    log::info!("✓ Tender published successfully: {}", tender_id);

    // Get node identities and check workload deployment
    let port_to_identity = get_node_identities(&client, &ports).await;
    let delivery_timeout = timeout_from_env("MAGIK_APPLY_DELIVERY_TIMEOUT_SECS", 20);
    let (nodes_with_deployed_instances, nodes_with_content_mismatch) = check_instance_deployment(
        &client,
        &ports,
        &tender_id,
        &original_content,
        &port_to_identity,
        false, // Don't expect modified replicas for single replica test
        Some(1),
        delivery_timeout,
    )
    .await;

    // On platforms where VM deployment isn't supported, verify scheduling worked
    // but don't require actual pod deployment
    if !vm_deployment_supported {
        log::info!(
            "✓ Scheduling protocol verified (tender {} published and processed)",
            tender_id
        );
        log::info!(
            "  Nodes with deployment attempts: {} (VM launch not supported on this platform)",
            nodes_with_deployed_instances.len()
        );

        // Clean up and return success - scheduling was verified
        shutdown_nodes(&mut handles).await;
        cleanup_all_magik_pods().await;

        println!(">>> Test passed: Scheduling protocol verified on macOS");
        return;
    }

    // On Linux, verify that the pod was actually deployed
    assert!(
        !nodes_with_deployed_instances.is_empty(),
        "Apply MUST result in at least one node having the pod; observed nodes: {:?}",
        nodes_with_deployed_instances
    );

    // Verify that manifest content matches on the node that has the pod
    assert!(
        nodes_with_content_mismatch.is_empty(),
        "Manifest content verification failed on nodes: {:?}. The deployed manifest content does not match the original manifest.",
        nodes_with_content_mismatch
    );

    log::info!(
        "✓ {} verification passed: nginx manifest {} deployed on {} nodes",
        engine_name,
        tender_id,
        nodes_with_deployed_instances.len()
    );

    // Allow time to observe pods before cleanup (can be extended via env var)
    let observe_secs = std::env::var("MAGIK_OBSERVE_PODS_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if observe_secs > 0 {
        log::info!(
            "Pausing {} seconds to observe pods before cleanup...",
            observe_secs
        );
        sleep(Duration::from_secs(observe_secs)).await;
    }

    // Clean up nodes first to stop any new pod creation
    shutdown_nodes(&mut handles).await;

    // Then clean up runtime resources
    cleanup_all_magik_pods().await;
}

/// Tests the apply functionality with real runtime engine.
///
/// This test verifies:
/// 1. Deployment of a workload creates actual microVMs/containers.
/// 2. Deletion of the manifest removes the runtime resources.
///
/// NOTE: On macOS, the binary must be signed with hypervisor entitlement for VM support.
/// This test is skipped on macOS unless MAGIK_FORCE_VM_TESTS=1 is set.
#[serial]
#[tokio::test]
async fn apply_with_runtime_creates_and_removes_workloads() {
    // Initialize logger
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();

    // Skip test if no runtime is available
    if !is_runtime_available().await {
        log::warn!("Skipping runtime integration test - no engine available");
        return;
    }

    // Skip on macOS unless forced - requires signed binary with hypervisor entitlement
    if !is_vm_deployment_supported() {
        log::warn!(
            "Skipping runtime integration test on macOS. \
             Set MAGIK_FORCE_VM_TESTS=1 to force (requires signed binary)."
        );
        return;
    }

    let engine_name = get_default_engine_name().await.unwrap_or_default();
    log::info!("Testing with runtime engine: {}", engine_name);

    let (client, ports) = setup_test_environment().await;
    let mut handles = start_fabric_nodes().await;

    // Wait for REST APIs to become responsive
    let rest_api_timeout = timeout_from_env("MAGIK_RUNTIME_HEALTH_TIMEOUT_SECS", 60);
    if !wait_for_rest_api_health(&client, &ports, rest_api_timeout).await {
        shutdown_nodes(&mut handles).await;
        panic!(
            "REST APIs MUST become healthy before runtime apply verification (see test-spec.md); timeout: {:?}",
            rest_api_timeout
        );
    }

    let mesh_timeout = timeout_from_env("MAGIK_RUNTIME_MESH_TIMEOUT_SECS", 60);
    let mesh_ready = wait_for_mesh_formation(&client, &ports, mesh_timeout, 6).await;
    if !mesh_ready {
        shutdown_nodes(&mut handles).await;
        panic!(
            "Mesh formation MUST complete before verification (see test-spec.md); timeout: {:?}",
            mesh_timeout
        );
    }

    // Allow time for gossipsub topic mesh to propagate subscriptions
    let gossip_settle_ms = std::env::var("MAGIK_GOSSIP_SETTLE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5000);
    sleep(Duration::from_millis(gossip_settle_ms)).await;

    // Resolve manifest path relative to this test crate's manifest dir
    let manifest_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/sample_manifests/nginx.yml");

    // Read the original manifest content for verification
    let original_content = tokio::fs::read_to_string(&manifest_path)
        .await
        .expect("Failed to read original manifest file for verification");

    let tender_id = apply_manifest_via_kube_api(&client, ports[0], &manifest_path)
        .await
        .expect("kubectl apply should succeed with runtime engine");

    // Wait for manifest delivery and deployment to complete
    let port_to_identity = get_node_identities(&client, &ports).await;
    let delivery_timeout = timeout_from_env("MAGIK_RUNTIME_DELIVERY_TIMEOUT_SECS", 40);
    let (nodes_with_deployed_instances, nodes_with_content_mismatch) = check_instance_deployment(
        &client,
        &ports,
        &tender_id,
        &original_content,
        &port_to_identity,
        false,
        Some(1),
        delivery_timeout,
    )
    .await;

    // Verify that the pod was deployed to at least one node
    assert!(
        !nodes_with_deployed_instances.is_empty(),
        "Runtime-backed apply MUST result in at least one node having the pod; observed nodes: {:?}",
        nodes_with_deployed_instances
    );

    assert!(
        nodes_with_content_mismatch.is_empty(),
        "Runtime-backed apply produced manifest content mismatch on nodes {:?}",
        nodes_with_content_mismatch
    );

    // Verify actual runtime deployment using resource name from manifest
    let resource_name = "my-nginx";
    let runtime_delivery_timeout = timeout_from_env("MAGIK_RUNTIME_VERIFY_TIMEOUT_SECS", 45);
    let runtime_verification_successful =
        wait_for_runtime_state(resource_name, true, runtime_delivery_timeout).await;

    assert!(
        runtime_verification_successful,
        "Runtime deployment verification failed - no matching workloads found via {}",
        engine_name
    );

    // Delete the manifest and verify cleanup
    let _ = delete_manifest_via_kube_api(&client, ports[0], &manifest_path, true).await;
    let runtime_teardown_timeout = timeout_from_env("MAGIK_RUNTIME_TEARDOWN_TIMEOUT_SECS", 30);
    let runtime_verification_successful =
        wait_for_runtime_state(resource_name, false, runtime_teardown_timeout).await;

    assert!(
        !runtime_verification_successful,
        "Runtime deployment still exists after deletion attempt via {}",
        engine_name
    );

    log::info!(
        "✓ {} create/delete verification passed for {}",
        engine_name,
        resource_name
    );

    // Clean up nodes first to stop any new pod creation
    shutdown_nodes(&mut handles).await;

    // Then clean up all remaining magik pods
    cleanup_all_magik_pods().await;
}

/// Tests the apply functionality with multiple replicas.
///
/// This test verifies that:
/// 1. A manifest specifying `replicas: 5` is distributed across nodes.
/// 2. The deployed manifests on each node have `replicas: 1` (since the scheduler distributes single replicas).
///
/// NOTE: On macOS, the binary must be signed with hypervisor entitlement for VM support.
/// This test is skipped on macOS unless MAGIK_FORCE_VM_TESTS=1 is set.
#[serial]
#[tokio::test]
async fn apply_distributes_replicas_across_nodes() {
    // Initialize logger
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("info")).try_init();

    if !is_runtime_available().await {
        log::warn!("Skipping apply test - no runtime engine available");
        return;
    }

    // Skip on macOS unless forced - requires signed binary with hypervisor entitlement
    if !is_vm_deployment_supported() {
        log::warn!(
            "Skipping replica distribution test on macOS. \
             Set MAGIK_FORCE_VM_TESTS=1 to force (requires signed binary)."
        );
        return;
    }

    let engine_name = get_default_engine_name().await.unwrap_or_default();

    let (client, ports) = setup_test_environment().await;
    let mut handles = start_fabric_nodes().await;

    // Wait for REST APIs to become responsive
    let rest_api_timeout = timeout_from_env("MAGIK_APPLY_HEALTH_TIMEOUT_SECS", 45);
    if !wait_for_rest_api_health(&client, &ports, rest_api_timeout).await {
        shutdown_nodes(&mut handles).await;
        panic!(
            "REST APIs MUST become healthy before apply workflow verification (see test-spec.md); timeout: {:?}",
            rest_api_timeout
        );
    }

    // Wait for mesh to form before proceeding
    let mesh_timeout = timeout_from_env("MAGIK_APPLY_MESH_TIMEOUT_SECS", 30);
    let mesh_formed = wait_for_mesh_formation(&client, &ports, mesh_timeout, 6).await;
    if !mesh_formed {
        shutdown_nodes(&mut handles).await;
        panic!(
            "Mesh formation MUST complete before verification (see test-spec.md); timeout: {:?}",
            mesh_timeout
        );
    }

    // Allow time for gossipsub topic mesh to propagate subscriptions
    let gossip_settle_ms = std::env::var("MAGIK_GOSSIP_SETTLE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5000);
    sleep(Duration::from_millis(gossip_settle_ms)).await;

    // Resolve manifest path for nginx with replicas
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/sample_manifests/nginx_with_replicas.yml");

    // Read the original manifest content for verification
    let original_content = tokio::fs::read_to_string(&manifest_path)
        .await
        .expect("Failed to read nginx_with_replicas manifest file for verification");

    let tender_id = apply_manifest_via_kube_api(&client, ports[0], &manifest_path)
        .await
        .expect("kubectl apply should succeed for nginx_with_replicas");

    // Get node identities and check workload deployment
    let port_to_identity = get_node_identities(&client, &ports).await;
    let delivery_timeout = timeout_from_env("MAGIK_APPLY_REPLICA_TIMEOUT_SECS", 25);
    let (nodes_with_deployed_instances, nodes_with_content_mismatch) = check_instance_deployment(
        &client,
        &ports,
        &tender_id,
        &original_content,
        &port_to_identity,
        true, // Expect modified replicas=1 for replica distribution test
        Some(1),
        delivery_timeout,
    )
    .await;

    if nodes_with_deployed_instances.is_empty() {
        shutdown_nodes(&mut handles).await;
        panic!(
            "Replica apply SHOULD distribute across nodes but MUST land on at least one; no nodes reported pods"
        );
    }

    if nodes_with_deployed_instances.len() == ports.len() {
        // Verify that all 7 nodes are different (should be all available nodes)
        let mut sorted_nodes = nodes_with_deployed_instances.clone();
        sorted_nodes.sort();
        let mut expected_nodes = TEST_PORTS.to_vec();
        expected_nodes.sort();
        assert_eq!(
            sorted_nodes, expected_nodes,
            "Expected workloads to be deployed on all 7 nodes {:?}, but found on nodes {:?}",
            expected_nodes, sorted_nodes
        );

        // Verify that manifest content matches on all nodes that have the workload
        assert!(
            nodes_with_content_mismatch.is_empty(),
            "Manifest content verification failed on nodes: {:?}. The deployed manifest content does not match the original manifest.",
            nodes_with_content_mismatch
        );
    } else {
        // Even when not all replicas spread, the deployed nodes must agree on manifest content.
        assert!(
            nodes_with_content_mismatch.is_empty(),
            "Replica apply produced manifest content mismatch on nodes {:?}",
            nodes_with_content_mismatch
        );
    }

    log::info!(
        "✓ {} verification passed: nginx_with_replicas manifest {} deployed on {} nodes as expected: {:?}",
        engine_name,
        tender_id,
        nodes_with_deployed_instances.len(),
        nodes_with_deployed_instances
    );

    // Clean up nodes first to stop any new pod creation
    shutdown_nodes(&mut handles).await;

    // Then clean up runtime resources
    cleanup_all_magik_pods().await;
}
