#![allow(dead_code)]

use machineplane::{DaemonConfig, start_machineplane as spawn_machineplane};
use reqwest::Client;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant, sleep};

/// Build a daemon configuration suitable for integration tests.
pub fn make_test_daemon(
    rest_api_port: u16,
    bootstrap_peers: Vec<String>,
    korium_port: u16,
) -> DaemonConfig {
    DaemonConfig {
        rest_api_host: "127.0.0.1".to_string(),
        rest_api_port,
        bootstrap_peer: bootstrap_peers,
        korium_port,
        signing_ephemeral: true,
        kem_ephemeral: true,
        ephemeral_keys: true,
        ..DaemonConfig::default()
    }
}

/// Start a list of magik machineplane given their configurations.
///
/// Returns `JoinHandle`s for spawned background tasks.
pub async fn start_nodes(
    daemons: Vec<DaemonConfig>,
    startup_delay: Duration,
) -> Vec<JoinHandle<()>> {
    let mut all_handles = Vec::new();
    for daemon in daemons {
        let rest_api_host = daemon.rest_api_host.clone();
        let rest_api_port = daemon.rest_api_port;
        let korium_host = daemon.korium_host.clone();
        let korium_port = daemon.korium_port;
        log::info!(
            "starting test daemon: REST http://{}:{}, Korium host {} port {}",
            rest_api_host,
            rest_api_port,
            korium_host,
            korium_port
        );
        match spawn_machineplane(daemon).await {
            Ok(mut handles) => {
                all_handles.append(&mut handles);
            }
            Err(e) => panic!("failed to start node: {e:?}"),
        }
        tokio::spawn(log_local_address(
            rest_api_host,
            rest_api_port,
            korium_host,
            korium_port,
        ));
        sleep(startup_delay).await;
    }
    all_handles
}

/// Wait for a daemon to report its local identity.
/// Returns a korium bootstrap string in format: `<identity>@<ip:port>`
pub async fn wait_for_local_multiaddr(
    rest_api_host: &str,
    rest_api_port: u16,
    korium_host: &str,
    korium_port: u16,
    timeout: Duration,
) -> Option<String> {
    let display_host = if korium_host == "0.0.0.0" {
        "127.0.0.1"
    } else {
        korium_host
    };

    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(identity) = fetch_local_identity(rest_api_host, rest_api_port).await {
            // Korium bootstrap format: <identity>@<ip:port>
            let address = format!(
                "{}@{}:{}",
                identity, display_host, korium_port
            );
            log::info!("detected local address with identity: {}", address);
            return Some(address);
        }

        sleep(Duration::from_millis(500)).await;
    }

    None
}

async fn fetch_local_identity(rest_api_host: &str, rest_api_port: u16) -> Option<String> {
    let base = format!("http://{}:{}", rest_api_host, rest_api_port);
    let client = Client::new();
    let request = client
        .get(format!("{}/debug/local_identity", base))
        .timeout(Duration::from_secs(5));

    match request.send().await {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(Value::Object(map)) if map.get("ok").and_then(|v| v.as_bool()) == Some(true) => map
                .get("local_identity")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            Ok(_) => None,
            Err(_) => None,
        },
        Err(_) => None,
    }
}

async fn log_local_address(
    rest_api_host: String,
    rest_api_port: u16,
    korium_host: String,
    korium_port: u16,
) {
    let display_host = if korium_host == "0.0.0.0" {
        "127.0.0.1".to_string()
    } else {
        korium_host.clone()
    };

    let base = format!("http://{}:{}", rest_api_host, rest_api_port);
    let client = Client::new();
    let request = client
        .get(format!("{}/debug/local_identity", base))
        .timeout(Duration::from_secs(5));

    match request.send().await {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(Value::Object(map)) => {
                if map.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                    if let Some(identity) = map.get("local_identity").and_then(|v| v.as_str()) {
                        // Korium bootstrap format: <identity>@<ip:port>
                        let korium_address = format!(
                            "{}@{}:{}",
                            identity, display_host, korium_port
                        );
                        log::info!(
                            "machineplane node at {} reports Korium address {}",
                            base,
                            korium_address
                        );
                        return;
                    }
                }
                log::warn!("machineplane node at {} did not return an identity", base);
            }
            Ok(_) => {
                log::warn!("machineplane node at {} returned unexpected JSON", base);
            }
            Err(err) => {
                log::warn!(
                    "failed to parse local_identity response from {}: {}",
                    base,
                    err
                );
            }
        },
        Err(err) => {
            log::warn!("failed to fetch local_identity from {}: {}", base, err);
        }
    }
}

/// Abort all spawned node tasks.
pub async fn shutdown_nodes(handles: &mut Vec<JoinHandle<()>>) {
    for handle in handles.drain(..) {
        handle.abort();
    }
}
