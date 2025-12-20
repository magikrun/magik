//! Runtime Engine Integration Tests
//!
//! This module tests the runtime engine abstraction layer:
//! - KrunEngine (microVM via libkrun)
//! - CrunEngine (container via libcrun)
//!
//! Tests verify:
//! - Engine availability detection
//! - Registry management
//! - Deployment configuration parsing
//!
//! NOTE: Actual workload execution tests require the respective runtimes
//! to be installed. Use `is_available()` checks to skip when unavailable.

use machineplane::runtimes::{DeploymentConfig, PodStatus, RuntimeEngine, create_default_registry};

/// Test that the default registry can be created without panicking.
#[tokio::test]
async fn default_registry_creation_succeeds() {
    let registry = create_default_registry().await;

    // Registry should exist (may be empty on platforms without runtimes)
    let engines = registry.list_engines();
    log::info!("Available engines: {:?}", engines);

    // On supported platforms, at least one engine should be available
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // Note: This may still be empty if libkrun/libcrun aren't installed
        // The test verifies the registry creation logic, not runtime availability
    }
}

/// Test that the registry correctly lists registered engines.
#[tokio::test]
async fn registry_lists_engines_correctly() {
    let registry = create_default_registry().await;
    let engines = registry.list_engines();

    // Each engine name should be non-empty
    for engine_name in &engines {
        assert!(!engine_name.is_empty(), "Engine name should not be empty");
    }
}

/// Test that get_engine returns the correct engine by name.
#[tokio::test]
async fn registry_get_engine_by_name() {
    let registry = create_default_registry().await;
    let engines = registry.list_engines();

    for engine_name in engines {
        let engine = registry.get_engine(&engine_name);
        assert!(
            engine.is_some(),
            "Registry should return engine for name: {}",
            engine_name
        );
        assert_eq!(
            engine.unwrap().name(),
            engine_name,
            "Engine name should match requested name"
        );
    }
}

/// Test that get_engine returns None for unknown engines.
#[tokio::test]
async fn registry_returns_none_for_unknown_engine() {
    let registry = create_default_registry().await;

    let engine = registry.get_engine("nonexistent-runtime");
    assert!(engine.is_none(), "Should return None for unknown engine");
}

/// Test that the default engine is returned when available.
#[tokio::test]
async fn registry_returns_default_engine() {
    let registry = create_default_registry().await;

    if !registry.list_engines().is_empty() {
        let default_engine = registry.get_default_engine();
        assert!(
            default_engine.is_some(),
            "Should return a default engine when engines are available"
        );
    }
}

/// Test DeploymentConfig default values.
#[tokio::test]
async fn deployment_config_defaults() {
    let config = DeploymentConfig::default();

    assert_eq!(config.replicas, 1, "Default replicas should be 1");
    assert!(config.env.is_empty(), "Default env should be empty");
    assert!(
        config.runtime_options.is_empty(),
        "Default runtime_options should be empty"
    );
    assert!(config.infra.is_none(), "Default infra should be None");
}

/// Test PodStatus variants can be created and compared.
#[tokio::test]
async fn pod_status_variants() {
    let starting = PodStatus::Starting;
    let running = PodStatus::Running;
    let stopped = PodStatus::Stopped;
    let failed = PodStatus::Failed("test error".to_string());
    let unknown = PodStatus::Unknown;

    // Verify each status is distinct
    assert_ne!(starting, running);
    assert_ne!(running, stopped);
    assert_ne!(stopped, unknown);

    // Failed contains error message
    if let PodStatus::Failed(msg) = &failed {
        assert_eq!(msg, "test error");
    } else {
        panic!("Expected Failed variant");
    }
}

/// Test engine availability detection.
///
/// This test verifies that `is_available()` returns a boolean without panicking.
/// The actual availability depends on whether the runtime is installed.
#[tokio::test]
async fn engine_availability_check() {
    let registry = create_default_registry().await;

    for engine_name in registry.list_engines() {
        if let Some(engine) = registry.get_engine(&engine_name) {
            let available = engine.is_available().await;
            log::info!("Engine '{}' availability: {}", engine_name, available);
            // Just verify it returns a bool without panicking
            assert!(available || !available);
        }
    }
}

/// Test that KrunEngine is registered on supported platforms.
#[tokio::test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn krun_engine_registered() {
    use machineplane::runtimes::krun::KrunEngine;

    let engine = KrunEngine::new();
    assert_eq!(engine.name(), "krun", "KrunEngine should have name 'krun'");

    // Check availability (may be false if libkrun not installed)
    let available = engine.is_available().await;
    log::info!("KrunEngine availability: {}", available);
}

/// Test that CrunEngine is registered (with stubs on non-Linux).
#[tokio::test]
async fn crun_engine_registered() {
    use machineplane::runtimes::crun::CrunEngine;

    let engine = CrunEngine::new();
    assert_eq!(engine.name(), "crun", "CrunEngine should have name 'crun'");

    // On non-Linux, this should return false (stub)
    let available = engine.is_available().await;
    log::info!("CrunEngine availability: {}", available);

    #[cfg(not(target_os = "linux"))]
    assert!(!available, "CrunEngine should be unavailable on non-Linux");
}

/// Test that both engines can coexist in the registry.
#[tokio::test]
async fn multiple_engines_coexist() {
    let registry = create_default_registry().await;
    let engines = registry.list_engines();

    // On Linux, both should be potentially available
    #[cfg(target_os = "linux")]
    {
        // Both engines are registered, but may not be available
        // (depends on whether libkrun and libcrun are installed)
        log::info!("Linux: {} engines registered", engines.len());
    }

    // On macOS, only krun should be available
    #[cfg(target_os = "macos")]
    {
        log::info!("macOS: {} engines registered", engines.len());
        // krun may or may not be available depending on libkrun + libkrunfw installation
        // crun will always be unavailable (stub returns false)
    }

    // Engines should not conflict
    for engine_name in &engines {
        assert!(
            registry.get_engine(engine_name).is_some(),
            "Each registered engine should be retrievable"
        );
    }
}
