//! CLI Tests
//!
//! This module tests CLI argument parsing and environment variable handling.
//!
//! Note: Previous CONTAINER_HOST environment variable test was removed when
//! Magik transitioned to libkrun microVM runtime exclusively.
//! Container socket configuration is no longer applicable.

use clap::Parser;
use machineplane::Cli;

/// Verifies that default CLI values are properly set.
#[test]
fn default_cli_values() {
    let cli = Cli::parse_from(["magik-machineplane"]);

    // REST API defaults
    assert_eq!(cli.rest_api_host, "0.0.0.0");
    assert_eq!(cli.rest_api_port, 3000);

    // Korium transport defaults
    assert_eq!(cli.korium_host, "0.0.0.0");
    assert_eq!(cli.korium_port, 0); // 0 = auto-assign

    // Bootstrap peers default to empty
    assert!(cli.bootstrap_peer.is_empty());

    // Ephemeral keys default to false (production mode)
    assert!(!cli.ephemeral_keys);
}
