//! Build script for machineplane.
//!
//! Detects and embeds the workplane binary if available for the target platform.
//! The workplane binary is used as the infra container (replacing pause) in pods.

use std::env;
use std::path::PathBuf;

fn main() {
    // Determine the target architecture for the workplane binary
    let target = env::var("TARGET").unwrap_or_default();
    let host = env::var("HOST").unwrap_or_default();
    
    // We need the musl-linked binary for the container/VM
    // The workplane binary must match the architecture of the VM, not the host
    let workplane_target = if target.contains("linux") && target.contains("musl") {
        // Use the same target if it's already musl Linux
        target.clone()
    } else {
        // Determine target architecture based on host
        if host.contains("aarch64") || host.contains("arm64") {
            "aarch64-unknown-linux-musl".to_string()
        } else {
            "x86_64-unknown-linux-musl".to_string()
        }
    };

    // Look for the workplane binary in the target directory
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let workspace_root = PathBuf::from(&manifest_dir).parent().unwrap().to_path_buf();
    
    let binary_path = workspace_root
        .join("target")
        .join(&workplane_target)
        .join("release")
        .join("workplane");

    if binary_path.exists() {
        println!("cargo:rustc-cfg=feature=\"embedded-workplane\"");
        println!(
            "cargo:rustc-env=WORKPLANE_BINARY_PATH={}",
            binary_path.display()
        );
        println!(
            "cargo:warning=Embedding workplane binary from {}",
            binary_path.display()
        );
    } else {
        println!(
            "cargo:warning=Workplane binary not found at {}. \
             Build with: cargo build -p workplane --release --target {}",
            binary_path.display(),
            workplane_target
        );
    }

    // Rerun if the workplane binary changes
    println!("cargo:rerun-if-changed={}", binary_path.display());
    println!("cargo:rerun-if-changed=build.rs");
}
