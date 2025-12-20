//! # Init Supervisor
//!
//! This module provides init supervisor functionality for running workplane
//! as PID 1 inside a microVM (crun-krun, libkrun, etc.).
//!
//! When workplane runs as an init process, it:
//! - Spawns the workload command as a child process
//! - Reaps zombie processes (init responsibility)
//! - Forwards signals to the child process
//! - Exits with the child's exit code
//!
//! # Usage
//!
//! ```bash
//! workplane --pod my-app.default.abc123 --exec "nginx -g 'daemon off;'"
//! ```

use anyhow::{Context, Result};
use std::process::Stdio;
use tokio::process::{Child, Command};
use tokio::signal::unix::{SignalKind, signal};
use tracing::{debug, error, info, warn};

/// Parses a command string into program and arguments.
///
/// Handles basic shell-like quoting (single and double quotes).
/// Does not handle escape sequences or complex shell features.
fn parse_command(cmd: &str) -> Result<(String, Vec<String>)> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for ch in cmd.chars() {
        match ch {
            '\'' if !in_double_quote => {
                in_single_quote = !in_single_quote;
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
            }
            ' ' | '\t' if !in_single_quote && !in_double_quote => {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.is_empty() {
        parts.push(current);
    }

    if in_single_quote || in_double_quote {
        anyhow::bail!("Unclosed quote in command: {}", cmd);
    }

    let program = parts
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Empty command"))?;
    let args = parts.into_iter().skip(1).collect();

    Ok((program, args))
}

/// Spawns the workload process.
///
/// The child process inherits stdout/stderr for logging.
/// stdin is set to null to prevent blocking on input.
fn spawn_workload(program: &str, args: &[String]) -> Result<Child> {
    info!(program = %program, args = ?args, "Spawning workload process");

    let child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("Failed to spawn workload: {} {:?}", program, args))?;

    info!(pid = ?child.id(), "Workload process started");
    Ok(child)
}

/// Runs the init supervisor loop.
///
/// This function:
/// 1. Spawns the workload command
/// 2. Sets up signal handlers (SIGCHLD for zombie reaping, SIGTERM/SIGINT forwarding)
/// 3. Waits for the child to exit
/// 4. Returns the child's exit code
///
/// # Arguments
///
/// * `exec_command` - The command string to execute
///
/// # Returns
///
/// The exit code of the child process (0 if the child was killed by a signal).
pub async fn run_supervisor(exec_command: &str) -> Result<i32> {
    let (program, args) = parse_command(exec_command)?;

    let mut child = spawn_workload(&program, &args)?;
    let child_pid = child.id();

    // Set up signal handlers
    let mut sigterm =
        signal(SignalKind::terminate()).context("Failed to set up SIGTERM handler")?;
    let mut sigint = signal(SignalKind::interrupt()).context("Failed to set up SIGINT handler")?;
    let mut sigchld = signal(SignalKind::child()).context("Failed to set up SIGCHLD handler")?;

    info!(pid = ?child_pid, "Init supervisor running, waiting for child");

    loop {
        tokio::select! {
            // Child process exited
            status = child.wait() => {
                match status {
                    Ok(exit_status) => {
                        let code = exit_status.code().unwrap_or(1);
                        if exit_status.success() {
                            info!(code = code, "Workload exited successfully");
                        } else {
                            warn!(code = code, "Workload exited with error");
                        }
                        return Ok(code);
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to wait for workload");
                        return Ok(1);
                    }
                }
            }

            // SIGTERM received - forward to child
            _ = sigterm.recv() => {
                info!("Received SIGTERM, forwarding to workload");
                if let Some(pid) = child_pid {
                    // SAFETY: `pid` was obtained from `tokio::process::Child::id()` and represents
                    // a valid PID of a process we spawned. SIGTERM (15) is a valid signal number.
                    // If the process has already exited, kill() returns -1 which is benign here.
                    unsafe {
                        libc::kill(pid as i32, libc::SIGTERM);
                    }
                }
            }

            // SIGINT received - forward to child
            _ = sigint.recv() => {
                info!("Received SIGINT, forwarding to workload");
                if let Some(pid) = child_pid {
                    // SAFETY: `pid` was obtained from `tokio::process::Child::id()` and represents
                    // a valid PID of a process we spawned. SIGINT (2) is a valid signal number.
                    // If the process has already exited, kill() returns -1 which is benign here.
                    unsafe {
                        libc::kill(pid as i32, libc::SIGINT);
                    }
                }
            }

            // SIGCHLD received - reap zombies
            _ = sigchld.recv() => {
                debug!("Received SIGCHLD, reaping zombies");
                reap_zombies();
            }
        }
    }
}

/// Reaps all zombie child processes.
///
/// As PID 1 (init), we're responsible for reaping any orphaned processes
/// that get reparented to us.
fn reap_zombies() {
    loop {
        // SAFETY: waitpid(-1, NULL, WNOHANG) is a standard POSIX idiom for non-blocking
        // zombie reaping. Passing -1 waits for any child, NULL status pointer discards
        // exit info, WNOHANG makes it non-blocking. Returns 0 if no zombies, -1 on error.
        let result = unsafe { libc::waitpid(-1, std::ptr::null_mut(), libc::WNOHANG) };

        if result <= 0 {
            // No more zombies to reap (result == 0) or error (result < 0)
            break;
        }

        debug!(pid = result, "Reaped zombie process");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_command() {
        let (prog, args) = parse_command("nginx -g daemon off;").unwrap();
        assert_eq!(prog, "nginx");
        assert_eq!(args, vec!["-g", "daemon", "off;"]);
    }

    #[test]
    fn test_parse_quoted_command() {
        let (prog, args) = parse_command("nginx -g 'daemon off;'").unwrap();
        assert_eq!(prog, "nginx");
        assert_eq!(args, vec!["-g", "daemon off;"]);
    }

    #[test]
    fn test_parse_double_quoted_command() {
        let (prog, args) = parse_command(r#"sh -c "echo hello world""#).unwrap();
        assert_eq!(prog, "sh");
        assert_eq!(args, vec!["-c", "echo hello world"]);
    }

    #[test]
    fn test_parse_empty_command() {
        assert!(parse_command("").is_err());
    }

    #[test]
    fn test_parse_unclosed_quote() {
        assert!(parse_command("nginx -g 'daemon off;").is_err());
    }
}
