//! # Machineplane Binary
//!
//! Entry point for the machineplane daemon. Parses CLI arguments and starts
//! the control-plane services including the REST API and mesh networking.
//!
//! ## Usage
//! ```bash
//! machineplane --rest-api-port 3000 --korium-port 4000 --bootstrap-peer <id>@<ip:port>
//! ```

use clap::Parser;
use machineplane::{Cli, start_machineplane};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let handles = start_machineplane(cli).await?;
    if !handles.is_empty() {
        let _ = futures::future::join_all(handles).await;
    }
    Ok(())
}
