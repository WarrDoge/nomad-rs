// SPDX-License-Identifier: Apache-2.0

//! Nomad-rs binary entrypoint.
//!
//! Supports subcommands:
//! - `nomad-rs agent` — run a client and/or server agent
//! - `nomad-rs server` — run a server-only agent
//! - `nomad-rs client` — run a client-only agent

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::path::PathBuf;

use clap::Parser;
use nomad_rs::agent::Agent;
use nomad_rs::config::Config;
use nomad_rs::error::Result;

/// Nomad-rs: a Nomad rewrite in Rust.
#[derive(Debug, Parser)]
#[command(name = "nomad-rs", version, about)]
struct Cli {
    /// Path to the configuration file (TOML).
    #[arg(short = 'c', long = "config", value_name = "FILE")]
    config_file: Option<PathBuf>,

    /// Data directory.
    #[arg(long, value_name = "DIR")]
    data_dir: Option<PathBuf>,

    /// Log directory.
    #[arg(long, value_name = "DIR")]
    log_dir: Option<PathBuf>,

    /// Log level (error, warn, info, debug, trace).
    #[arg(long, value_name = "LEVEL")]
    log_level: Option<String>,

    /// Bind address.
    #[arg(long, value_name = "ADDR")]
    bind_addr: Option<String>,

    /// Node name.
    #[arg(long, value_name = "NAME")]
    node_name: Option<String>,

    /// Region.
    #[arg(long, value_name = "REGION")]
    region: Option<String>,

    /// Subcommand (defaults to `agent` if omitted).
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Parser)]
enum Command {
    /// Run a client agent.
    Client,
    /// Run a server agent.
    Server,
    /// Run a client and/or server agent (default).
    Agent,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    // Build config: defaults → file → env vars → CLI flags
    let config = if let Some(ref path) = cli.config_file { Config::from_file(path)? } else { Config::default() };
    let config = config.merge_env().merge_cli(
        cli.data_dir,
        cli.log_dir,
        cli.log_level.as_deref().and_then(nomad_rs::config::LogLevel::parse),
        cli.bind_addr,
        cli.node_name,
        cli.region,
    );

    // Validate before starting
    config.validate()?;

    // Setup logging
    let _guard = nomad_rs::logging::init(&config);

    // Determine mode
    let mode = cli.command.unwrap_or(Command::Agent);

    match mode {
        Command::Agent | Command::Client | Command::Server => {
            let mut agent = Agent::new(config);
            agent.run().await?;
        },
    }

    Ok(())
}
