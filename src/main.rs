// SPDX-License-Identifier: Apache-2.0

//! Nomad-rs binary entrypoint.
//!
//! Supports subcommands:
//! - `nomad-rs agent` — run a client and/or server agent
//! - `nomad-rs server` — run a server-only agent
//! - `nomad-rs client` — run a client-only agent

#![forbid(unsafe_code)]
#![deny(missing_docs)]
// Startup errors need stderr output before logging init.
#![allow(clippy::print_stderr)]

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

/// CLI subcommands for the Nomad agent.
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
    let cli = Cli::parse();

    // Build config: defaults → file → env vars → CLI flags
    let config = match cli.config_file {
        Some(ref path) => match Config::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                // No logging subsystem yet — eprintln before init
                eprintln!("error: failed to load config: {e}");
                std::process::exit(1);
            },
        },
        None => Config::default(),
    };
    let config = config.merge_env().merge_cli(
        cli.data_dir,
        cli.log_dir,
        cli.log_level.as_deref().and_then(nomad_rs::config::LogLevel::parse),
        cli.bind_addr,
        cli.node_name,
        cli.region,
    );

    // Validate before starting
    if let Err(e) = config.validate() {
        eprintln!("error: invalid config: {e}");
        std::process::exit(1);
    }

    // Init logging subsystem now — all errors from here on are logged
    let _guard = nomad_rs::logging::init(&config);

    if let Err(e) = run(config, cli.command.unwrap_or(Command::Agent)).await {
        tracing::error!("fatal error: {e}");
        std::process::exit(1);
    }
}

/// Build and run the agent.
async fn run(config: Config, command: Command) -> Result<()> {
    match command {
        Command::Agent | Command::Client | Command::Server => {
            let mut agent = Agent::new(config);
            agent.run().await?;
        },
    }

    Ok(())
}
