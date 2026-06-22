//! Nomad server — cluster management, scheduling, and state.
//!
//! The server handles cluster leadership, job registration, scheduler
//! evaluations, and maintains cluster state via Raft consensus.

use crate::config::Config;
use crate::error::Result;

/// A Nomad server node in the cluster.
#[derive(Debug)]
pub struct Server {
    /// Server configuration.
    config: Config,
    /// Whether the server is currently running.
    running: bool,
}

impl Server {
    /// Create a new server with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            running: false,
        }
    }

    /// Start the server. This blocks until the server is stopped.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to initialise or encounters a
    /// fatal runtime error.
    pub async fn run(&mut self) -> Result<()> {
        self.running = true;
        tracing::info!("server starting");
        // TODO: implement server lifecycle (Raft, RPC, scheduling)
        self.running = false;
        Ok(())
    }
}
