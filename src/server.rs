// SPDX-License-Identifier: Apache-2.0

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
    #[allow(dead_code, reason = "read once the server lifecycle is implemented")]
    config: Config,
}

impl Server {
    /// Create a new server with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Start the server. This blocks until the server is stopped.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to initialise or encounters a
    /// fatal runtime error.
    #[allow(clippy::unused_async, reason = "awaits Raft/RPC once implemented")]
    pub async fn run(&mut self) -> Result<()> {
        todo!("run the server lifecycle: bootstrap Raft, serve RPC, drive scheduling")
    }
}
