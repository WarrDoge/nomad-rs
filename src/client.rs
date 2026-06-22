// SPDX-License-Identifier: Apache-2.0

//! Nomad client agent — manages task execution on a node.
//!
//! The client communicates with Nomad servers to receive allocations,
//! runs tasks using drivers, and reports back status.

use crate::config::Config;
use crate::error::Result;

/// A Nomad client agent that manages local task execution.
#[derive(Debug)]
pub struct Client {
    /// Client configuration.
    #[allow(dead_code, reason = "read once the client lifecycle is implemented")]
    config: Config,
}

impl Client {
    /// Create a new client with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Start the client agent. This blocks until the client is stopped.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to initialise or encounters a
    /// fatal runtime error.
    #[allow(clippy::unused_async, reason = "awaits driver I/O once implemented")]
    pub async fn run(&mut self) -> Result<()> {
        todo!("run the client agent lifecycle: fingerprint, register, pull allocs, run tasks")
    }
}
