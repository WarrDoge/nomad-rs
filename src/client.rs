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
    config: Config,
    /// Whether the client is currently running.
    running: bool,
}

impl Client {
    /// Create a new client with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            running: false,
        }
    }

    /// Start the client agent. This blocks until the client is stopped.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to initialise or encounters a
    /// fatal runtime error.
    pub async fn run(&mut self) -> Result<()> {
        self.running = true;
        tracing::info!("client starting");
        // TODO: implement client agent lifecycle
        self.running = false;
        Ok(())
    }
}
