// SPDX-License-Identifier: Apache-2.0

//! Nomad client agent — manages task execution on a node.
//!
//! The client communicates with Nomad servers to receive allocations,
//! runs tasks using drivers, and reports back status.

use crate::config::Config;
use crate::error::Result;

/// The possible states a Nomad client can be in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStatus {
    /// The client has been created but is not running.
    Initialized,
    /// The client is actively running.
    Running,
    /// The client has been stopped.
    Stopped,
    /// The client encountered a terminal error.
    Failed,
}

/// A Nomad client agent that manages local task execution.
#[derive(Debug)]
pub struct Client {
    /// Client configuration.
    config: Config,
    /// Current client status.
    status: ClientStatus,
}

impl Client {
    /// Create a new client with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self { config, status: ClientStatus::Initialized }
    }

    /// Returns the configuration this client was created with.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the current status of the client.
    #[must_use]
    pub fn status(&self) -> ClientStatus {
        self.status
    }

    /// Returns `true` if the client is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == ClientStatus::Running
    }

    /// Start the client agent. This method transitions the client
    /// into the running state.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to initialise or encounters a
    /// fatal runtime error.
    #[allow(clippy::unused_async)]
    pub async fn run(&mut self) -> Result<()> {
        if self.status == ClientStatus::Running {
            return Ok(());
        }
        self.status = ClientStatus::Running;
        tracing::info!("client starting");
        // TODO: implement full client-agent lifecycle
        Ok(())
    }

    /// Gracefully stop the client agent.
    pub fn stop(&mut self) {
        self.status = ClientStatus::Stopped;
        tracing::info!("client stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::block_on;

    fn test_config() -> Config {
        Config { node_name: "test-client".to_owned(), ..Config::default() }
    }

    #[test]
    fn test_client_new() {
        let config = test_config();
        let client = Client::new(config.clone());
        assert_eq!(client.status(), ClientStatus::Initialized);
        assert!(!client.is_running());
        assert_eq!(*client.config(), config);
    }

    #[test]
    fn test_client_config_accessor() {
        let client = Client::new(test_config());
        assert_eq!(client.config().node_name, "test-client");
    }

    #[test]
    fn test_client_run() {
        let mut client = Client::new(test_config());
        assert!(client.status() == ClientStatus::Initialized);
        let result = block_on(client.run());
        assert!(result.is_ok());
        assert!(client.is_running());
        assert_eq!(client.status(), ClientStatus::Running);
    }

    #[test]
    fn test_client_run_idempotent() {
        let mut client = Client::new(test_config());
        let _ = block_on(client.run());
        assert!(client.is_running());
        let result = block_on(client.run());
        assert!(result.is_ok());
        assert!(client.is_running());
    }

    #[test]
    fn test_client_stop() {
        let mut client = Client::new(test_config());
        let _ = block_on(client.run());
        assert!(client.is_running());
        client.stop();
        assert_eq!(client.status(), ClientStatus::Stopped);
        assert!(!client.is_running());
    }

    #[test]
    fn test_client_stop_before_run() {
        let mut client = Client::new(test_config());
        assert_eq!(client.status(), ClientStatus::Initialized);
        client.stop();
        assert_eq!(client.status(), ClientStatus::Stopped);
    }
}
