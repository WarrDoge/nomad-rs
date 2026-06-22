// SPDX-License-Identifier: Apache-2.0

//! Nomad server — cluster management, scheduling, and state.
//!
//! The server handles cluster leadership, job registration, scheduler
//! evaluations, and maintains cluster state via Raft consensus.

use crate::config::Config;
use crate::error::Result;

/// The possible states a Nomad server can be in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerStatus {
    /// The server has been created but is not running.
    Initialized,
    /// The server is actively running.
    Running,
    /// The server has been stopped.
    Stopped,
    /// The server encountered a terminal error.
    Failed,
}

/// A Nomad server node in the cluster.
#[derive(Debug)]
pub struct Server {
    /// Server configuration.
    config: Config,
    /// Current server status.
    status: ServerStatus,
}

impl Server {
    /// Create a new server with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self { config, status: ServerStatus::Initialized }
    }

    /// Returns the configuration this server was created with.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the current status of the server.
    #[must_use]
    pub fn status(&self) -> ServerStatus {
        self.status
    }

    /// Returns `true` if the server is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == ServerStatus::Running
    }

    /// Start the server. This method transitions the server into the
    /// running state.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails to initialise or encounters a
    /// fatal runtime error.
    #[allow(clippy::unused_async)]
    pub async fn run(&mut self) -> Result<()> {
        if self.status == ServerStatus::Running {
            return Ok(());
        }
        self.status = ServerStatus::Running;
        tracing::info!("server starting");
        // TODO: implement server lifecycle (Raft, RPC, scheduling)
        Ok(())
    }

    /// Gracefully stop the server.
    pub fn stop(&mut self) {
        self.status = ServerStatus::Stopped;
        tracing::info!("server stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::block_on;

    fn test_config() -> Config {
        Config { node_name: "test-server".to_owned(), bind_addr: "0.0.0.0:4647".to_owned(), ..Config::default() }
    }

    #[test]
    fn test_server_new() {
        let config = test_config();
        let server = Server::new(config.clone());
        assert_eq!(server.status(), ServerStatus::Initialized);
        assert!(!server.is_running());
        assert_eq!(*server.config(), config);
    }

    #[test]
    fn test_server_config_accessor() {
        let server = Server::new(test_config());
        assert_eq!(server.config().bind_addr, "0.0.0.0:4647");
    }

    #[test]
    fn test_server_run() {
        let mut server = Server::new(test_config());
        assert_eq!(server.status(), ServerStatus::Initialized);
        let result = block_on(server.run());
        assert!(result.is_ok());
        assert!(server.is_running());
        assert_eq!(server.status(), ServerStatus::Running);
    }

    #[test]
    fn test_server_run_idempotent() {
        let mut server = Server::new(test_config());
        let _ = block_on(server.run());
        assert!(server.is_running());
        let result = block_on(server.run());
        assert!(result.is_ok());
        assert!(server.is_running());
    }

    #[test]
    fn test_server_stop() {
        let mut server = Server::new(test_config());
        let _ = block_on(server.run());
        assert!(server.is_running());
        server.stop();
        assert_eq!(server.status(), ServerStatus::Stopped);
        assert!(!server.is_running());
    }

    #[test]
    fn test_server_stop_before_run() {
        let mut server = Server::new(test_config());
        assert_eq!(server.status(), ServerStatus::Initialized);
        server.stop();
        assert_eq!(server.status(), ServerStatus::Stopped);
    }

    #[test]
    fn test_server_bind_addr() {
        let server = Server::new(test_config());
        assert_eq!(server.config().bind_addr, "0.0.0.0:4647");
    }
}
