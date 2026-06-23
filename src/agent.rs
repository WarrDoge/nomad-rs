// SPDX-License-Identifier: Apache-2.0

//! Agent lifecycle — the top-level orchestrator that holds a [`crate::client::Client`]
//! and/or [`crate::server::Server`] process and drives their graceful lifecycle.

use tokio::signal::unix::{SignalKind, signal};

use crate::client::Client;
use crate::config::Config;
use crate::error::Result;
use crate::server::Server;

/// Shared lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentStatus {
    /// Created but not running.
    Initialized,
    /// Actively running.
    Running,
    /// Stopped.
    Stopped,
}

/// A Nomad agent that runs a client, a server, or both.
#[derive(Debug)]
pub struct Agent {
    /// Shared configuration.
    config: Config,
    /// Optional client agent.
    client: Option<Client>,
    /// Optional server agent.
    server: Option<Server>,
    /// Current agent status.
    status: AgentStatus,
}

impl Agent {
    /// Create a new agent from config. Sub-agents are created lazily when
    /// started.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self { config, client: None, server: None, status: AgentStatus::Initialized }
    }

    /// Run the agent: start sub-agents, then block until a shutdown signal.
    ///
    /// # Errors
    ///
    /// Returns an error if sub-agent creation, startup, or signal
    /// registration fails.
    pub async fn run(&mut self) -> Result<()> {
        if self.status == AgentStatus::Running {
            return Ok(());
        }

        // Build sub-agents.
        let mut client = Client::new(self.config.clone());
        let mut server = Server::new(self.config.clone());

        // Start sub-agents.
        client.run().await?;
        server.run().await?;

        self.client = Some(client);
        self.server = Some(server);
        self.status = AgentStatus::Running;
        tracing::info!("agent running");

        // Set up signal handling.
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sighup = signal(SignalKind::hangup())?;

        // Wait for any shutdown signal.
        tokio::select! {
            _ = sigint.recv() => tracing::info!("received SIGINT, shutting down"),
            _ = sigterm.recv() => tracing::info!("received SIGTERM, shutting down"),
            _ = sighup.recv() => tracing::info!("received SIGHUP, shutting down"),
        }

        self.stop();
        Ok(())
    }

    /// Stop the sub-agents and mark the agent stopped.
    pub fn stop(&mut self) {
        if self.status != AgentStatus::Running {
            self.status = AgentStatus::Stopped;
            return;
        }
        tracing::info!("graceful shutdown started");
        if let Some(client) = self.client.as_mut() {
            client.stop();
        }
        if let Some(server) = self.server.as_mut() {
            server.stop();
        }
        self.status = AgentStatus::Stopped;
        tracing::info!("graceful shutdown complete");
    }
}
