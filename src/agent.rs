// SPDX-License-Identifier: Apache-2.0

//! Agent lifecycle — the top-level orchestrator that holds a [`Client`]
//! and/or [`Server`] process and drives their graceful lifecycle.

use std::time::Duration;

use tokio::signal::unix::{SignalKind, signal};
use tokio::time::timeout;

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
    /// Hit a terminal error.
    Failed,
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

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the current status.
    #[must_use]
    pub fn status(&self) -> AgentStatus {
        self.status
    }

    /// Returns `true` if the agent is running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == AgentStatus::Running
    }

    /// Reconfigure the agent from a new config. Sub-agents are dropped
    /// and recreated on the next call to [`Agent::run`].
    pub fn reconfigure(&mut self, config: Config) {
        self.config = config;
        self.client = None;
        self.server = None;
        tracing::info!("agent reconfigured");
    }

    /// Run the agent, handling signals and graceful shutdown.
    ///
    /// Returns `Ok(())` after a clean shutdown, or an error if the
    /// shutdown timeout expires.
    ///
    /// # Errors
    ///
    /// Returns an error if sub-agent creation or startup fails, or if the
    /// shutdown grace period expires.
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
        let old_status = std::mem::replace(&mut self.status, AgentStatus::Running);
        tracing::info!(?old_status, "agent running");

        // Set up signal handling.
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sighup = signal(SignalKind::hangup())?;

        // Wait for a signal.
        tokio::select! {
            _ = sigint.recv() => {
                tracing::info!("received SIGINT, shutting down");
            }
            _ = sigterm.recv() => {
                tracing::info!("received SIGTERM, shutting down");
            }
            _ = sighup.recv() => {
                tracing::info!("received SIGHUP, reloading config");
                self.reload_config().await?;
                // After reload, keep running — loop back to wait for next
                // signal. For simplicity in this phase, we stop after
                // one HUP. A background task would replace this in Phase 2.
                return self.stop().await;
            }
        }

        self.stop().await
    }

    /// Graceful shutdown with a timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the shutdown grace period expires.
    pub async fn stop(&mut self) -> Result<()> {
        if self.status != AgentStatus::Running {
            self.status = AgentStatus::Stopped;
            return Ok(());
        }
        tracing::info!("graceful shutdown started");

        let shutdown = async {
            if let Some(client) = self.client.as_mut() {
                client.stop();
            }
            if let Some(server) = self.server.as_mut() {
                server.stop();
            }
        };

        if let Ok(()) = timeout(Duration::from_secs(30), shutdown).await {
            self.status = AgentStatus::Stopped;
            tracing::info!("graceful shutdown complete");
            Ok(())
        } else {
            self.status = AgentStatus::Failed;
            tracing::error!("graceful shutdown timed out after 30s");
            Err(crate::error::Error::Runtime("shutdown timed out after 30s".to_owned()))
        }
    }

    /// Reload configuration from the config file path.
    #[allow(clippy::unused_async)]
    async fn reload_config(&mut self) -> Result<()> {
        tracing::info!("config reload triggered");
        // TODO: re-read config file, merge env vars, merge CLI flags
        Ok(())
    }
}
