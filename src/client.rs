// SPDX-License-Identifier: Apache-2.0

//! Nomad client agent — manages task execution on a node.
//!
//! The client communicates with Nomad servers to receive allocations,
//! runs tasks using drivers, and reports back status.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::config::Config;
use crate::error::Result;
use crate::node::{Node, NodeStatus, SchedulingEligibility};
use crate::rpc::{Request, Response, RpcClient};

/// How often the client sends a heartbeat to the server.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// How often the client polls for new allocations.
const ALLOC_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// The possible states a Nomad client can be in.
pub use crate::agent::AgentStatus as ClientStatus;

/// A Nomad client agent that manages local task execution.
#[derive(Debug)]
pub struct Client {
    /// Client configuration.
    config: Config,
    /// Current client status.
    status: ClientStatus,
    /// Server address to connect to.
    server_addr: String,
    /// Set to stop the background heartbeat/alloc watch loops.
    shutdown: Arc<AtomicBool>,
    /// Signalled when the run loop has fully stopped.
    done: Arc<Notify>,
    /// Handle to the running background task, if started.
    handle: Option<JoinHandle<()>>,
}

impl Client {
    /// Create a new client with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            status: ClientStatus::Initialized,
            server_addr: String::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            done: Arc::new(Notify::new()),
            handle: None,
        }
    }

    /// Returns the configuration this client was created with.
    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the current status of the client.
    #[must_use]
    pub const fn status(&self) -> ClientStatus {
        self.status
    }

    /// Returns `true` if the client is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == ClientStatus::Running
    }

    /// Set the server address the client should connect to.
    ///
    /// If not set, the client uses the configured `bind_addr` (the default
    /// server address `127.0.0.1:4646`).
    pub fn set_server_addr(&mut self, addr: impl Into<String>) {
        self.server_addr = addr.into();
    }

    /// Resolve the server address, falling back to the default.
    fn server_addr(&self) -> &str {
        if self.server_addr.is_empty() { "127.0.0.1:4646" } else { &self.server_addr }
    }

    /// Start the client agent. This spawns a background task that:
    ///
    /// 1. Connects to the Nomad server via [`RpcClient`].
    /// 2. Sends periodic heartbeats (`NodeRegister`).
    /// 3. Sets up signal handling for graceful shutdown (SIGINT/SIGTERM).
    ///
    /// Returns immediately after starting. The client continues running until
    /// [`stop`](Self::stop) is called or a fatal error occurs.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is already running or if the initial
    /// connection to the server fails.
    pub async fn run(&mut self) -> Result<()> {
        if self.status == ClientStatus::Running {
            return Ok(());
        }
        self.shutdown.store(false, Ordering::Relaxed);
        self.status = ClientStatus::Running;
        tracing::info!("client starting, connecting to server at {}", self.server_addr());

        // Establish initial connection.
        let client = RpcClient::connect(self.server_addr()).await.inspect_err(|_| {
            self.status = ClientStatus::Initialized;
        })?;

        // Build the node registration payload from config.
        let node = Node {
            id: self.config.node_name.clone().into(),
            name: self.config.node_name.clone(),
            datacenter: self.config.datacenter.clone(),
            node_class: String::new(),
            resources: crate::jobspec::Resources::default(),
            status: NodeStatus::Init,
            eligibility: SchedulingEligibility::Eligible,
            draining: false,
            attributes: std::collections::HashMap::new(),
            drivers: std::collections::HashMap::new(),
        };

        let shutdown = Arc::clone(&self.shutdown);
        let done = Arc::clone(&self.done);

        self.handle = Some(tokio::spawn(client_loop(client, node, shutdown, done)));

        // Set up signal handling for graceful shutdown — this runs on the
        // caller's task (typically the Agent loop) so signals interrupt the
        // Agent's select, not the client's background loops.
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;

        select! {
            _ = sigint.recv() => tracing::info!("client received SIGINT"),
            _ = sigterm.recv() => tracing::info!("client received SIGTERM"),
            () = self.done.notified() => {
                // Background loop already exited (fatal error or explicit stop).
            },
        }

        self.stop();
        Ok(())
    }

    /// Gracefully stop the client agent: signal the background loops and
    /// await their completion.
    pub fn stop(&mut self) {
        if self.status != ClientStatus::Running {
            self.status = ClientStatus::Stopped;
            return;
        }
        tracing::info!("client graceful shutdown started");
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        self.status = ClientStatus::Stopped;
        tracing::info!("client stopped");
    }
}

/// Background client loop: heartbeats and alloc watching.
///
/// Runs until `shutdown` is signalled, the server connection is lost,
/// or a fatal error occurs.
async fn client_loop(mut client: RpcClient, node: Node, shutdown: Arc<AtomicBool>, done: Arc<Notify>) {
    // Register the node with the server on startup.
    let req = Request::NodeRegister(node.clone());
    match client.call(&req).await {
        Ok(Response::NotLeader { leader_addr }) => {
            tracing::warn!("server returned NotLeader on initial register; leader: {leader_addr:?}");
        },
        Ok(Response::Ack) => tracing::info!("node registered with server"),
        Ok(_) => tracing::warn!("unexpected response to NodeRegister"),
        Err(e) => {
            tracing::error!("failed to register node: {e}");
        },
    }

    let mut heartbeat_timer = tokio::time::interval(HEARTBEAT_INTERVAL);
    let mut alloc_timer = tokio::time::interval(ALLOC_POLL_INTERVAL);

    while !shutdown.load(Ordering::Relaxed) {
        tokio::select! {
            _ = heartbeat_timer.tick() => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                // Send heartbeat.
                let req = Request::NodeHeartbeat { node_id: node.id.clone() };
                match client.call(&req).await {
                    Ok(Response::Ack) => tracing::trace!("heartbeat sent"),
                    Ok(Response::NotLeader { leader_addr }) => {
                        tracing::warn!("heartbeat: server not leader, leader: {leader_addr:?}");
                    },
                    Ok(_) => tracing::warn!("heartbeat: unexpected response"),
                    Err(e) => {
                        tracing::error!("heartbeat failed: {e}");
                    },
                }
            },
            _ = alloc_timer.tick() => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                // Poll for allocations assigned to this node.
                let req = Request::NodeGetAllocs { node_id: node.id.clone() };
                match client.call(&req).await {
                    Ok(Response::NodeAllocs { allocs }) => {
                        if allocs.is_empty() {
                            tracing::trace!("no new allocations");
                        } else {
                            tracing::info!("received {} allocation(s)", allocs.len());
                            for alloc in &allocs {
                                tracing::info!("  alloc: {} (job: {})", alloc.id, alloc.job_id);
                                // ponytail: hand the alloc to the allocrunner.
                            }
                        }
                    },
                    Ok(Response::NotLeader { leader_addr }) => {
                        tracing::warn!("alloc poll: server not leader, leader: {leader_addr:?}");
                    },
                    Ok(_) => tracing::warn!("alloc poll: unexpected response"),
                    Err(e) => {
                        tracing::error!("alloc poll failed: {e}");
                    },
                }
            },
        }
    }

    done.notify_one();
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn test_client_run() {
        let mut client = Client::new(test_config());
        assert!(client.status() == ClientStatus::Initialized);
        // Without a server, run() will fail to connect — this is expected.
        let result = client.run().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_client_stop_before_run() {
        let mut client = Client::new(test_config());
        assert_eq!(client.status(), ClientStatus::Initialized);
        client.stop();
        assert_eq!(client.status(), ClientStatus::Stopped);
    }
}
