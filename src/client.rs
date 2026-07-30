// SPDX-License-Identifier: Apache-2.0

//! Nomad client agent — manages task execution on a node.
//!
//! The client communicates with Nomad servers to receive allocations,
//! runs tasks using drivers, and reports back status.

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::alloc::{Allocation, DesiredStatus};
use crate::allocrunner::AllocRunner;
use crate::config::Config;
use crate::error::Result;
use crate::id::AllocId;
use crate::jobspec::Task;

/// The possible states a Nomad client can be in.
pub use crate::agent::AgentStatus as ClientStatusEnum;

/// An update telling the client's event loop to start, stop, or modify
/// an allocation's lifecycle on this node.
#[derive(Debug, Clone)]
pub struct AllocUpdate {
    /// The allocation to act on.
    pub alloc: Allocation,
    /// Tasks belonging to the allocation's task group.
    pub tasks: Vec<Task>,
}

/// A handle for sending allocation updates to a running client.
#[derive(Debug, Clone)]
pub struct Allocator {
    tx: mpsc::UnboundedSender<AllocUpdate>,
}

impl Allocator {
    /// Submit one allocation to the client's event loop for execution.
    ///
    /// Returns `Err(update)` if the client is not running (channel closed).
    pub fn submit(&self, update: AllocUpdate) -> std::result::Result<(), AllocUpdate> {
        self.tx.send(update).map_err(|e| e.0)
    }
}

/// A Nomad client agent that manages local task execution.
#[derive(Debug)]
pub struct Client {
    /// Client configuration.
    config: Config,
    /// Current client status.
    status: ClientStatusEnum,
    /// Channel for receiving allocation updates from the server.
    alloc_rx: Option<mpsc::UnboundedReceiver<AllocUpdate>>,
    /// Handle for sending allocation updates (exposed to the server).
    alloc_tx: mpsc::UnboundedSender<AllocUpdate>,
    /// Running allocations keyed by id.
    runners: HashMap<AllocId, AllocRunner>,
}

impl Client {
    /// Create a new client with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        let (alloc_tx, alloc_rx) = mpsc::unbounded_channel();
        Self {
            config,
            status: ClientStatusEnum::Initialized,
            alloc_rx: Some(alloc_rx),
            alloc_tx,
            runners: HashMap::new(),
        }
    }

    /// Returns the configuration this client was created with.
    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the current status of the client.
    #[must_use]
    pub const fn status(&self) -> ClientStatusEnum {
        self.status
    }

    /// Returns `true` if the client is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == ClientStatusEnum::Running
    }

    /// An [`Allocator`] handle that can be used to submit allocation updates
    /// to this client's event loop.
    #[must_use]
    pub fn allocator(&self) -> Allocator {
        Allocator { tx: self.alloc_tx.clone() }
    }

    /// Start the client agent, entering the event loop.
    ///
    /// The event loop:
    /// - Listens for allocation updates on the internal channel
    /// - Starts/stops [`AllocRunner`]s in response
    /// - Periodically sends heartbeats (placeholder — no RPC yet)
    /// - Exits when the channel is closed or runner.stop() is called
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to initialise or encounters a
    /// fatal runtime error.
    pub async fn run(&mut self) -> Result<()> {
        if self.status == ClientStatusEnum::Running {
            return Ok(());
        }
        self.status = ClientStatusEnum::Running;
        tracing::info!("client starting");

        let mut alloc_rx = self.alloc_rx.take()
            .expect("alloc_rx consumed — run() called twice without a rebuild");
        // Replace the internal sender with a fresh (dead) one so the old
        // sender is dropped. Otherwise the channel stays open even when
        // all external `Allocator` handles are gone.
        let (tx, _rx) = mpsc::unbounded_channel();
        let _ = std::mem::replace(&mut self.alloc_tx, tx);

        // ponytail: heartbeat interval hardcoded to 15s; make configurable
        // once the ClientConfig carries a heartbeat_secs field.
        const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat.tick().await; // skip the immediate first tick

        let mut shutdown = false;

        loop {
            // NOTE: the `if shutdown { ready(()) } else { pending() }` pattern
            // won't type-check because the two arms produce different future types.
            // Instead we check the flag after each branch (inside the loop body).
            tokio::select! {
                update = alloc_rx.recv() => {
                    match update {
                        Some(update) => self.handle_alloc_update(update),
                        None => {
                            tracing::info!("allocation channel closed, shutting down");
                            shutdown = true;
                        },
                    }
                }

                _ = heartbeat.tick() => {
                    // Periodic heartbeat / status update to server.
                    // ponytail: no RPC to a server yet — this is a placeholder
                    // that will send node status + alloc health once the RPC
                    // layer extends to client→server communication.
                    tracing::trace!("client heartbeat");
                }
            }

            if shutdown {
                break;
            }
        }

        // Graceful shutdown: stop all runners.
        self.stop_all_runners();
        self.status = ClientStatusEnum::Stopped;
        tracing::info!("client stopped");
        Ok(())
    }

    /// Process one allocation update from the channel.
    fn handle_alloc_update(&mut self, update: AllocUpdate) {
        let alloc_id = update.alloc.id.clone();

        match update.alloc.desired_status {
            DesiredStatus::Run => {
                // Start or update the allocation.
                if self.runners.contains_key(&alloc_id) {
                    tracing::debug!("alloc {alloc_id} already running, ignoring update");
                    return;
                }

                match AllocRunner::new(update.alloc, update.tasks) {
                    Ok(mut runner) => {
                        if let Err(e) = runner.run() {
                            tracing::error!("alloc {alloc_id} failed to start: {e}");
                            return;
                        }
                        tracing::info!("alloc {alloc_id} started");
                        self.runners.insert(alloc_id, runner);
                    },
                    Err(e) => {
                        tracing::error!("alloc {alloc_id} failed to create runner: {e}");
                    },
                }
            },
            DesiredStatus::Stop | DesiredStatus::Evict => {
                // Stop the allocation if it exists.
                if let Some(mut runner) = self.runners.remove(&alloc_id) {
                    if let Err(e) = runner.destroy() {
                        tracing::error!("alloc {alloc_id} failed to stop: {e}");
                    }
                    tracing::info!("alloc {alloc_id} stopped");
                } else {
                    tracing::debug!("alloc {alloc_id} not found for stop, ignoring");
                }
            },
        }
    }

    /// Stop all running allocations (used during shutdown).
    fn stop_all_runners(&mut self) {
        let ids: Vec<AllocId> = self.runners.keys().cloned().collect();
        for id in &ids {
            if let Some(mut runner) = self.runners.remove(id) {
                if let Err(e) = runner.destroy() {
                    tracing::error!("alloc {id} stop failed during shutdown: {e}");
                }
            }
        }
    }

    /// Gracefully stop the client agent, stopping all running allocations.
    pub fn stop(&mut self) {
        if self.status != ClientStatusEnum::Running {
            self.status = ClientStatusEnum::Stopped;
            return;
        }
        tracing::info!("client stopping {} allocations", self.runners.len());
        self.stop_all_runners();
        self.status = ClientStatusEnum::Stopped;
        tracing::info!("client stopped");
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
#[allow(clippy::unwrap_used, reason = "tests may unwrap")]
mod tests {
    use super::*;
    use crate::alloc::ClientStatus;
    use crate::id::{AllocId, EvalId, JobId, NodeId};
    use crate::jobspec::Resources;
    use std::collections::HashMap;

    fn test_config() -> Config {
        Config { node_name: "test-client".to_owned(), ..Config::default() }
    }

    fn running_alloc(id: &str, desired: DesiredStatus) -> Allocation {
        Allocation {
            id: AllocId::from(id),
            eval_id: EvalId::from("e1"),
            node_id: NodeId::from("n1"),
            job_id: JobId::from("redis"),
            task_group: "cache".to_owned(),
            desired_status: desired,
            client_status: ClientStatus::Pending,
            resources: Resources::default(),
        }
    }

    fn sleep_task() -> Task {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("sleep"));
        config.insert("args".to_owned(), serde_json::json!(["30"]));
        Task { name: "web".to_owned(), driver: "exec".to_owned(), config, resources: Resources::default() }
    }

    #[test]
    fn test_client_new() {
        let client = Client::new(test_config());
        assert_eq!(client.status(), ClientStatusEnum::Initialized);
        assert!(!client.is_running());
    }

    #[test]
    fn test_client_allocator_handle() {
        let client = Client::new(test_config());
        let allocator = client.allocator();
        assert!(!client.is_running());
        // Allocator should exist but sending into a non-running client will
        // just buffer — that's fine.
        let update = AllocUpdate { alloc: running_alloc("a1", DesiredStatus::Run), tasks: vec![sleep_task()] };
        // Send should succeed even though client isn't running.
        allocator.submit(update).ok();
    }

    #[test]
    fn test_client_stop_before_run() {
        let mut client = Client::new(test_config());
        assert_eq!(client.status(), ClientStatusEnum::Initialized);
        client.stop();
        assert_eq!(client.status(), ClientStatusEnum::Stopped);
    }

    #[tokio::test]
    async fn test_client_full_lifecycle_via_spawn() {
        // Run the event loop in the background, submit an allocator update,
        // then close the channel. The loop should exit cleanly and runners
        // should have been started/stopped.
        let mut client = Client::new(test_config());
        let allocator = client.allocator();

        let handle = tokio::spawn(async move { client.run().await });

        // Give the loop time to get to the recv() branch.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Submit start.
        let update = AllocUpdate { alloc: running_alloc("a1", DesiredStatus::Run), tasks: vec![sleep_task()] };
        allocator.submit(update).ok();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Submit stop.
        let update = AllocUpdate { alloc: running_alloc("a1", DesiredStatus::Stop), tasks: vec![] };
        allocator.submit(update).ok();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Closing the channel should cause the event loop to exit gracefully.
        drop(allocator);
        let result = handle.await;
        assert!(result.is_ok(), "client event loop must exit cleanly");
    }

    #[tokio::test]
    async fn test_client_dropped_sender_exits_loop() {
        // A client that never receives any allocator updates should still
        // exit cleanly when the only sender is dropped.
        let mut client = Client::new(test_config());
        let allocator = client.allocator();

        let handle = tokio::spawn(async move { client.run().await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(allocator);

        let result = handle.await;
        assert!(result.is_ok(), "client must exit when allocator is dropped");
    }
}
