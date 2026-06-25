// SPDX-License-Identifier: Apache-2.0

//! Nomad server — cluster management, scheduling, and state.
//!
//! The server owns the single-node [`RaftNode`](crate::raft::RaftNode) (its FSM
//! is the authoritative cluster state), an [`EvalQueue`](crate::eval_queue::EvalQueue),
//! and a background scheduler worker that ties them together: it drains pending
//! evaluations, places allocations via [`scheduler::process_eval`](crate::scheduler::process_eval),
//! and commits each plan through Raft. Writes enter via [`Server::endpoint`](crate::server::Server::endpoint).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::config::Config;
use crate::error::Result;
use crate::eval_queue::EvalQueue;
use crate::fsm::Command;
use crate::raft::RaftNode;
use crate::rpc::RpcEndpoint;
use crate::scheduler::process_eval;

/// The possible states a Nomad server can be in.
pub use crate::agent::AgentStatus as ServerStatus;

/// A Nomad server node in the cluster.
#[derive(Debug)]
pub struct Server {
    /// Server configuration.
    config: Config,
    /// Current server status.
    status: ServerStatus,
    /// Single-node Raft, owning the authoritative FSM/state. Shared with the
    /// scheduler worker and any [`RpcEndpoint`] handed out by [`Server::endpoint`].
    raft: Arc<Mutex<RaftNode>>,
    /// Pending-evaluation broker, shared with the scheduler worker.
    queue: EvalQueue,
    /// Set to stop the background scheduler worker.
    shutdown: Arc<AtomicBool>,
    /// Handle to the running scheduler worker, if started.
    worker: Option<JoinHandle<()>>,
}

/// Lock a Raft handle, recovering from a poisoned mutex.
fn lock(raft: &Mutex<RaftNode>) -> MutexGuard<'_, RaftNode> {
    raft.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Background scheduler worker: while not shut down, drains the eval queue,
/// places allocations, and commits each plan through Raft. Acks evals that
/// place cleanly and nacks failures for redelivery. Only the leader schedules.
async fn scheduler_worker(raft: Arc<Mutex<RaftNode>>, queue: EvalQueue, shutdown: Arc<AtomicBool>) {
    /// Idle delay when there is nothing to do.
    const IDLE: Duration = Duration::from_millis(25);
    while !shutdown.load(Ordering::Relaxed) {
        if !lock(&raft).is_leader() {
            tokio::time::sleep(IDLE).await;
            continue;
        }
        let eval = match queue.dequeue() {
            Ok(Some(e)) => e,
            Ok(None) => {
                tokio::time::sleep(IDLE).await;
                continue;
            },
            Err(_) => break, // queue mutex poisoned: nothing more we can do
        };
        let plan = process_eval(&eval, lock(&raft).state());
        let committed = {
            let mut node = lock(&raft);
            plan.allocs.iter().try_for_each(|a| node.propose(Command::UpsertAlloc(a.clone())))
        };
        match committed {
            Ok(()) => drop(queue.ack(&eval.id)),
            Err(_) => drop(queue.nack(&eval.id)),
        }
    }
}

impl Server {
    /// Create a new server with the given configuration. The single-node Raft is
    /// bootstrapped as leader immediately; the scheduler worker starts on
    /// [`Server::run`].
    #[must_use]
    pub fn new(config: Config) -> Self {
        let raft = Arc::new(Mutex::new(RaftNode::bootstrap(&config.node_name)));
        Self {
            config,
            status: ServerStatus::Initialized,
            raft,
            queue: EvalQueue::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            worker: None,
        }
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

    /// An RPC endpoint over this server's queue and Raft. Job/node registrations
    /// submitted here flow into the FSM and (for jobs) enqueue evals the
    /// scheduler worker will process.
    #[must_use]
    pub fn endpoint(&self) -> RpcEndpoint {
        RpcEndpoint::with_raft(self.queue.clone(), Arc::clone(&self.raft))
    }

    /// A shared handle to this server's Raft node, for reading cluster state.
    #[must_use]
    pub fn raft(&self) -> Arc<Mutex<RaftNode>> {
        Arc::clone(&self.raft)
    }

    /// Start the server: spawn the background scheduler worker. Idempotent.
    ///
    /// # Errors
    ///
    /// Currently infallible; returns `Result` for API stability as Raft/RPC
    /// transport wiring lands.
    #[allow(clippy::unused_async)]
    pub async fn run(&mut self) -> Result<()> {
        if self.status == ServerStatus::Running {
            return Ok(());
        }
        self.shutdown.store(false, Ordering::Relaxed);
        self.status = ServerStatus::Running;
        tracing::info!(node = %self.config.node_name, "server starting");
        self.worker = Some(tokio::spawn(scheduler_worker(
            Arc::clone(&self.raft),
            self.queue.clone(),
            Arc::clone(&self.shutdown),
        )));
        Ok(())
    }

    /// Gracefully stop the server: signal and abort the scheduler worker.
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.worker.take() {
            handle.abort();
        }
        self.status = ServerStatus::Stopped;
        tracing::info!("server stopped");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "tests may unwrap")]
mod tests {
    use super::*;
    use crate::jobspec::{Job, Resources, Task, TaskGroup};
    use crate::node::{Node, NodeStatus, SchedulingEligibility};
    use crate::rpc::Request;
    use std::collections::HashMap;

    fn test_config() -> Config {
        Config { node_name: "test-server".to_owned(), bind_addr: "0.0.0.0:4647".to_owned(), ..Config::default() }
    }

    fn node_with(id: &str, cpu: i32, mem: i32) -> Node {
        Node {
            id: id.to_owned(),
            name: id.to_owned(),
            datacenter: "dc1".to_owned(),
            node_class: String::new(),
            resources: Resources { cpu_mhz: cpu, memory_mb: mem, network_mbps: 0 },
            status: NodeStatus::Ready,
            eligibility: SchedulingEligibility::Eligible,
            draining: false,
            attributes: HashMap::new(),
            drivers: HashMap::new(),
        }
    }

    fn job_with(name: &str, cpu: i32, mem: i32) -> Job {
        let task = Task {
            name: "t".to_owned(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources { cpu_mhz: cpu, memory_mb: mem, network_mbps: 0 },
        };
        Job {
            name: name.to_owned(),
            task_groups: vec![TaskGroup { name: "web".to_owned(), count: 1, tasks: vec![task] }],
            ..Job::default()
        }
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

    #[tokio::test]
    async fn test_server_run() {
        let mut server = Server::new(test_config());
        assert_eq!(server.status(), ServerStatus::Initialized);
        server.run().await.unwrap();
        assert!(server.is_running());
        assert_eq!(server.status(), ServerStatus::Running);
        server.stop();
    }

    #[tokio::test]
    async fn test_server_run_idempotent() {
        let mut server = Server::new(test_config());
        server.run().await.unwrap();
        assert!(server.is_running());
        server.run().await.unwrap();
        assert!(server.is_running());
        server.stop();
    }

    #[tokio::test]
    async fn test_server_stop() {
        let mut server = Server::new(test_config());
        server.run().await.unwrap();
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

    #[tokio::test]
    async fn worker_places_alloc_for_registered_job() {
        let mut server = Server::new(test_config());
        server.run().await.unwrap();
        let ep = server.endpoint();
        ep.handle(Request::NodeRegister(node_with("n1", 1000, 1024))).unwrap();
        ep.handle(Request::JobRegister(job_with("redis", 100, 128))).unwrap();

        let raft = server.raft();
        let mut placed = false;
        for _ in 0..50 {
            if !lock(&raft).state().list_allocs().is_empty() {
                placed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        server.stop();
        assert!(placed, "scheduler worker placed an alloc for the registered job");
        // The eval was acked, not left in flight.
        assert_eq!(server.queue.in_flight_len(), 0);
    }
}
