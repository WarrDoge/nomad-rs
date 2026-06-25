// SPDX-License-Identifier: Apache-2.0

//! RPC — dependency-agnostic.
//!
//! Defines the request/response surface servers expose to nodes and each other,
//! processed by the in-tree [`RpcEndpoint`](crate::rpc::RpcEndpoint) (forwarding writes
//! to the leader). A real wire transport (custom-over-mTLS, gRPC, ...) replaces
//! its body later. Behaviour is specified by the tests and is unimplemented.

use crate::error::Result;
use crate::eval::{EvalStatus, EvalTrigger, Evaluation};
use crate::eval_queue::EvalQueue;
use crate::fsm::Command;
use crate::jobspec::Job;
use crate::node::Node;
use crate::raft::RaftNode;
use std::sync::{Arc, Mutex};

/// A request a server can handle.
#[derive(Debug, Clone)]
pub enum Request {
    /// Register or update a job.
    JobRegister(Job),
    /// Deregister the job with the given name.
    JobDeregister(String),
    /// Register or update a client node.
    NodeRegister(Node),
    /// Dequeue a pending evaluation for the given scheduler types.
    EvalDequeue {
        /// Scheduler types the worker can handle (e.g. `["service","batch"]`).
        schedulers: Vec<String>,
    },
}

/// A response to a [`Request`].
#[derive(Debug, Clone)]
pub enum Response {
    /// A job was registered; an evaluation was created.
    JobRegistered {
        /// Id of the evaluation created for the registration.
        eval_id: String,
    },
    /// The request was applied with no payload.
    Ack,
    /// The dequeued evaluation, if any was available.
    Eval(Option<Evaluation>),
    /// This node is not the leader; the caller should forward to `leader_addr`.
    NotLeader {
        /// Address of the current leader, if known.
        leader_addr: Option<String>,
    },
}

/// The in-tree RPC handler.
///
/// Writes are committed through the local [`RaftNode`] (so they land in the
/// FSM-backed state), then any follow-up eval is enqueued. On a follower, a
/// write returns [`Response::NotLeader`].
///
/// ponytail: no wire transport yet — `NotLeader` reports where to forward, but
/// the actual node↔server RPC socket (custom-over-mTLS / gRPC) is still TBD.
#[derive(Debug)]
pub struct RpcEndpoint {
    /// Priority eval queue shared with the scheduler loop.
    eval_queue: EvalQueue,
    /// Consensus node writes are committed through.
    raft: Arc<Mutex<RaftNode>>,
}

impl RpcEndpoint {
    /// Create an endpoint with its own single-node bootstrap leader.
    #[must_use]
    pub fn new(eval_queue: EvalQueue) -> Self {
        Self { eval_queue, raft: Arc::new(Mutex::new(RaftNode::bootstrap("rpc-local"))) }
    }

    /// Create an endpoint wired to an existing consensus node.
    #[must_use]
    pub fn with_raft(eval_queue: EvalQueue, raft: Arc<Mutex<RaftNode>>) -> Self {
        Self { eval_queue, raft }
    }

    /// The consensus node this endpoint commits through.
    #[must_use]
    pub fn raft(&self) -> Arc<Mutex<RaftNode>> {
        Arc::clone(&self.raft)
    }

    /// Commit a write command through consensus.
    ///
    /// Returns `Ok(Some(NotLeader))` if this node is a follower (caller should
    /// forward), or `Ok(None)` once the command is committed and applied.
    fn commit(&self, command: Command) -> Result<Option<Response>> {
        let mut raft = self.raft.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !raft.is_leader() {
            return Ok(Some(Response::NotLeader { leader_addr: raft.leader_addr() }));
        }
        raft.propose(command)?;
        Ok(None)
    }

    /// Handle a request and produce a response.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is invalid, the node cannot reach the
    /// leader, or the underlying state operation fails.
    pub fn handle(&self, request: Request) -> Result<Response> {
        match request {
            Request::JobRegister(job) => {
                job.validate()?;
                let (name, priority) = (job.name.clone(), job.priority);
                // Commit the job through consensus first; bail with NotLeader
                // on a follower so the caller can forward.
                if let Some(resp) = self.commit(Command::UpsertJob(job))? {
                    return Ok(resp);
                }
                let eval_id = eval_id_for(&name);
                let eval = Evaluation {
                    id: eval_id.clone(),
                    job_id: name,
                    priority,
                    trigger: EvalTrigger::JobRegister,
                    status: EvalStatus::Pending,
                };
                self.eval_queue.enqueue(eval)?;
                Ok(Response::JobRegistered { eval_id })
            },
            Request::JobDeregister(name) => {
                if let Some(resp) = self.commit(Command::DeregisterJob(name.clone()))? {
                    return Ok(resp);
                }
                // Enqueue a cleanup eval so the scheduler stops the allocs.
                self.eval_queue.enqueue(Evaluation {
                    id: eval_id_for(&name),
                    job_id: name,
                    priority: 50,
                    trigger: EvalTrigger::JobDeregister,
                    status: EvalStatus::Pending,
                })?;
                Ok(Response::Ack)
            },
            Request::NodeRegister(node) => {
                if let Some(resp) = self.commit(Command::UpsertNode(node))? {
                    return Ok(resp);
                }
                Ok(Response::Ack)
            },
            Request::EvalDequeue { schedulers: _ } => {
                // Only one scheduler type exists at this stage (service), so
                // the type filter is a no-op. Once batch/system/sysbatch types
                // land, filter self.eval_queue.dequeue() by the requested
                // schedulers BEFORE popping from the heap — otherwise the
                // wrong scheduler type burns an eval meant for another.
                Ok(Response::Eval(self.eval_queue.dequeue()?))
            },
        }
    }
}

/// A non-deterministic eval id (nanosecond timestamp); tests must not assert
/// on its exact value.
fn eval_id_for(job_name: &str) -> String {
    format!(
        "eval-{}-{}",
        job_name,
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()
    )
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn job_register_returns_eval_id() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q);
        let job = Job { name: "redis".to_owned(), ..Job::default() };
        let resp = ep.handle(Request::JobRegister(job)).unwrap();
        assert!(matches!(resp, Response::JobRegistered { .. }));
    }

    #[test]
    fn eval_dequeue_returns_eval_variant() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q);
        let req = Request::EvalDequeue { schedulers: vec!["service".to_owned()] };
        assert!(matches!(ep.handle(req).unwrap(), Response::Eval(_)));
    }

    #[test]
    fn job_register_enqueues_and_dequeue_returns_it() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q.clone());
        let job = Job { name: "web".to_owned(), ..Job::default() };
        let resp = ep.handle(Request::JobRegister(job)).unwrap();
        let Response::JobRegistered { eval_id } = resp else { panic!("expected JobRegistered") };
        assert!(!eval_id.is_empty());
        // The eval queue now has a pending eval; dequeue it.
        let dequeued = q.dequeue().unwrap().unwrap();
        assert_eq!(dequeued.id, eval_id);
        assert_eq!(dequeued.job_id, "web");
        assert_eq!(dequeued.status, EvalStatus::Pending);
    }

    #[test]
    fn dequeue_returns_highest_priority_first() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q.clone());
        // Register a low-priority job, then a high-priority job.
        let low = Job { name: "low".to_owned(), priority: 30, ..Job::default() };
        let high = Job { name: "high".to_owned(), priority: 80, ..Job::default() };
        ep.handle(Request::JobRegister(low)).unwrap();
        ep.handle(Request::JobRegister(high)).unwrap();
        // Dequeue should yield the high-priority eval first.
        let first = q.dequeue().unwrap().unwrap();
        assert_eq!(first.job_id, "high");
        let second = q.dequeue().unwrap().unwrap();
        assert_eq!(second.job_id, "low");
    }

    #[test]
    fn empty_dequeue_returns_none() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::new(q);
        let req = Request::EvalDequeue { schedulers: vec!["service".to_owned()] };
        let resp = ep.handle(req).unwrap();
        assert!(matches!(resp, Response::Eval(None)));
    }

    #[test]
    fn job_register_persists_job_to_state() {
        let ep = RpcEndpoint::new(EvalQueue::new());
        ep.handle(Request::JobRegister(Job { name: "redis".to_owned(), ..Job::default() })).unwrap();
        assert!(ep.raft().lock().unwrap().state().get_job("redis").is_some());
    }

    fn node(id: &str) -> Node {
        use crate::node::{NodeStatus, SchedulingEligibility};
        Node {
            id: id.to_owned(),
            name: id.to_owned(),
            datacenter: "dc1".to_owned(),
            node_class: String::new(),
            resources: crate::jobspec::Resources::default(),
            status: NodeStatus::Ready,
            eligibility: SchedulingEligibility::Eligible,
            draining: false,
            attributes: std::collections::HashMap::new(),
            drivers: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn node_register_persists_node_to_state() {
        let ep = RpcEndpoint::new(EvalQueue::new());
        assert!(matches!(ep.handle(Request::NodeRegister(node("n1"))).unwrap(), Response::Ack));
        assert!(ep.raft().lock().unwrap().state().get_node("n1").is_some());
    }

    #[test]
    fn write_on_follower_returns_not_leader() {
        let raft = Arc::new(Mutex::new(RaftNode::new("f1")));
        let ep = RpcEndpoint::with_raft(EvalQueue::new(), raft);
        let resp = ep.handle(Request::JobRegister(Job { name: "x".to_owned(), ..Job::default() })).unwrap();
        assert!(matches!(resp, Response::NotLeader { .. }));
    }

    #[test]
    fn job_deregister_removes_job_and_enqueues_cleanup_eval() {
        let q = EvalQueue::new();
        let ep = RpcEndpoint::with_raft(q.clone(), Arc::new(Mutex::new(RaftNode::bootstrap("l1"))));
        ep.handle(Request::JobRegister(Job { name: "web".to_owned(), ..Job::default() })).unwrap();
        let _ = q.dequeue().unwrap(); // drain the register eval
        ep.handle(Request::JobDeregister("web".to_owned())).unwrap();
        assert!(ep.raft().lock().unwrap().state().get_job("web").is_none());
        let cleanup = q.dequeue().unwrap().expect("cleanup eval enqueued");
        assert_eq!(cleanup.job_id, "web");
        assert_eq!(cleanup.trigger, EvalTrigger::JobDeregister);
    }
}
