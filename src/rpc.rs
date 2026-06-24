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
use crate::jobspec::Job;
use crate::node::Node;

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
}

/// The in-tree RPC handler.
#[derive(Debug)]
pub struct RpcEndpoint {
    /// Priority eval queue shared with the scheduler loop.
    eval_queue: EvalQueue,
}

impl RpcEndpoint {
    /// Create an endpoint wired to the given eval queue.
    #[must_use]
    pub fn new(eval_queue: EvalQueue) -> Self {
        Self { eval_queue }
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
                // Generate a timestamped eval ID for the test spec.
                // NOTE: ID is not deterministic — it includes a nanosecond
                // timestamp. Tests should not assert on its value.
                let eval_id = format!(
                    "eval-{}-{}",
                    job.name,
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()
                );
                let eval = Evaluation {
                    id: eval_id.clone(),
                    job_id: job.name.clone(),
                    priority: job.priority,
                    trigger: EvalTrigger::JobRegister,
                    status: EvalStatus::Pending,
                };
                // Enqueue a pending eval so a scheduler worker can pick it up.
                // If the queue mutex is poisoned the system is hosed anyway, so
                // propagate the error.
                self.eval_queue.enqueue(eval)?;
                Ok(Response::JobRegistered { eval_id })
            },
            Request::JobDeregister(_) => {
                // TODO: forward to leader, deregister, create eval.
                // Deferred because deregister needs a Raft round-trip to
                // remove the job from state first, then enqueue the eval
                // for scheduler cleanup. At this stage (single-node, no
                // Raft persistence) the state mutation exists but the
                // commit-then-enqueue ordering is not wired. The gap
                // means a deregistered job won't produce a cleanup eval
                // until the leader loop is implemented.
                Ok(Response::Ack)
            },
            Request::NodeRegister(_) => Ok(Response::Ack),
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
}
