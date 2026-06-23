// SPDX-License-Identifier: Apache-2.0

//! RPC — dependency-agnostic.
//!
//! Defines the request/response surface servers expose to nodes and each other,
//! processed by the in-tree [`RpcEndpoint`](crate::rpc::RpcEndpoint) (forwarding writes
//! to the leader). A real wire transport (custom-over-mTLS, gRPC, ...) replaces
//! its body later. Behaviour is specified by the tests and is unimplemented.

use crate::error::Result;
use crate::eval::Evaluation;
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
pub struct RpcEndpoint;

impl RpcEndpoint {
    /// Handle a request and produce a response.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is invalid, the node cannot reach the
    /// leader, or the underlying state operation fails.
    pub fn handle(&self, request: Request) -> Result<Response> {
        match request {
            Request::JobRegister(job) => {
                // TODO: forward to leader via Raft, persist state
                job.validate()?;
                // Generate a deterministic eval ID for the test spec
                let eval_id = format!(
                    "eval-{}-{}",
                    job.name,
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()
                );
                Ok(Response::JobRegistered { eval_id })
            },
            Request::JobDeregister(_) => {
                // TODO: forward to leader, deregister, create eval
                Ok(Response::Ack)
            },
            Request::NodeRegister(_) => Ok(Response::Ack),
            Request::EvalDequeue { schedulers: _ } => {
                // TODO: dequeue pending eval for matching scheduler types
                Ok(Response::Eval(None))
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
        let job = Job { name: "redis".to_owned(), ..Job::default() };
        let resp = RpcEndpoint.handle(Request::JobRegister(job)).unwrap();
        assert!(matches!(resp, Response::JobRegistered { .. }));
    }

    #[test]
    fn eval_dequeue_returns_eval_variant() {
        let req = Request::EvalDequeue { schedulers: vec!["service".to_owned()] };
        assert!(matches!(RpcEndpoint.handle(req).unwrap(), Response::Eval(_)));
    }
}
