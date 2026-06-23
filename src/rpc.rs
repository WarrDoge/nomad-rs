// SPDX-License-Identifier: Apache-2.0

//! RPC contract — dependency-agnostic.
//!
//! Defines the request/response surface servers expose to nodes and each other,
//! and the handler that processes them (forwarding writes to the leader). The
//! wire transport (custom-over-mTLS, gRPC, etc.) lives behind [`RpcHandler`](crate::rpc::RpcHandler).
//! [`RpcEndpoint`](crate::rpc::RpcEndpoint) is the in-tree handler whose behaviour is specified by the
//! tests and is unimplemented.

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

/// Processes RPC requests, forwarding writes to the leader when needed.
pub trait RpcHandler {
    /// Handle a request and produce a response.
    ///
    /// # Errors
    ///
    /// Returns an error if the request is invalid, the node cannot reach the
    /// leader, or the underlying state operation fails.
    fn handle(&self, request: Request) -> Result<Response>;
}

/// The in-tree RPC handler.
#[derive(Debug)]
pub struct RpcEndpoint;

impl RpcHandler for RpcEndpoint {
    #[allow(clippy::needless_pass_by_value, reason = "request is dispatched/forwarded once implemented")]
    fn handle(&self, request: Request) -> Result<Response> {
        let _ = request;
        todo!("dispatch the request, forwarding writes to the leader")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn job_register_returns_eval_id() {
        let job = Job { name: "redis".to_owned(), ..Job::default() };
        let resp = RpcEndpoint.handle(Request::JobRegister(job)).unwrap();
        assert!(matches!(resp, Response::JobRegistered { .. }));
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn eval_dequeue_returns_eval_variant() {
        let req = Request::EvalDequeue { schedulers: vec!["service".to_owned()] };
        assert!(matches!(RpcEndpoint.handle(req).unwrap(), Response::Eval(_)));
    }
}
