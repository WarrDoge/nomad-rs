// SPDX-License-Identifier: Apache-2.0

//! HTTP API — dependency-agnostic.
//!
//! Defines the request/response shape and the in-tree [`HttpApi`](crate::api::HttpApi)
//! that routes API calls. A real web framework (axum/hyper/...) replaces its
//! body later. Behaviour is specified by the tests and is unimplemented.

use crate::error::Result;
use crate::state::StateStore;

/// HTTP method of an API request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
    /// `DELETE`.
    Delete,
}

/// A decoded API request.
#[derive(Debug, Clone)]
pub struct ApiRequest {
    /// Request method.
    pub method: Method,
    /// Request path, e.g. `"/v1/jobs"`.
    pub path: String,
    /// Optional JSON body.
    pub body: Option<String>,
}

/// An API response.
#[derive(Debug, Clone)]
pub struct ApiResponse {
    /// HTTP status code.
    pub status: u16,
    /// JSON (or plain-text) response body.
    pub body: String,
}

impl ApiResponse {
    /// Build a 200 OK JSON response.
    #[must_use]
    pub fn ok(body: impl serde::Serialize) -> Self {
        Self { status: 200, body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_owned()) }
    }

    /// Build a 201 Created JSON response.
    #[must_use]
    pub fn created(body: impl serde::Serialize) -> Self {
        Self { status: 201, body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_owned()) }
    }

    /// Build a 400 Bad Request response.
    #[must_use]
    pub fn bad_request(msg: &str) -> Self {
        Self { status: 400, body: serde_json::json!({ "error": msg }).to_string() }
    }

    /// Build a 404 Not Found response.
    #[must_use]
    pub fn not_found() -> Self {
        Self { status: 404, body: serde_json::json!({ "error": "not found" }).to_string() }
    }

    /// Build a 500 Internal Server Error response.
    #[must_use]
    pub fn internal_error(msg: &str) -> Self {
        Self { status: 500, body: serde_json::json!({ "error": msg }).to_string() }
    }
}

/// The in-tree HTTP API.
#[derive(Debug)]
pub struct HttpApi {
    /// Shared cluster state (optional — not all endpoints need it).
    state: Option<StateStore>,
}

#[allow(
    clippy::unnecessary_wraps,
    clippy::unused_self,
    reason = "handler methods use Result<ApiResponse> for future write-endpoint compatibility"
)]
impl HttpApi {
    /// Create a new API handler.
    #[must_use]
    pub fn new(state: Option<StateStore>) -> Self {
        Self { state }
    }

    /// Handle one request.
    ///
    /// # Errors
    ///
    /// Returns an error if routing fails or the underlying operation errors.
    #[allow(
        clippy::needless_pass_by_value,
        clippy::result_unit_err,
        reason = "request is routed/consumed once implemented; future write endpoints will return errors"
    )]
    pub fn handle(&self, request: ApiRequest) -> Result<ApiResponse> {
        match (request.method, request.path.as_str()) {
            (Method::Get, "/v1/jobs") => self.handle_list_jobs(),
            (Method::Get, path) if path.starts_with("/v1/job/") => {
                let name = path.trim_start_matches("/v1/job/");
                self.handle_get_job(name)
            },
            (Method::Get, "/v1/evaluations") => self.handle_list_evals(),
            (Method::Get, "/v1/allocations") => self.handle_list_allocs(),
            (Method::Get, "/v1/nodes") => self.handle_list_nodes(),
            (Method::Get, "/v1/agent") => self.handle_agent_health(),
            (Method::Get, "/v1/agent/members") => self.handle_agent_members(),
            (Method::Get, "/v1/agent/self") => self.handle_agent_self(),
            (Method::Get, "/v1/status/leader") => self.handle_status_leader(),
            (Method::Get, "/v1/status/peers") => self.handle_status_peers(),
            (Method::Get, "/v1/operator/raft/configuration") => self.handle_operator_raft_config(),
            _ => Ok(ApiResponse::not_found()),
        }
    }

    /// GET /v1/jobs — list all jobs.
    fn handle_list_jobs(&self) -> Result<ApiResponse> {
        match &self.state {
            Some(state) => {
                let jobs = state.list_jobs();
                Ok(ApiResponse::ok(&jobs))
            },
            None => Ok(ApiResponse::ok(serde_json::json!([]))),
        }
    }

    /// GET /v1/job/{name} — get a single job.
    fn handle_get_job(&self, name: &str) -> Result<ApiResponse> {
        if name.is_empty() {
            return Ok(ApiResponse::bad_request("job name is required"));
        }
        match &self.state {
            Some(state) => match state.get_job(name) {
                Some(job) => Ok(ApiResponse::ok(&job)),
                None => Ok(ApiResponse::not_found()),
            },
            None => Ok(ApiResponse::not_found()),
        }
    }

    /// GET /v1/evaluations — list all evaluations.
    fn handle_list_evals(&self) -> Result<ApiResponse> {
        match &self.state {
            Some(state) => {
                let evals = state.list_evals();
                Ok(ApiResponse::ok(&evals))
            },
            None => Ok(ApiResponse::ok(serde_json::json!([]))),
        }
    }

    /// GET /v1/allocations — list all allocations.
    fn handle_list_allocs(&self) -> Result<ApiResponse> {
        match &self.state {
            Some(state) => {
                let allocs = state.list_allocs();
                Ok(ApiResponse::ok(&allocs))
            },
            None => Ok(ApiResponse::ok(serde_json::json!([]))),
        }
    }

    /// GET /v1/nodes — list all nodes.
    fn handle_list_nodes(&self) -> Result<ApiResponse> {
        match &self.state {
            Some(state) => {
                let nodes = state.list_nodes();
                Ok(ApiResponse::ok(&nodes))
            },
            None => Ok(ApiResponse::ok(serde_json::json!([]))),
        }
    }

    /// GET /v1/agent — agent health check.
    fn handle_agent_health(&self) -> Result<ApiResponse> {
        Ok(ApiResponse::ok(serde_json::json!({
            "status": "running",
            "version": env!("CARGO_PKG_VERSION"),
        })))
    }

    /// GET /v1/agent/members — cluster members (stub).
    fn handle_agent_members(&self) -> Result<ApiResponse> {
        Ok(ApiResponse::ok(serde_json::json!({
            "members": [],
            "serf": "alive",
        })))
    }

    /// GET /v1/agent/self — agent self information.
    fn handle_agent_self(&self) -> Result<ApiResponse> {
        Ok(ApiResponse::ok(serde_json::json!({
            "config": {},
            "version": env!("CARGO_PKG_VERSION"),
        })))
    }

    /// GET /v1/status/leader — current cluster leader (stub).
    fn handle_status_leader(&self) -> Result<ApiResponse> {
        Ok(ApiResponse::ok(serde_json::json!({
            "leader": "",
        })))
    }

    /// GET /v1/status/peers — cluster peers (stub).
    fn handle_status_peers(&self) -> Result<ApiResponse> {
        Ok(ApiResponse::ok(serde_json::json!([])))
    }

    /// GET /v1/operator/raft/configuration — Raft configuration (stub).
    fn handle_operator_raft_config(&self) -> Result<ApiResponse> {
        Ok(ApiResponse::ok(serde_json::json!({
            "servers": [],
            "index": 0,
        })))
    }
}

impl Default for HttpApi {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::alloc::{Allocation, ClientStatus, DesiredStatus};
    use crate::eval::{EvalStatus, EvalTrigger, Evaluation};
    use crate::jobspec::{Job, Resources};
    use crate::node::{Node, NodeStatus, SchedulingEligibility};
    use std::collections::HashMap;

    fn populated_state() -> StateStore {
        let mut state = StateStore::new();
        state.upsert_job(Job { name: "redis".to_owned(), priority: 50, ..Job::default() }).unwrap();
        state
            .upsert_node(Node {
                id: "n1".to_owned(),
                name: "node-1".to_owned(),
                datacenter: "dc1".to_owned(),
                node_class: String::new(),
                resources: Resources::default(),
                status: NodeStatus::Ready,
                eligibility: SchedulingEligibility::Eligible,
                draining: false,
                attributes: HashMap::new(),
                drivers: HashMap::new(),
            })
            .unwrap();
        state
            .upsert_alloc(Allocation {
                id: "a1".to_owned(),
                eval_id: "e1".to_owned(),
                node_id: "n1".to_owned(),
                job_id: "redis".to_owned(),
                task_group: "cache".to_owned(),
                desired_status: DesiredStatus::Run,
                client_status: ClientStatus::Running,
                resources: Resources::default(),
            })
            .unwrap();
        state
            .upsert_eval(Evaluation {
                id: "e1".to_owned(),
                job_id: "redis".to_owned(),
                priority: 50,
                trigger: EvalTrigger::JobRegister,
                status: EvalStatus::Pending,
            })
            .unwrap();
        state
    }

    #[test]
    fn list_jobs_returns_200() {
        let api = HttpApi::new(Some(populated_state()));
        let req = ApiRequest { method: Method::Get, path: "/v1/jobs".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 200);
        let jobs: Vec<serde_json::Value> = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["name"], "redis");
    }

    #[test]
    fn get_job_returns_job() {
        let api = HttpApi::new(Some(populated_state()));
        let req = ApiRequest { method: Method::Get, path: "/v1/job/redis".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 200);
        let job: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(job["name"], "redis");
    }

    #[test]
    fn get_missing_job_returns_404() {
        let api = HttpApi::new(Some(populated_state()));
        let req = ApiRequest { method: Method::Get, path: "/v1/job/nonexistent".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn list_evals_returns_200() {
        let api = HttpApi::new(Some(populated_state()));
        let req = ApiRequest { method: Method::Get, path: "/v1/evaluations".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 200);
        let evals: Vec<serde_json::Value> = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(evals.len(), 1);
    }

    #[test]
    fn list_allocs_returns_200() {
        let api = HttpApi::new(Some(populated_state()));
        let req = ApiRequest { method: Method::Get, path: "/v1/allocations".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 200);
        let allocs: Vec<serde_json::Value> = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(allocs.len(), 1);
    }

    #[test]
    fn list_nodes_returns_200() {
        let api = HttpApi::new(Some(populated_state()));
        let req = ApiRequest { method: Method::Get, path: "/v1/nodes".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 200);
        let nodes: Vec<serde_json::Value> = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn agent_health_returns_200() {
        let api = HttpApi::default();
        let req = ApiRequest { method: Method::Get, path: "/v1/agent".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 200);
        let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["status"], "running");
    }

    #[test]
    fn status_leader_returns_200() {
        let api = HttpApi::default();
        let req = ApiRequest { method: Method::Get, path: "/v1/status/leader".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn operator_raft_config_returns_200() {
        let api = HttpApi::default();
        let req = ApiRequest { method: Method::Get, path: "/v1/operator/raft/configuration".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 200);
    }

    #[test]
    fn unknown_path_returns_404() {
        let api = HttpApi::default();
        let req = ApiRequest { method: Method::Get, path: "/v1/nope".to_owned(), body: None };
        assert_eq!(api.handle(req).unwrap().status, 404);
    }

    #[test]
    fn empty_job_name_returns_400() {
        let api = HttpApi::default();
        let req = ApiRequest { method: Method::Get, path: "/v1/job/".to_owned(), body: None };
        let resp = api.handle(req).unwrap();
        assert_eq!(resp.status, 400);
    }
}
