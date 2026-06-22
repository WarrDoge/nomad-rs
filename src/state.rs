// SPDX-License-Identifier: Apache-2.0

//! In-memory cluster state store.
//!
//! The authoritative store the servers keep in sync via Raft. This is the
//! query/mutation surface the scheduler and RPC handlers use; the Raft FSM
//! applies committed log entries through these same operations. Construction
//! yields an empty store; the operations are specified by the tests and are
//! unimplemented.

use std::collections::HashMap;

use crate::alloc::Allocation;
use crate::error::Result;
use crate::eval::Evaluation;
use crate::jobspec::Job;
use crate::node::Node;

/// The cluster's in-memory state: jobs, nodes, allocations, and evaluations.
#[derive(Debug, Default)]
#[allow(dead_code, reason = "fields are read once the state operations are implemented")]
pub struct StateStore {
    /// Jobs keyed by job name.
    jobs: HashMap<String, Job>,
    /// Nodes keyed by node id.
    nodes: HashMap<String, Node>,
    /// Allocations keyed by allocation id.
    allocs: HashMap<String, Allocation>,
    /// Evaluations keyed by evaluation id.
    evals: HashMap<String, Evaluation>,
}

impl StateStore {
    /// Create an empty state store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a job, keyed by its name.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if the job fails validation.
    #[allow(clippy::needless_pass_by_value, reason = "the value is moved into storage once implemented")]
    pub fn upsert_job(&mut self, job: Job) -> Result<()> {
        let _ = job;
        todo!("validate then insert/replace the job keyed by name")
    }

    /// Fetch a clone of the job named `name`, if present.
    #[must_use]
    pub fn get_job(&self, name: &str) -> Option<Job> {
        todo!("return a clone of the job named {name:?}")
    }

    /// Remove the job named `name`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if no such job exists.
    pub fn delete_job(&mut self, name: &str) -> Result<()> {
        todo!("remove the job named {name:?}, erroring if absent")
    }

    /// All jobs currently stored.
    #[must_use]
    pub fn list_jobs(&self) -> Vec<Job> {
        todo!("clone and return every stored job")
    }

    /// Insert or replace a node, keyed by its id.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if the node fails validation.
    #[allow(clippy::needless_pass_by_value, reason = "the value is moved into storage once implemented")]
    pub fn upsert_node(&mut self, node: Node) -> Result<()> {
        let _ = node;
        todo!("validate then insert/replace the node keyed by id")
    }

    /// Fetch a clone of the node with id `id`, if present.
    #[must_use]
    pub fn get_node(&self, id: &str) -> Option<Node> {
        todo!("return a clone of the node with id {id:?}")
    }

    /// All nodes currently stored.
    #[must_use]
    pub fn list_nodes(&self) -> Vec<Node> {
        todo!("clone and return every stored node")
    }

    /// Insert or replace an allocation, keyed by its id.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if the allocation fails validation.
    #[allow(clippy::needless_pass_by_value, reason = "the value is moved into storage once implemented")]
    pub fn upsert_alloc(&mut self, alloc: Allocation) -> Result<()> {
        let _ = alloc;
        todo!("validate then insert/replace the allocation keyed by id")
    }

    /// All allocations placed on the node with id `node_id`.
    #[must_use]
    pub fn allocs_by_node(&self, node_id: &str) -> Vec<Allocation> {
        todo!("clone and return allocations whose node_id == {node_id:?}")
    }

    /// All allocations belonging to the job named `job_id`.
    #[must_use]
    pub fn allocs_by_job(&self, job_id: &str) -> Vec<Allocation> {
        todo!("clone and return allocations whose job_id == {job_id:?}")
    }

    /// Insert or replace an evaluation, keyed by its id.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if the evaluation fails validation.
    #[allow(clippy::needless_pass_by_value, reason = "the value is moved into storage once implemented")]
    pub fn upsert_eval(&mut self, eval: Evaluation) -> Result<()> {
        let _ = eval;
        todo!("validate then insert/replace the evaluation keyed by id")
    }

    /// Fetch a clone of the evaluation with id `id`, if present.
    #[must_use]
    pub fn get_eval(&self, id: &str) -> Option<Evaluation> {
        todo!("return a clone of the evaluation with id {id:?}")
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

    fn job(name: &str) -> Job {
        Job { name: name.to_owned(), ..Job::default() }
    }

    fn node(id: &str) -> Node {
        Node {
            id: id.to_owned(),
            name: id.to_owned(),
            datacenter: "dc1".to_owned(),
            node_class: String::new(),
            resources: Resources::default(),
            status: NodeStatus::Ready,
            eligibility: SchedulingEligibility::Eligible,
            draining: false,
            attributes: HashMap::new(),
            drivers: HashMap::new(),
        }
    }

    fn alloc(id: &str, node_id: &str, job_id: &str) -> Allocation {
        Allocation {
            id: id.to_owned(),
            eval_id: "e1".to_owned(),
            node_id: node_id.to_owned(),
            job_id: job_id.to_owned(),
            task_group: "g".to_owned(),
            desired_status: DesiredStatus::Run,
            client_status: ClientStatus::Running,
            resources: Resources::default(),
        }
    }

    fn eval(id: &str) -> Evaluation {
        Evaluation {
            id: id.to_owned(),
            job_id: "redis".to_owned(),
            priority: 50,
            trigger: EvalTrigger::JobRegister,
            status: EvalStatus::Pending,
        }
    }

    #[test]
    fn new_store_has_no_jobs() {
        assert!(StateStore::new().list_jobs().is_empty());
    }

    #[test]
    fn upsert_then_get_job() {
        let mut s = StateStore::new();
        s.upsert_job(job("redis")).unwrap();
        assert_eq!(s.get_job("redis").unwrap().name, "redis");
    }

    #[test]
    fn get_missing_job_is_none() {
        assert!(StateStore::new().get_job("nope").is_none());
    }

    #[test]
    fn upsert_replaces_job() {
        let mut s = StateStore::new();
        s.upsert_job(job("redis")).unwrap();
        s.upsert_job(job("redis")).unwrap();
        assert_eq!(s.list_jobs().len(), 1);
    }

    #[test]
    fn delete_job_removes_it() {
        let mut s = StateStore::new();
        s.upsert_job(job("redis")).unwrap();
        s.delete_job("redis").unwrap();
        assert!(s.get_job("redis").is_none());
    }

    #[test]
    fn delete_missing_job_errors() {
        let mut s = StateStore::new();
        assert!(s.delete_job("nope").is_err());
    }

    #[test]
    fn upsert_then_get_node() {
        let mut s = StateStore::new();
        s.upsert_node(node("n1")).unwrap();
        assert_eq!(s.get_node("n1").unwrap().id, "n1");
    }

    #[test]
    fn list_nodes_reflects_inserts() {
        let mut s = StateStore::new();
        s.upsert_node(node("n1")).unwrap();
        s.upsert_node(node("n2")).unwrap();
        assert_eq!(s.list_nodes().len(), 2);
    }

    #[test]
    fn allocs_filtered_by_node() {
        let mut s = StateStore::new();
        s.upsert_alloc(alloc("a1", "n1", "redis")).unwrap();
        s.upsert_alloc(alloc("a2", "n2", "redis")).unwrap();
        assert_eq!(s.allocs_by_node("n1").len(), 1);
    }

    #[test]
    fn allocs_filtered_by_job() {
        let mut s = StateStore::new();
        s.upsert_alloc(alloc("a1", "n1", "redis")).unwrap();
        s.upsert_alloc(alloc("a2", "n1", "web")).unwrap();
        assert_eq!(s.allocs_by_job("redis").len(), 1);
    }

    #[test]
    fn upsert_then_get_eval() {
        let mut s = StateStore::new();
        s.upsert_eval(eval("ev1")).unwrap();
        assert_eq!(s.get_eval("ev1").unwrap().id, "ev1");
    }
}
