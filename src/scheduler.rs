// SPDX-License-Identifier: Apache-2.0

//! Nomad scheduler — evaluation, feasibility, ranking, and placement.
//!
//! The scheduler dequeues evaluations, finds feasible nodes for each task
//! group, ranks them (bin-packing), and emits a [`Plan`] of allocations to
//! create and evict. Behaviour is specified by the tests; the logic is
//! unimplemented.

use crate::alloc::Allocation;
use crate::error::Result;
use crate::eval::Evaluation;
use crate::jobspec::Resources;
use crate::node::Node;
use crate::state::StateStore;

/// The output of processing one evaluation: allocations to create and existing
/// allocations to evict.
#[derive(Debug, Clone)]
pub struct Plan {
    /// Evaluation this plan was produced for.
    pub eval_id: String,
    /// Allocations the scheduler wants to create (each names its target node).
    pub allocations: Vec<Allocation>,
    /// Ids of existing allocations to evict (e.g. for preemption).
    pub evictions: Vec<String>,
}

/// Whether `node` has enough free capacity to satisfy `required`.
///
/// Compares CPU, memory, and network across the node's advertised resources.
#[must_use]
pub fn node_fits(node: &Node, required: &Resources) -> bool {
    let _ = required;
    todo!("true iff node {:?} advertises >= required cpu/memory/network", node.id)
}

/// The core scheduler that turns evaluations into placement plans.
#[derive(Debug, Default)]
pub struct Scheduler;

impl Scheduler {
    /// Create a scheduler.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Process one evaluation against the current cluster state, producing a
    /// placement [`Plan`].
    ///
    /// # Errors
    ///
    /// Returns an error if the evaluation references a missing job or the
    /// cluster has no feasible placement and the eval cannot be blocked.
    pub fn process_eval(&self, eval: &Evaluation, state: &StateStore) -> Result<Plan> {
        let _ = state;
        todo!("find feasible nodes, rank them, and build a Plan for eval {:?}", eval.id)
    }

    /// Run the scheduler evaluation loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the scheduling loop encounters a fatal error.
    #[allow(clippy::unused_async, reason = "awaits eval queue once implemented")]
    pub async fn run(&mut self) -> Result<()> {
        todo!("drive the evaluation queue: dequeue, schedule, submit plans")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::eval::{EvalStatus, EvalTrigger};
    use crate::node::{NodeStatus, SchedulingEligibility};
    use std::collections::HashMap;

    fn node_with(cpu: i32, mem: i32) -> Node {
        Node {
            id: "n1".to_owned(),
            name: "n1".to_owned(),
            datacenter: "dc1".to_owned(),
            node_class: String::new(),
            resources: Resources { cpu_mhz: cpu, memory_mb: mem, network_mbps: 100 },
            status: NodeStatus::Ready,
            eligibility: SchedulingEligibility::Eligible,
            draining: false,
            attributes: HashMap::new(),
            drivers: HashMap::new(),
        }
    }

    fn eval() -> Evaluation {
        Evaluation {
            id: "ev1".to_owned(),
            job_id: "redis".to_owned(),
            priority: 50,
            trigger: EvalTrigger::JobRegister,
            status: EvalStatus::Pending,
        }
    }

    #[test]
    fn ample_node_fits() {
        let big = node_with(4000, 8192);
        let need = Resources { cpu_mhz: 500, memory_mb: 256, network_mbps: 10 };
        assert!(node_fits(&big, &need));
    }

    #[test]
    fn starved_node_does_not_fit() {
        let small = node_with(100, 64);
        let need = Resources { cpu_mhz: 500, memory_mb: 256, network_mbps: 10 };
        assert!(!node_fits(&small, &need));
    }

    #[test]
    fn process_eval_targets_the_eval() {
        let scheduler = Scheduler::new();
        let plan = scheduler.process_eval(&eval(), &StateStore::new()).unwrap();
        assert_eq!(plan.eval_id, "ev1");
    }
}
