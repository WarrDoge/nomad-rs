// SPDX-License-Identifier: Apache-2.0

//! Nomad scheduler — evaluation, ranking, and placement logic.
//!
//! The scheduler watches for pending evaluations, calculates placement
//! scores across candidate nodes, and produces allocation plans.

use crate::alloc::{Allocation, ClientStatus, DesiredStatus};
use crate::error::Result;
use crate::eval::Evaluation;
use crate::fsm::{Command, Fsm};
use crate::jobspec::{Resources, TaskGroup};
use crate::node::{Node, NodeStatus, SchedulingEligibility};
use crate::state::StateStore;

/// A set of placements the scheduler wants to apply for one evaluation.
#[derive(Debug, Default, Clone)]
pub struct Plan {
    /// Allocations to create.
    pub allocs: Vec<Allocation>,
}

/// Total resources a task group demands (sum of its tasks).
fn group_demand(group: &TaskGroup) -> Resources {
    group.tasks.iter().fold(Resources { cpu_mhz: 0, memory_mb: 0, network_mbps: 0 }, |acc, t| Resources {
        cpu_mhz: acc.cpu_mhz + t.resources.cpu_mhz,
        memory_mb: acc.memory_mb + t.resources.memory_mb,
        network_mbps: acc.network_mbps + t.resources.network_mbps,
    })
}

/// Free capacity on a node: total minus all non-terminal allocs placed on it.
fn free_capacity(node: &Node, state: &StateStore) -> Resources {
    state.allocs_by_node(node.id.as_str()).iter().filter(|a| is_live(a.client_status)).fold(
        node.resources,
        |free, a| Resources {
            cpu_mhz: free.cpu_mhz - a.resources.cpu_mhz,
            memory_mb: free.memory_mb - a.resources.memory_mb,
            network_mbps: free.network_mbps - a.resources.network_mbps,
        },
    )
}

/// An alloc reserves capacity until it reaches a terminal client status.
fn is_live(status: ClientStatus) -> bool {
    matches!(status, ClientStatus::Pending | ClientStatus::Running)
}

/// Whether a node is in a placeable state (ready, eligible, not draining).
fn node_eligible(node: &Node) -> bool {
    node.status == NodeStatus::Ready && node.eligibility == SchedulingEligibility::Eligible && !node.draining
}

/// Whether `avail` covers `need` across every tracked resource.
fn fits(avail: Resources, need: Resources) -> bool {
    avail.cpu_mhz >= need.cpu_mhz && avail.memory_mb >= need.memory_mb && avail.network_mbps >= need.network_mbps
}

/// Whether `node`'s attributes satisfy every hard constraint on `group`.
fn meets_constraints(node: &Node, group: &TaskGroup) -> bool {
    group.constraints.iter().all(|c| c.satisfied_by(&node.attributes))
}

/// Total instances the eval's job currently wants placed across its task
/// groups; `0` if the job is unknown (e.g. a post-deregister cleanup eval).
#[must_use]
pub fn desired_count(eval: &Evaluation, state: &StateStore) -> i32 {
    state.get_job(eval.job_id.as_str()).map_or(0, |j| j.task_groups.iter().map(|g| g.count.max(0)).sum())
}

/// Process one evaluation into a [`Plan`]: place each instance of each task
/// group on the first node that still has room, decrementing that node's
/// running free capacity as allocations are added so a node is never
/// oversubscribed (across counts or multiple groups).
///
/// ponytail: first-fit, no scoring/spread. Real ranking is backlog #1b.
#[must_use]
pub fn process_eval(eval: &Evaluation, state: &StateStore) -> Plan {
    let mut plan = Plan::default();
    let Some(job) = state.get_job(eval.job_id.as_str()) else { return plan };
    // Eligible nodes paired with their current free capacity; decremented as we
    // reserve placements within this plan.
    let mut free: Vec<(Node, Resources)> = state
        .list_nodes()
        .into_iter()
        .filter(node_eligible)
        .map(|n| {
            let avail = free_capacity(&n, state);
            (n, avail)
        })
        .collect();
    for group in &job.task_groups {
        let need = group_demand(group);
        for _ in 0..group.count.max(0) {
            let Some((node, avail)) =
                free.iter_mut().find(|(node, avail)| fits(*avail, need) && meets_constraints(node, group))
            else {
                break; // no node has room and satisfies constraints
            };
            plan.allocs.push(Allocation {
                id: format!("{}-{}", eval.id, plan.allocs.len()).into(),
                eval_id: eval.id.clone(),
                node_id: node.id.clone(),
                job_id: job.name.clone().into(),
                task_group: group.name.clone(),
                desired_status: DesiredStatus::Run,
                client_status: ClientStatus::Pending,
                resources: need,
            });
            avail.cpu_mhz -= need.cpu_mhz;
            avail.memory_mb -= need.memory_mb;
            avail.network_mbps -= need.network_mbps;
        }
    }
    plan
}

/// Apply a [`Plan`] by committing each placement to the FSM as an `UpsertAlloc`.
///
/// # Errors
///
/// Returns the first error from [`Fsm::apply`] (e.g. a rejected allocation).
///
/// ponytail: applies directly to the local FSM. Real Nomad routes plans through
/// the leader's plan applier over Raft — swap the apply target when raft lands.
pub fn apply_plan(fsm: &mut Fsm, plan: &Plan) -> Result<()> {
    for alloc in &plan.allocs {
        fsm.apply(Command::UpsertAlloc(alloc.clone()))?;
    }
    Ok(())
}

/// Process one evaluation against the FSM's state and commit the resulting
/// placements. Returns the [`Plan`] that was applied.
///
/// # Errors
///
/// Propagates any error from [`apply_plan`].
pub fn process_and_apply(eval: &Evaluation, fsm: &mut Fsm) -> Result<Plan> {
    let plan = process_eval(eval, fsm.state());
    apply_plan(fsm, &plan)?;
    Ok(plan)
}

/// Dequeue and process every pending evaluation in `queue`, applying each plan
/// to `fsm`. Returns the total number of allocations placed.
///
/// # Errors
///
/// Propagates the first dequeue or apply error.
///
/// Each eval is `ack`ed on successful apply or `nack`ed (re-enqueued, up to the
/// delivery cap) if applying it errors.
///
/// ponytail: synchronous drain — one pass. The async worker loop with leader
/// leasing is backlog #8.
pub fn drain_queue(queue: &crate::eval_queue::EvalQueue, fsm: &mut Fsm) -> Result<usize> {
    let mut placed = 0;
    while let Some(eval) = queue.dequeue()? {
        match process_and_apply(&eval, fsm) {
            Ok(plan) => {
                placed += plan.allocs.len();
                // Wanted placement but got none → park as blocked for retry.
                if plan.allocs.is_empty() && desired_count(&eval, fsm.state()) > 0 {
                    queue.block(eval.clone())?;
                }
                queue.ack(eval.id.as_str())?;
            },
            Err(e) => {
                queue.nack(eval.id.as_str())?;
                return Err(e);
            },
        }
    }
    Ok(placed)
}

/// The possible states a scheduler can be in.
pub use crate::agent::AgentStatus as SchedulerStatus;

/// The core scheduler responsible for placing tasks onto nodes.
#[derive(Debug)]
pub struct Scheduler {
    /// Whether the scheduler is currently running.
    status: SchedulerStatus,
}

impl Scheduler {
    /// Create a new scheduler instance.
    #[must_use]
    pub fn new() -> Self {
        Self { status: SchedulerStatus::Initialized }
    }

    /// Returns the current status of the scheduler.
    #[must_use]
    pub fn status(&self) -> SchedulerStatus {
        self.status
    }

    /// Returns `true` if the scheduler is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == SchedulerStatus::Running
    }

    /// Run the scheduler evaluation loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the scheduling loop encounters a fatal error.
    #[allow(clippy::unused_async)]
    pub async fn run(&mut self) -> Result<()> {
        if self.status == SchedulerStatus::Running {
            return Ok(());
        }
        self.status = SchedulerStatus::Running;
        tracing::info!("scheduler starting");
        // TODO: implement the bin-packing scheduler loop
        Ok(())
    }

    /// Gracefully stop the scheduler.
    pub fn stop(&mut self) {
        self.status = SchedulerStatus::Stopped;
        tracing::info!("scheduler stopped");
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_new() {
        let scheduler = Scheduler::new();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_scheduler_default() {
        let scheduler = Scheduler::default();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
    }

    #[tokio::test]
    async fn test_scheduler_run() {
        let mut scheduler = Scheduler::new();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
        let result = scheduler.run().await;
        assert!(result.is_ok());
        assert!(scheduler.is_running());
        assert_eq!(scheduler.status(), SchedulerStatus::Running);
    }

    #[tokio::test]
    async fn test_scheduler_run_idempotent() {
        let mut scheduler = Scheduler::new();
        let _ = scheduler.run().await;
        assert!(scheduler.is_running());
        let result = scheduler.run().await;
        assert!(result.is_ok());
        assert!(scheduler.is_running());
    }

    #[tokio::test]
    async fn test_scheduler_stop() {
        let mut scheduler = Scheduler::new();
        let _ = scheduler.run().await;
        assert!(scheduler.is_running());
        scheduler.stop();
        assert_eq!(scheduler.status(), SchedulerStatus::Stopped);
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_scheduler_stop_before_run() {
        let mut scheduler = Scheduler::new();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
        scheduler.stop();
        assert_eq!(scheduler.status(), SchedulerStatus::Stopped);
    }

    use crate::alloc::DesiredStatus;
    use crate::eval::{EvalStatus, EvalTrigger, Evaluation};
    use crate::jobspec::{Job, Resources, Task, TaskGroup};
    use crate::node::{Node, NodeStatus, SchedulingEligibility};
    use crate::state::StateStore;
    use std::collections::HashMap;

    fn node_with(id: &str, cpu: i32, mem: i32) -> Node {
        Node {
            id: id.into(),
            name: "n".to_owned(),
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

    fn job_with(name: &str, group: &str, cpu: i32, mem: i32) -> Job {
        let task = Task {
            name: "t".to_owned(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources { cpu_mhz: cpu, memory_mb: mem, network_mbps: 0 },
        };
        Job {
            name: name.to_owned(),
            task_groups: vec![TaskGroup { name: group.to_owned(), count: 1, tasks: vec![task], constraints: vec![] }],
            ..Job::default()
        }
    }

    fn eval_for(job: &str) -> Evaluation {
        eval_for_id("e1", job)
    }

    fn eval_for_id(id: &str, job: &str) -> Evaluation {
        Evaluation {
            id: id.into(),
            job_id: job.into(),
            priority: 50,
            trigger: EvalTrigger::JobRegister,
            status: EvalStatus::Pending,
        }
    }

    #[test]
    fn process_eval_places_alloc_on_feasible_node() {
        let mut state = StateStore::new();
        state.upsert_node(node_with("node1", 1000, 1024)).unwrap();
        state.upsert_job(job_with("web", "g1", 500, 512)).unwrap();

        let plan = process_eval(&eval_for("web"), &state);

        assert_eq!(plan.allocs.len(), 1);
        let alloc = &plan.allocs[0];
        assert_eq!(alloc.node_id, "node1");
        assert_eq!(alloc.job_id, "web");
        assert_eq!(alloc.task_group, "g1");
        assert_eq!(alloc.eval_id, "e1");
        assert_eq!(alloc.desired_status, DesiredStatus::Run);
    }

    #[test]
    fn process_eval_empty_when_no_node_fits() {
        let mut state = StateStore::new();
        state.upsert_node(node_with("node1", 100, 128)).unwrap();
        state.upsert_job(job_with("web", "g1", 500, 512)).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert!(plan.allocs.is_empty());
    }

    #[test]
    fn process_eval_empty_when_job_missing() {
        let state = StateStore::new();
        let plan = process_eval(&eval_for("ghost"), &state);
        assert!(plan.allocs.is_empty());
    }

    #[test]
    fn process_eval_subtracts_existing_allocs_from_capacity() {
        let mut state = StateStore::new();
        state.upsert_node(node_with("node1", 1000, 1024)).unwrap();
        // Running alloc already consumes most of node1.
        state
            .upsert_alloc(Allocation {
                id: "old".into(),
                eval_id: "e0".into(),
                node_id: "node1".into(),
                job_id: "other".into(),
                task_group: "g".to_owned(),
                desired_status: DesiredStatus::Run,
                client_status: ClientStatus::Running,
                resources: Resources { cpu_mhz: 800, memory_mb: 800, network_mbps: 0 },
            })
            .unwrap();
        // Needs 500/512 — only 200/224 free → must not place.
        state.upsert_job(job_with("web", "g1", 500, 512)).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert!(plan.allocs.is_empty());
    }

    #[test]
    fn process_eval_ignores_terminal_allocs_for_capacity() {
        let mut state = StateStore::new();
        state.upsert_node(node_with("node1", 1000, 1024)).unwrap();
        // A completed alloc should NOT reserve capacity.
        state
            .upsert_alloc(Allocation {
                id: "done".into(),
                eval_id: "e0".into(),
                node_id: "node1".into(),
                job_id: "other".into(),
                task_group: "g".to_owned(),
                desired_status: DesiredStatus::Stop,
                client_status: ClientStatus::Complete,
                resources: Resources { cpu_mhz: 800, memory_mb: 800, network_mbps: 0 },
            })
            .unwrap();
        state.upsert_job(job_with("web", "g1", 500, 512)).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert_eq!(plan.allocs.len(), 1);
    }

    #[test]
    fn process_eval_emits_one_alloc_per_count() {
        let mut state = StateStore::new();
        state.upsert_node(node_with("node1", 1000, 1024)).unwrap();
        let mut job = job_with("web", "g1", 100, 128);
        job.task_groups[0].count = 3;
        state.upsert_job(job).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert_eq!(plan.allocs.len(), 3);
        assert!(plan.allocs.iter().all(|a| a.node_id == "node1"));
    }

    #[test]
    fn process_and_apply_writes_allocs_to_state() {
        let mut fsm = Fsm::new();
        fsm.apply(Command::UpsertNode(node_with("node1", 1000, 1024))).unwrap();
        fsm.apply(Command::UpsertJob(job_with("web", "g1", 500, 512))).unwrap();

        let plan = process_and_apply(&eval_for("web"), &mut fsm).unwrap();

        assert_eq!(plan.allocs.len(), 1);
        assert_eq!(fsm.state().allocs_by_node("node1").len(), 1);
        assert_eq!(fsm.state().allocs_by_job("web").len(), 1);
    }

    #[test]
    fn drain_queue_processes_all_pending_evals() {
        use crate::eval_queue::EvalQueue;
        let mut fsm = Fsm::new();
        fsm.apply(Command::UpsertNode(node_with("node1", 1000, 1024))).unwrap();
        fsm.apply(Command::UpsertJob(job_with("a", "g", 100, 100))).unwrap();
        fsm.apply(Command::UpsertJob(job_with("b", "g", 100, 100))).unwrap();
        let queue = EvalQueue::new();
        queue.enqueue(eval_for_id("ea", "a")).unwrap();
        queue.enqueue(eval_for_id("eb", "b")).unwrap();

        let placed = drain_queue(&queue, &mut fsm).unwrap();

        assert_eq!(placed, 2, "both evals placed one alloc each");
        assert!(queue.dequeue().unwrap().is_none(), "queue drained");
        assert_eq!(fsm.state().list_allocs().len(), 2);
    }

    #[test]
    fn drain_queue_blocks_unplaceable_eval() {
        use crate::eval_queue::EvalQueue;
        let mut fsm = Fsm::new();
        fsm.apply(Command::UpsertNode(node_with("node1", 100, 128))).unwrap();
        // Job needs more than the only node has → cannot place.
        fsm.apply(Command::UpsertJob(job_with("big", "g", 500, 512))).unwrap();
        let queue = EvalQueue::new();
        queue.enqueue(eval_for_id("e", "big")).unwrap();

        let placed = drain_queue(&queue, &mut fsm).unwrap();

        assert_eq!(placed, 0);
        assert_eq!(queue.blocked_len(), 1, "unplaceable eval parked as blocked");
        assert_eq!(queue.in_flight_len(), 0, "and acked off the in-flight set");
    }

    #[test]
    fn drain_queue_does_not_block_when_job_absent() {
        use crate::eval_queue::EvalQueue;
        let mut fsm = Fsm::new();
        let queue = EvalQueue::new();
        // No such job (e.g. a post-deregister cleanup eval) → nothing to block.
        queue.enqueue(eval_for_id("e", "ghost")).unwrap();
        drain_queue(&queue, &mut fsm).unwrap();
        assert_eq!(queue.blocked_len(), 0);
    }

    #[test]
    fn process_and_apply_second_eval_respects_first_placement() {
        // Two evals for two single-instance jobs onto one node with room for one.
        let mut fsm = Fsm::new();
        fsm.apply(Command::UpsertNode(node_with("node1", 600, 600))).unwrap();
        fsm.apply(Command::UpsertJob(job_with("a", "g", 500, 500))).unwrap();
        fsm.apply(Command::UpsertJob(job_with("b", "g", 500, 500))).unwrap();

        let first = process_and_apply(&eval_for("a"), &mut fsm).unwrap();
        let second = process_and_apply(&eval_for("b"), &mut fsm).unwrap();

        assert_eq!(first.allocs.len(), 1, "first job placed");
        assert!(second.allocs.is_empty(), "second job has no capacity left");
        assert_eq!(fsm.state().allocs_by_node("node1").len(), 1);
    }

    #[test]
    fn process_eval_does_not_oversubscribe_node_for_count() {
        // Node fits 2 instances of 250/250, not 3.
        let mut state = StateStore::new();
        state.upsert_node(node_with("node1", 600, 600)).unwrap();
        let mut job = job_with("web", "g1", 250, 250);
        job.task_groups[0].count = 3;
        state.upsert_job(job).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert_eq!(plan.allocs.len(), 2);
    }

    #[test]
    fn process_eval_enforces_network_demand() {
        // Node has no network capacity; group needs some → cannot place.
        let mut state = StateStore::new();
        state.upsert_node(node_with("node1", 1000, 1024)).unwrap();
        let mut job = job_with("web", "g1", 100, 128);
        job.task_groups[0].tasks[0].resources.network_mbps = 10;
        state.upsert_job(job).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert!(plan.allocs.is_empty());
    }

    #[test]
    fn process_eval_skips_node_failing_constraint() {
        use crate::constraint::Constraint;
        let mut state = StateStore::new();
        let mut node = node_with("win1", 1000, 1024);
        node.attributes.insert("os".to_owned(), "windows".to_owned());
        state.upsert_node(node).unwrap();
        let mut job = job_with("web", "g1", 100, 128);
        job.task_groups[0].constraints =
            vec![Constraint { left: "os".to_owned(), right: "linux".to_owned(), operand: "=".to_owned() }];
        state.upsert_job(job).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert!(plan.allocs.is_empty(), "windows node fails os=linux constraint");
    }

    #[test]
    fn process_eval_steers_to_constraint_satisfying_node() {
        use crate::constraint::Constraint;
        let mut state = StateStore::new();
        let mut win = node_with("win1", 1000, 1024);
        win.attributes.insert("os".to_owned(), "windows".to_owned());
        let mut lin = node_with("lin1", 1000, 1024);
        lin.attributes.insert("os".to_owned(), "linux".to_owned());
        state.upsert_node(win).unwrap();
        state.upsert_node(lin).unwrap();
        let mut job = job_with("web", "g1", 100, 128);
        job.task_groups[0].constraints =
            vec![Constraint { left: "os".to_owned(), right: "linux".to_owned(), operand: "=".to_owned() }];
        state.upsert_job(job).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert_eq!(plan.allocs.len(), 1);
        assert_eq!(plan.allocs[0].node_id, "lin1", "placed only on the linux node");
    }

    #[test]
    fn process_eval_skips_ineligible_and_down_nodes() {
        let mut down = node_with("down1", 1000, 1024);
        down.status = NodeStatus::Down;
        let mut inelig = node_with("inelig1", 1000, 1024);
        inelig.eligibility = SchedulingEligibility::Ineligible;
        let mut draining = node_with("drain1", 1000, 1024);
        draining.draining = true;
        let mut state = StateStore::new();
        state.upsert_node(down).unwrap();
        state.upsert_node(inelig).unwrap();
        state.upsert_node(draining).unwrap();
        state.upsert_job(job_with("web", "g1", 100, 128)).unwrap();
        let plan = process_eval(&eval_for("web"), &state);
        assert!(plan.allocs.is_empty());
    }
}
