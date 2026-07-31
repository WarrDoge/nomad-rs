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

use std::time::Duration;
use tokio::sync::watch;

/// How long the evaluation loop pauses when the queue is empty before
/// checking for new work or shutdown. 100 ms gives 10 wakeups/s on an
/// idle leader — negligible for a single-node scheduler.
const IDLE_SLEEP: Duration = Duration::from_millis(100);

/// A set of placements the scheduler wants to apply for one evaluation.
#[derive(Debug, Default, Clone)]
pub struct Plan {
    /// Allocations to create.
    pub allocs: Vec<Allocation>,
}

/// A weighted node candidate for placement, produced by ranking.
#[derive(Debug, Clone)]
struct Candidate {
    /// The candidate node.
    node: Node,
    /// Free capacity on this node after existing allocs.
    #[allow(dead_code)]
    free: Resources,
    /// Composite score (higher = better).
    score: f64,
    /// Original index in the candidate list (tiebreaker -> stable order).
    order: usize,
}

/// Compute a composite score for placing one instance of `group` on `node`.
///
/// Components (lower-is-better components are negated so the composite is
/// always higher-is-better):
///
///   **Affinity** — each satisfied affinity adds its `weight` to the score.
///     (weights are in `-100..=100` so the range is proportional.)
///
///   **Bin-pack** — a small positive nudge for nodes whose free capacity
///     *after placement* is proportionally tighter (prefer consolidating
///     rather than spreading thin across many nodes). Computed as the
///     utilization fraction after placing the workload.
///
///   **Spread** — computed externally via [`SpreadRanker`] and injected.
///
/// ponytail: simple additive model. Real Nomad uses a more sophisticated
/// normalized scoring system with separate rank methods (bin-pack, spread,
/// affinity) and penalty-based tiebreaking.
#[must_use]
fn score_node(node: &Node, free: &Resources, need: &Resources, group: &TaskGroup, spread_bonus: f64) -> f64 {
    let mut score = 0.0;

    // Affinity: sum weight of each satisfied affinity.
    for affinity in &group.affinities {
        if affinity.satisfied_by(&node.attributes) {
            score += f64::from(affinity.weight);
        }
    }

    // Bin-pack: prefer higher utilization after placing (consolidate).
    let after_cpu = free.cpu_mhz - need.cpu_mhz;
    let after_mem = free.memory_mb - need.memory_mb;
    let after_net = free.network_mbps - need.network_mbps;
    if after_cpu > 0 || after_mem > 0 || after_net > 0 {
        let util_cpu = if node.resources.cpu_mhz > 0 {
            f64::from(node.resources.cpu_mhz - after_cpu) / f64::from(node.resources.cpu_mhz)
        } else {
            0.0
        };
        let util_mem = if node.resources.memory_mb > 0 {
            f64::from(node.resources.memory_mb - after_mem) / f64::from(node.resources.memory_mb)
        } else {
            0.0
        };
        let util_net = if node.resources.network_mbps > 0 {
            f64::from(node.resources.network_mbps - after_net) / f64::from(node.resources.network_mbps)
        } else {
            0.0
        };
        // Bin-pack bonus: max 50 for full utilization, scaled.
        let avg_util = (util_cpu + util_mem + util_net) / 3.0;
        score += avg_util * 50.0;
    }

    // Spread bonus (from SpreadRanker, may be negative).
    score += spread_bonus;

    score
}

/// Tracks per-attribute-value allocation counts for spread scoring.
struct SpreadRanker<'a> {
    /// The task group whose spread targets drive the scoring.
    group: &'a TaskGroup,
}

impl<'a> SpreadRanker<'a> {
    /// Create a new ranker for the given task group.
    const fn new(group: &'a TaskGroup) -> Self {
        Self { group }
    }

    /// Compute a spread bonus for placing on `node` given the current
    /// allocation distribution in `state`.
    ///
    /// For each spread target in the group, awards a positive bonus when
    /// the node's attribute value is *under-represented* (below its target
    /// percent) and a penalty when over-represented.
    #[must_use]
    #[allow(clippy::cast_precision_loss, reason = "cluster sizes fit in f64 mantissa")]
    fn bonus_for(&self, node: &Node, state: &StateStore) -> f64 {
        let mut bonus = 0.0;
        for spread in &self.group.spreads {
            let attr_val = node.attributes.get(spread.attribute.as_str());
            let Some(attr_val) = attr_val else { continue };

            // Total non-terminal allocs in the cluster.
            let total_live = state.list_allocs().iter().filter(|a| is_live(a.client_status)).count();
            if total_live == 0 {
                continue;
            }

            // Allocs on nodes with the same attribute value.
            let same_val = state
                .list_allocs()
                .iter()
                .filter(|a| {
                    if !is_live(a.client_status) {
                        return false;
                    }
                    let n = state.get_node(a.node_id.as_str());
                    n.is_some_and(|n| n.attributes.get(spread.attribute.as_str()) == Some(attr_val))
                })
                .count();

            let current_pct = (same_val as f64 / total_live as f64) * 100.0;

            // Sum target percent for this value across all targets.
            let target_pct: f64 =
                spread.targets.iter().filter(|t| t.value == *attr_val).map(|t| f64::from(t.percent)).sum();

            // If no explicit target, assume even distribution over unique values.
            let target_pct = if target_pct > 0.0 {
                target_pct
            } else {
                // Guess: share remaining percent equally among untargeted values.
                let unique_vals = state
                    .list_nodes()
                    .iter()
                    .filter_map(|n| n.attributes.get(spread.attribute.as_str()))
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    .max(1);
                100.0 / unique_vals as f64
            };

            let deviation = current_pct - target_pct;
            // Penalty for being over-represented, bonus for under-represented.
            // Max magnitude ~50 (at 50% deviation).
            bonus -= deviation * 1.0;
        }
        bonus
    }
}
/// Sum of all tasks' resource demands within a task group.
#[must_use]
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
const fn is_live(status: ClientStatus) -> bool {
    matches!(status, ClientStatus::Pending | ClientStatus::Running)
}

/// Whether a node is in a placeable state (ready, eligible, not draining).
fn node_eligible(node: &Node) -> bool {
    node.status == NodeStatus::Ready && node.eligibility == SchedulingEligibility::Eligible && !node.draining
}

/// Whether `avail` covers `need` across every tracked resource.
const fn fits(avail: Resources, need: Resources) -> bool {
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

/// Process one evaluation into a [`Plan`]: score eligible nodes by
/// bin-pack utilization, affinity weights, and spread targets, then place
/// each instance on the best-scoring node that has room.
///
/// ponytail: simple weighted-sum scoring. Real Nomad uses normalized
/// rank methods with penalty-based tiebreaking.
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
        // Build ranked candidates for this group.
        let has_affinities = !group.affinities.is_empty();
        let has_spreads = !group.spreads.is_empty();
        if !has_affinities && !has_spreads {
            // Fast path: no scoring needed — first-fit with decrement.
            for _ in 0..group.count.max(0) {
                let Some((node, avail)) =
                    free.iter_mut().find(|(node, avail)| fits(*avail, need) && meets_constraints(node, group))
                else {
                    break;
                };
                plan.allocs.push(make_alloc(eval, &plan, &job, group, need, node.id.as_str()));
                avail.cpu_mhz -= need.cpu_mhz;
                avail.memory_mb -= need.memory_mb;
                avail.network_mbps -= need.network_mbps;
            }
        } else {
            // Scoring path: rank candidates, try best first.
            let spread_ranker = SpreadRanker::new(group);
            for _ in 0..group.count.max(0) {
                let mut scored: Vec<Candidate> = free
                    .iter()
                    .enumerate()
                    .filter(|(_, (node, avail))| fits(*avail, need) && meets_constraints(node, group))
                    .map(|(idx, (node, avail))| {
                        let spread_bonus = spread_ranker.bonus_for(node, state);
                        Candidate {
                            node: node.clone(),
                            free: *avail,
                            score: score_node(node, avail, &need, group, spread_bonus),
                            order: idx,
                        }
                    })
                    .collect();
                // Sort descending by score, then by original order for stability.
                scored.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.order.cmp(&b.order)));
                let Some(best) = scored.into_iter().next() else { break };
                // Locate the matching node in free and decrement its capacity.
                let Some((_, old_avail)) = free.iter_mut().find(|(n, _)| n.id.as_str() == best.node.id.as_str()) else {
                    continue;
                };
                old_avail.cpu_mhz -= need.cpu_mhz;
                old_avail.memory_mb -= need.memory_mb;
                old_avail.network_mbps -= need.network_mbps;
                plan.allocs.push(make_alloc(eval, &plan, &job, group, need, best.node.id.as_str()));
            }
        }
    }
    plan
}

/// Build a single placement allocation for a task-group instance.
#[must_use]
fn make_alloc(
    eval: &Evaluation,
    plan: &Plan,
    job: &crate::jobspec::Job,
    group: &TaskGroup,
    need: Resources,
    node_id: &str,
) -> Allocation {
    Allocation {
        id: format!("{}-{}", eval.id, plan.allocs.len()).into(),
        eval_id: eval.id.clone(),
        node_id: node_id.to_owned().into(),
        job_id: job.name.clone().into(),
        task_group: group.name.clone(),
        desired_status: DesiredStatus::Run,
        client_status: ClientStatus::Pending,
        resources: need,
    }
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
    /// Sender side of a watch channel. When `stop()` is called, sends `true`
    /// to wake the `run()` loop. Chosen over `AtomicBool` because the watch
    /// receiver provides `wait_for()` — a native async primitive that avoids
    /// polling (busy-spinning on an empty queue) while still reacting
    /// instantly to shutdown, without needing a companion condvar or timer.
    shutdown_tx: watch::Sender<bool>,
}

impl Scheduler {
    /// Create a new scheduler instance.
    #[must_use]
    pub fn new() -> Self {
        let (shutdown_tx, _rx) = watch::channel(false);
        Self { status: SchedulerStatus::Initialized, shutdown_tx }
    }

    /// Returns the current status of the scheduler.
    #[must_use]
    pub const fn status(&self) -> SchedulerStatus {
        self.status
    }

    /// Returns `true` if the scheduler is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == SchedulerStatus::Running
    }

    /// A handle that can stop the scheduler from another task.
    ///
    /// Returns a [`watch::Sender`] clone: sending `true` wakes the `run()` loop
    /// and causes it to exit. Useful for tests that need to stop a scheduler
    /// running in a spawned task.
    #[must_use]
    pub fn shutdown_handle(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    /// Run the scheduler evaluation loop.
    ///
    /// Drains pending evaluations from `queue` by passing them through
    /// [`drain_queue`], then sleeps until new work arrives or [`stop`] is
    /// called. Returns `Ok` when stopped normally.
    ///
    /// # Errors
    ///
    /// Returns an error if the scheduling loop encounters a fatal error
    /// (e.g. a poisoned eval queue mutex or an FSM apply failure).
    pub async fn run(&mut self, queue: &crate::eval_queue::EvalQueue, fsm: &mut Fsm) -> Result<()> {
        if self.status == SchedulerStatus::Running {
            return Ok(());
        }
        self.status = SchedulerStatus::Running;
        tracing::info!("scheduler starting");

        // Subscribe fresh so the loop always has a live receiver.
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            // Drain any pending evaluations. Errors here (poisoned mutex,
            // apply failure) are fatal and abort the loop.
            if let Err(e) = drain_queue(queue, fsm) {
                self.status = SchedulerStatus::Stopped;
                return Err(e);
            }

            // Wait for either the shutdown signal or the idle interval.
            // `wait_for` returns immediately if the value is already `true`,
            // so an already-signalled stop exits on the next iteration.
            let should_stop = tokio::select! {
                _ = shutdown_rx.wait_for(|&b| b) => true,
                () = tokio::time::sleep(IDLE_SLEEP) => false,
            };

            if should_stop {
                break;
            }
        }

        self.status = SchedulerStatus::Stopped;
        tracing::info!("scheduler stopped");
        Ok(())
    }

    /// Gracefully stop the scheduler.
    ///
    /// Sends the shutdown signal to wake a running loop and flips the status.
    /// Idempotent — safe to call multiple times.
    pub fn stop(&mut self) {
        if self.status == SchedulerStatus::Stopped {
            return;
        }
        // Send `true` on the watch to wake the `run()` loop's `wait_for`.
        // `send` returns an error only when all receivers are dropped — we
        // ignore it because a dropped receiver means the loop already exited.
        let _ = self.shutdown_tx.send(true);
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
#[allow(
    clippy::missing_docs_in_private_items,
    clippy::wildcard_imports,
    clippy::unwrap_used,
    reason = "conventional inline test module"
)]
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

    #[test]
    fn test_scheduler_stop_before_run() {
        let mut scheduler = Scheduler::new();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
        scheduler.stop();
        assert_eq!(scheduler.status(), SchedulerStatus::Stopped);
    }

    // ---- Scheduler run-loop tests (requirement 5) -------------------------
    // The three old tests (test_scheduler_run, test_scheduler_run_idempotent,
    // test_scheduler_stop_from_another_task) tested stub behavior where
    // run() returned immediately. Now run() loops until stopped; the three
    // tests below cover the loop.

    use std::sync::Arc;
    use tokio::sync::Mutex as TokioMutex;

    /// Helper: spawn the scheduler loop in a background task with a separate
    /// stop handle so the test can control shutdown precisely. The FSM is
    /// shared via `Arc<tokio::sync::Mutex<Fsm>>` so the test can inspect
    /// state after the loop runs (the `tokio::sync::Mutex` guard is `Send`,
    /// unlike `std::sync::MutexGuard`).
    struct SchedulerTest {
        queue: crate::eval_queue::EvalQueue,
        fsm: Arc<TokioMutex<Fsm>>,
    }

    impl SchedulerTest {
        fn new() -> Self {
            Self { queue: crate::eval_queue::EvalQueue::new(), fsm: Arc::new(TokioMutex::new(Fsm::new())) }
        }

        /// Apply a command to the shared FSM.
        async fn apply(&self, cmd: Command) {
            self.fsm.lock().await.apply(cmd).unwrap();
        }

        /// Spawn `scheduler.run()` in a background task. Returns the
        /// scheduler's [`watch::Sender`] so the test can stop it remotely,
        /// and a [`tokio::task::JoinHandle`] that resolves to the run result.
        async fn spawn_run(&self) -> (watch::Sender<bool>, tokio::task::JoinHandle<Result<()>>) {
            let mut scheduler = Scheduler::new();
            let stop_tx = scheduler.shutdown_handle();
            let queue = self.queue.clone();
            let fsm = Arc::clone(&self.fsm);

            let handle = tokio::spawn(async move {
                let mut fsm_guard = fsm.lock().await;
                scheduler.run(&queue, &mut fsm_guard).await
            });

            // Yield once to let the spawned task make progress.
            tokio::task::yield_now().await;
            (stop_tx, handle)
        }
    }

    #[tokio::test]
    async fn run_loop_processes_queued_evals() {
        let t = SchedulerTest::new();
        // Register a node and two jobs in the FSM.
        t.apply(Command::UpsertNode(node_with("n1", 1000, 1024))).await;
        t.apply(Command::UpsertJob(job_with("a", "g", 100, 100))).await;
        t.apply(Command::UpsertJob(job_with("b", "g", 100, 100))).await;
        // Enqueue evals for both jobs.
        t.queue.enqueue(eval_for_id("ea", "a")).unwrap();
        t.queue.enqueue(eval_for_id("eb", "b")).unwrap();

        let (stop_tx, handle) = t.spawn_run().await;

        // Give the loop time to drain the queue.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Stop the scheduler.
        assert!(stop_tx.send(true).is_ok());
        handle.await.unwrap().unwrap();

        // Verify both evals were placed.
        assert_eq!(t.fsm.lock().await.state().list_allocs().len(), 2, "both evals placed an alloc");
        assert!(t.queue.dequeue().unwrap().is_none(), "eval queue drained");
    }

    #[tokio::test]
    async fn run_loop_exits_on_stop() {
        let t = SchedulerTest::new();
        let (stop_tx, handle) = t.spawn_run().await;

        // Stop immediately.
        assert!(stop_tx.send(true).is_ok());

        // The run loop should exit quickly (before the 5s timeout).
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("scheduler did not exit within 5s of stop signal")
            .expect("scheduler task panicked");

        assert!(result.is_ok(), "run() returned Ok on clean stop");
    }

    #[tokio::test]
    async fn run_loop_does_not_spin_on_empty_queue() {
        let t = SchedulerTest::new();
        let (stop_tx, handle) = t.spawn_run().await;

        // Let the loop run for a few idle cycles to see if it pegs a core.
        // Each idle cycle sleeps 100 ms; 350 ms = ~3 cycles.
        tokio::time::sleep(Duration::from_millis(350)).await;

        // Stop the scheduler. If it were busy-spinning it would have been
        // pegging a core for 350ms — but we cannot assert CPU usage from
        // here. Instead, verify that after stopping, the handle resolves
        // promptly (no hung loop).
        assert!(stop_tx.send(true).is_ok());
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("scheduler did not exit within 5s of stop signal")
            .expect("scheduler task panicked");

        assert!(result.is_ok(), "run() returned Ok on clean stop");
    }

    // ---- Existing pure-function tests (unchanged) ---------------------------

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
            task_groups: vec![TaskGroup {
                name: group.to_owned(),
                count: 1,
                tasks: vec![task],
                constraints: vec![],
                affinities: vec![],
                spreads: vec![],
            }],
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
