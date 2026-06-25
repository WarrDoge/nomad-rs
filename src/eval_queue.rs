// SPDX-License-Identifier: Apache-2.0

//! Priority eval queue — the central point where evaluations wait to be
//! picked up by a scheduler worker.  Evals enter via `enqueue` (called by
//! `RpcEndpoint` on job register/deregister) and leave via `dequeue` (polled
//! by the scheduler loop).  Queue order is descending by priority, then FIFO
//! within the same priority level.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, MutexGuard};

use crate::error::Result;
use crate::eval::Evaluation;

/// Maximum number of times an eval is delivered before a `nack` drops it
/// instead of re-enqueuing (upstream Nomad's `MAX_DEQUEUE`, default 3). Guards
/// against an eval that crashes every worker looping forever.
const MAX_DEQUEUE: u32 = 3;

/// A pending evaluation wrapper that orders by priority (high first) then by
/// insertion order (FIFO for equal priorities).
#[derive(Debug, Clone)]
struct PendingEval {
    /// Sequence number — monotonically increasing insertion counter used as a
    /// tie-breaker for equal-priority items so a `BinaryHeap` stays stable-FIFO.
    seq: u64,
    /// The evaluation.
    eval: Evaluation,
    /// How many times this eval has been handed out via `dequeue`. Carried
    /// across `nack` re-enqueues so the delivery cap is enforced. Does not
    /// affect ordering.
    dequeues: u32,
}

impl Eq for PendingEval {}

impl PartialEq for PendingEval {
    fn eq(&self, other: &Self) -> bool {
        self.eval.priority == other.eval.priority && self.seq == other.seq
    }
}

impl PartialOrd for PendingEval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingEval {
    fn cmp(&self, other: &Self) -> Ordering {
        // Rust's BinaryHeap is a max-heap; we want highest priority first,
        // and for equal priorities the oldest (lowest seq) first.
        match self.eval.priority.cmp(&other.eval.priority) {
            Ordering::Equal => other.seq.cmp(&self.seq), // lower seq = older = higher
            other => other,                              // higher priority = higher
        }
    }
}

/// Inner state of the eval queue, behind a mutex.
#[derive(Debug)]
struct Inner {
    /// Max-heap of pending evaluations (ordered by priority desc, then FIFO).
    heap: BinaryHeap<PendingEval>,
    /// Monotonically increasing insertion counter.
    next_seq: u64,
    /// Evals handed out by `dequeue` but not yet `ack`ed, keyed by eval id.
    /// A `nack` re-enqueues from here; an `ack` drops the entry.
    in_flight: HashMap<String, PendingEval>,
    /// Evals parked because nothing can place them yet (no capacity). Moved
    /// back to `heap` wholesale by `unblock_all` when the cluster changes.
    blocked: Vec<Evaluation>,
}

/// A thread-safe priority queue for pending evaluations.
///
/// Queue order is descending by priority.  Equal-priority evals are returned
/// FIFO (first enqueued, first out).
///
/// **Mutex-poison policy:** mutating methods (`enqueue`/`dequeue`/`ack`/`nack`/
/// `block`/`unblock_all`) surface a poisoned mutex as `Err` so callers can
/// react. The `usize` introspection helpers (`len`/`in_flight_len`/
/// `blocked_len`) cannot return `Result` without changing their contract, so
/// they report `0` on poison; the poison still surfaces on the next mutating
/// call.
#[derive(Debug, Clone)]
pub struct EvalQueue {
    /// Shared mutable state behind a mutex.
    inner: Arc<std::sync::Mutex<Inner>>,
}

impl EvalQueue {
    /// Create a new empty eval queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(Inner {
                heap: BinaryHeap::new(),
                next_seq: 0,
                in_flight: HashMap::new(),
                blocked: Vec::new(),
            })),
        }
    }

    /// Lock the inner state, mapping a poisoned mutex to a runtime error.
    fn lock(&self) -> Result<MutexGuard<'_, Inner>> {
        self.inner.lock().map_err(|_| crate::error::Error::Runtime("eval queue mutex poisoned".to_owned()))
    }

    /// Push an evaluation onto the queue.
    ///
    /// The caller should have validated the eval and set its status to
    /// `EvalStatus::Pending` before enqueuing.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal mutex is poisoned.
    pub fn enqueue(&self, eval: Evaluation) -> Result<()> {
        let mut inner = self.lock()?;
        let seq = inner.next_seq;
        inner.next_seq += 1;
        inner.heap.push(PendingEval { seq, eval, dequeues: 0 });
        Ok(())
    }

    /// Dequeue the highest-priority pending evaluation, if any, and move it into
    /// the in-flight set. The caller must later [`EvalQueue::ack`] it on success
    /// or [`EvalQueue::nack`] it on failure (or the entry lingers until then).
    ///
    /// Returns `None` when the queue is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal mutex is poisoned.
    pub fn dequeue(&self) -> Result<Option<Evaluation>> {
        let mut inner = self.lock()?;
        let Some(mut pe) = inner.heap.pop() else { return Ok(None) };
        pe.dequeues += 1;
        let eval = pe.eval.clone();
        inner.in_flight.insert(eval.id.clone(), pe);
        Ok(Some(eval))
    }

    /// Acknowledge an in-flight eval as processed, removing it for good.
    /// Unknown ids are a no-op (the eval was already acked or never in flight).
    ///
    /// # Errors
    ///
    /// Returns an error if the internal mutex is poisoned.
    pub fn ack(&self, eval_id: &str) -> Result<()> {
        self.lock()?.in_flight.remove(eval_id);
        Ok(())
    }

    /// Negatively acknowledge an in-flight eval: re-enqueue it for another
    /// attempt, unless it has already been delivered `MAX_DEQUEUE` times, in
    /// which case it is dropped (treated as failed). Unknown ids are a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal mutex is poisoned.
    pub fn nack(&self, eval_id: &str) -> Result<()> {
        let mut inner = self.lock()?;
        if let Some(pe) = inner.in_flight.remove(eval_id) {
            if pe.dequeues < MAX_DEQUEUE {
                let seq = inner.next_seq;
                inner.next_seq += 1;
                // Fresh seq: re-delivered after the current backlog, not ahead of it.
                inner.heap.push(PendingEval { seq, eval: pe.eval, dequeues: pe.dequeues });
            }
        }
        Ok(())
    }

    /// The number of evals dequeued but not yet acked.
    #[must_use]
    pub fn in_flight_len(&self) -> usize {
        // 0 on poison; see the struct-level mutex-poison policy.
        self.lock().map_or(0, |g| g.in_flight.len())
    }

    /// Park an eval as blocked: it cannot be placed until the cluster changes
    /// (e.g. capacity frees up). Held out of the pending heap until
    /// [`EvalQueue::unblock_all`].
    ///
    /// # Errors
    ///
    /// Returns an error if the internal mutex is poisoned.
    pub fn block(&self, eval: Evaluation) -> Result<()> {
        self.lock()?.blocked.push(eval);
        Ok(())
    }

    /// Re-enqueue every blocked eval onto the pending heap; returns how many
    /// were moved. Call when cluster state changes (node join/update) may have
    /// unblocked placement.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal mutex is poisoned.
    pub fn unblock_all(&self) -> Result<usize> {
        let mut inner = self.lock()?;
        let drained: Vec<Evaluation> = inner.blocked.drain(..).collect();
        let moved = drained.len();
        for eval in drained {
            let seq = inner.next_seq;
            inner.next_seq += 1;
            inner.heap.push(PendingEval { seq, eval, dequeues: 0 });
        }
        Ok(moved)
    }

    /// The number of blocked evals waiting for [`EvalQueue::unblock_all`].
    #[must_use]
    pub fn blocked_len(&self) -> usize {
        // 0 on poison; see the struct-level mutex-poison policy.
        self.lock().map_or(0, |g| g.blocked.len())
    }

    /// The number of evaluations currently waiting.
    #[must_use]
    pub fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(g) => g.heap.len(),
            Err(_) => {
                // Mutex poison: system is hosed, return 0 as a lie rather
                // than a poison panic. The poison will surface on the next
                // enqueue/dequeue call that propagates the error.
                0
            },
        }
    }

    /// Whether the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for EvalQueue {
    fn default() -> Self {
        Self::new()
    }
}

// ---- tests -----------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::missing_docs_in_private_items,
    clippy::wildcard_imports,
    clippy::unwrap_used,
    reason = "conventional inline test module"
)]
mod tests {
    use super::*;
    use crate::eval::EvalStatus;
    use crate::eval::EvalTrigger;

    fn pending_eval(id: &str, priority: i32) -> Evaluation {
        Evaluation {
            id: id.to_owned(),
            job_id: "redis".to_owned(),
            priority,
            trigger: EvalTrigger::JobRegister,
            status: EvalStatus::Pending,
        }
    }

    #[test]
    fn new_queue_is_empty() {
        let q = EvalQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn enqueue_increases_len() {
        let q = EvalQueue::new();
        q.enqueue(pending_eval("e1", 50)).unwrap();
        assert_eq!(q.len(), 1);
        assert!(!q.is_empty());
    }

    #[test]
    fn dequeue_returns_items_in_priority_order() {
        let q = EvalQueue::new();
        q.enqueue(pending_eval("low", 10)).unwrap();
        q.enqueue(pending_eval("high", 80)).unwrap();
        q.enqueue(pending_eval("mid", 50)).unwrap();

        assert_eq!(q.dequeue().unwrap().unwrap().id, "high");
        assert_eq!(q.dequeue().unwrap().unwrap().id, "mid");
        assert_eq!(q.dequeue().unwrap().unwrap().id, "low");
        assert!(q.dequeue().unwrap().is_none());
    }

    #[test]
    fn equal_priority_is_fifo() {
        let q = EvalQueue::new();
        q.enqueue(pending_eval("first", 50)).unwrap();
        q.enqueue(pending_eval("second", 50)).unwrap();
        q.enqueue(pending_eval("third", 50)).unwrap();

        assert_eq!(q.dequeue().unwrap().unwrap().id, "first");
        assert_eq!(q.dequeue().unwrap().unwrap().id, "second");
        assert_eq!(q.dequeue().unwrap().unwrap().id, "third");
    }

    #[test]
    fn dequeue_empty_returns_none() {
        let q: EvalQueue = EvalQueue::new();
        assert!(q.dequeue().unwrap().is_none());
    }

    #[test]
    fn clone_shares_same_queue() {
        let q1 = EvalQueue::new();
        q1.enqueue(pending_eval("e1", 50)).unwrap();

        let q2 = q1.clone();
        assert_eq!(q2.dequeue().unwrap().unwrap().id, "e1");
        assert!(q1.is_empty());
    }

    #[test]
    fn dequeue_moves_eval_in_flight() {
        let q = EvalQueue::new();
        q.enqueue(pending_eval("e1", 50)).unwrap();
        let e = q.dequeue().unwrap().unwrap();
        assert_eq!(e.id, "e1");
        assert_eq!(q.len(), 0, "off the pending heap");
        assert_eq!(q.in_flight_len(), 1, "now in flight, awaiting ack");
    }

    #[test]
    fn ack_clears_in_flight() {
        let q = EvalQueue::new();
        q.enqueue(pending_eval("e1", 50)).unwrap();
        q.dequeue().unwrap().unwrap();
        q.ack("e1").unwrap();
        assert_eq!(q.in_flight_len(), 0);
    }

    #[test]
    fn ack_unknown_id_is_noop() {
        let q = EvalQueue::new();
        assert!(q.ack("nope").is_ok());
        assert_eq!(q.in_flight_len(), 0);
    }

    #[test]
    fn nack_re_enqueues_for_redelivery() {
        let q = EvalQueue::new();
        q.enqueue(pending_eval("e1", 50)).unwrap();
        q.dequeue().unwrap().unwrap();
        q.nack("e1").unwrap();
        assert_eq!(q.in_flight_len(), 0, "left in-flight");
        assert_eq!(q.len(), 1, "back on the pending heap");
        assert_eq!(q.dequeue().unwrap().unwrap().id, "e1", "redelivered");
    }

    #[test]
    fn nack_drops_eval_after_max_deliveries() {
        let q = EvalQueue::new();
        q.enqueue(pending_eval("e1", 50)).unwrap();
        // Deliver-and-nack MAX_DEQUEUE times; the last nack must drop it.
        for _ in 0..MAX_DEQUEUE {
            assert_eq!(q.dequeue().unwrap().unwrap().id, "e1");
            q.nack("e1").unwrap();
        }
        assert_eq!(q.len(), 0, "exceeded delivery cap, not re-enqueued");
        assert_eq!(q.in_flight_len(), 0);
        assert!(q.dequeue().unwrap().is_none());
    }

    #[test]
    fn block_holds_out_of_pending_until_unblock() {
        let q = EvalQueue::new();
        q.block(pending_eval("b1", 50)).unwrap();
        assert_eq!(q.blocked_len(), 1);
        assert_eq!(q.len(), 0, "blocked evals are not pending");

        let moved = q.unblock_all().unwrap();
        assert_eq!(moved, 1);
        assert_eq!(q.blocked_len(), 0);
        assert_eq!(q.len(), 1, "now pending");
        assert_eq!(q.dequeue().unwrap().unwrap().id, "b1");
    }

    #[test]
    fn drain_all_respects_priority() {
        let q = EvalQueue::new();
        q.enqueue(pending_eval("a", 30)).unwrap();
        q.enqueue(pending_eval("b", 99)).unwrap();
        q.enqueue(pending_eval("c", 1)).unwrap();
        q.enqueue(pending_eval("d", 50)).unwrap();

        let mut ids = Vec::new();
        while let Some(eval) = q.dequeue().unwrap() {
            ids.push(eval.id);
        }
        assert_eq!(ids, &["b", "d", "a", "c"]);
    }
}
