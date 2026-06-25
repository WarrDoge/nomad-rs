// SPDX-License-Identifier: Apache-2.0

//! Priority eval queue — the central point where evaluations wait to be
//! picked up by a scheduler worker.  Evals enter via `enqueue` (called by
//! `RpcEndpoint` on job register/deregister) and leave via `dequeue` (polled
//! by the scheduler loop).  Queue order is descending by priority, then FIFO
//! within the same priority level.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::error::Result;
use crate::eval::Evaluation;

/// A pending evaluation wrapper that orders by priority (high first) then by
/// insertion order (FIFO for equal priorities).
#[derive(Debug, Clone)]
struct PendingEval {
    /// Sequence number — monotonically increasing insertion counter used as a
    /// tie-breaker for equal-priority items so a `BinaryHeap` stays stable-FIFO.
    seq: u64,
    /// The evaluation.
    eval: Evaluation,
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
}

/// A thread-safe priority queue for pending evaluations.
///
/// Queue order is descending by priority.  Equal-priority evals are returned
/// FIFO (first enqueued, first out).
#[derive(Debug, Clone)]
pub struct EvalQueue {
    /// Shared mutable state behind a mutex.
    inner: Arc<std::sync::Mutex<Inner>>,
}

impl EvalQueue {
    /// Create a new empty eval queue.
    #[must_use]
    pub fn new() -> Self {
        Self { inner: Arc::new(std::sync::Mutex::new(Inner { heap: BinaryHeap::new(), next_seq: 0 })) }
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
        match self.inner.lock() {
            Ok(mut inner) => {
                let seq = inner.next_seq;
                inner.next_seq += 1;
                inner.heap.push(PendingEval { seq, eval });
                Ok(())
            },
            Err(_) => Err(crate::error::Error::Runtime("eval queue mutex poisoned".to_owned())),
        }
    }

    /// Dequeue the highest-priority pending evaluation, if any.
    ///
    /// Returns `None` when the queue is empty.
    ///
    /// # Errors
    ///
    /// Returns an error if the internal mutex is poisoned.
    pub fn dequeue(&self) -> Result<Option<Evaluation>> {
        match self.inner.lock() {
            Ok(mut inner) => Ok(inner.heap.pop().map(|pe| pe.eval)),
            Err(_) => Err(crate::error::Error::Runtime("eval queue mutex poisoned".to_owned())),
        }
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
