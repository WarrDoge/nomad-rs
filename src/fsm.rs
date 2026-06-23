// SPDX-License-Identifier: Apache-2.0

//! Replicated finite state machine.
//!
//! The servers agree on an ordered log of [`Command`](crate::fsm::Command)s via
//! Raft; each committed command is applied here, in order, to the authoritative
//! [`StateStore`](crate::state::StateStore). This
//! module is the consensus-agnostic core: the transport and election live
//! behind it (deferred). Behaviour is specified by the tests and unimplemented.

use crate::alloc::Allocation;
use crate::error::Result;
use crate::eval::Evaluation;
use crate::jobspec::Job;
use crate::node::Node;
use crate::state::StateStore;

/// A committed state-change command — the payload of a Raft log entry.
#[derive(Debug, Clone)]
pub enum Command {
    /// Register or update a job.
    UpsertJob(Job),
    /// Deregister the job with the given name.
    DeregisterJob(String),
    /// Register or update a node.
    UpsertNode(Node),
    /// Insert or update an allocation.
    UpsertAlloc(Allocation),
    /// Insert or update an evaluation.
    UpsertEval(Evaluation),
}

/// Owns the authoritative [`StateStore`] and mutates it by applying committed
/// [`Command`]s in log order.
#[derive(Debug, Default)]
pub struct Fsm {
    /// The state this FSM is responsible for.
    state: StateStore,
}

impl Fsm {
    /// Create an FSM over an empty state store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Borrow the underlying state for reads.
    #[must_use]
    pub fn state(&self) -> &StateStore {
        &self.state
    }

    /// Apply a committed command to the state.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying [`StateStore`] operation rejects the
    /// command (e.g. validation failure, deleting a missing job).
    pub fn apply(&mut self, command: Command) -> Result<()> {
        match command {
            Command::UpsertJob(job) => self.state.upsert_job(job),
            Command::DeregisterJob(name) => self.state.delete_job(&name),
            Command::UpsertNode(node) => self.state.upsert_node(node),
            Command::UpsertAlloc(alloc) => self.state.upsert_alloc(alloc),
            Command::UpsertEval(eval) => self.state.upsert_eval(eval),
        }
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn job(name: &str) -> Job {
        Job { name: name.to_owned(), ..Job::default() }
    }

    #[test]
    fn new_fsm_state_is_empty() {
        assert!(Fsm::new().state().list_jobs().is_empty());
    }

    #[test]
    fn apply_upsert_job_makes_it_readable() {
        let mut fsm = Fsm::new();
        fsm.apply(Command::UpsertJob(job("redis"))).unwrap();
        assert!(fsm.state().get_job("redis").is_some());
    }

    #[test]
    fn apply_deregister_removes_job() {
        let mut fsm = Fsm::new();
        fsm.apply(Command::UpsertJob(job("redis"))).unwrap();
        fsm.apply(Command::DeregisterJob("redis".to_owned())).unwrap();
        assert!(fsm.state().get_job("redis").is_none());
    }
}
