// SPDX-License-Identifier: Apache-2.0

//! Consensus (Raft) — dependency-agnostic.
//!
//! [`RaftNode`](crate::raft::RaftNode) replicates committed [`Command`](crate::fsm::Command)s
//! and tracks the current role/leader. The concrete transport and election (a
//! Raft crate or hand-rolled) replace its bodies later. Behaviour is specified
//! by the tests and is unimplemented.

use crate::error::Result;
use crate::fsm::{Command, Fsm};
use crate::state::StateStore;

/// A node's role in the consensus group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    /// Replicates the log and serves writes.
    Leader,
    /// Follows the leader.
    Follower,
    /// Standing for election.
    Candidate,
}

/// The in-tree consensus node.
///
/// ponytail: single-node consensus only — a bootstrap node is the leader with a
/// quorum of one, so a proposal commits the instant it is appended and is
/// applied straight to the local FSM. The in-memory log is not yet persisted
/// (see `raft_log` for the on-disk store) and there is no replication or
/// election. Multi-node replication swaps in behind `propose`/`role` when the
/// RPC + membership layers land.
#[derive(Debug)]
pub struct RaftNode {
    /// This node's identifier within the consensus group.
    #[allow(dead_code, reason = "used once replication/membership lands")]
    id: String,
    /// This node's role. A bootstrap node leads; a joining node follows.
    role: RaftRole,
    /// The replicated command log (in-memory, single-node).
    log: Vec<Command>,
    /// The state machine committed entries are applied to.
    fsm: Fsm,
}

impl RaftNode {
    /// Create a follower node that will join an existing cluster.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self { id: id.to_owned(), role: RaftRole::Follower, log: Vec::new(), fsm: Fsm::new() }
    }

    /// Create a single-node bootstrap leader.
    #[must_use]
    pub fn bootstrap(id: &str) -> Self {
        Self { role: RaftRole::Leader, ..Self::new(id) }
    }
}

impl RaftNode {
    /// Propose a command for replication. Valid only on the leader.
    ///
    /// # Errors
    ///
    /// Returns an error if this node is not the leader or the entry fails to
    /// commit (e.g. the FSM rejects it).
    pub fn propose(&mut self, command: Command) -> Result<()> {
        if !self.is_leader() {
            return Err(crate::error::Error::Runtime("not the leader, cannot propose".to_owned()));
        }
        // Quorum of one: append == commit. Apply immediately to the FSM.
        self.fsm.apply(command.clone())?;
        self.log.push(command);
        Ok(())
    }

    /// Borrow the committed state for reads.
    #[must_use]
    pub fn state(&self) -> &StateStore {
        self.fsm.state()
    }

    /// Number of committed log entries.
    #[must_use]
    pub fn committed_index(&self) -> usize {
        self.log.len()
    }

    /// This node's current [`RaftRole`].
    #[must_use]
    pub fn role(&self) -> RaftRole {
        self.role
    }

    /// Whether this node is currently the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role() == RaftRole::Leader
    }

    /// Address of the current leader, if one is known.
    #[must_use]
    pub fn leader_addr(&self) -> Option<String> {
        None
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::jobspec::Job;

    #[test]
    fn fresh_node_is_not_leader() {
        assert!(!RaftNode::new("n1").is_leader());
    }

    #[test]
    fn fresh_node_has_no_leader_addr() {
        assert!(RaftNode::new("n1").leader_addr().is_none());
    }

    #[test]
    fn propose_on_follower_errors() {
        let mut node = RaftNode::new("n1");
        let cmd = Command::UpsertJob(Job { name: "redis".to_owned(), ..Job::default() });
        assert!(node.propose(cmd).is_err());
    }

    #[test]
    fn bootstrap_node_is_leader() {
        assert!(RaftNode::bootstrap("n1").is_leader());
    }

    #[test]
    fn propose_on_leader_commits_and_applies_to_state() {
        let mut node = RaftNode::bootstrap("n1");
        node.propose(Command::UpsertJob(Job { name: "redis".to_owned(), ..Job::default() })).unwrap();
        assert!(node.state().get_job("redis").is_some());
        assert_eq!(node.committed_index(), 1);
    }

    #[test]
    fn each_proposal_advances_the_commit_index() {
        let mut node = RaftNode::bootstrap("n1");
        node.propose(Command::UpsertJob(Job { name: "a".to_owned(), ..Job::default() })).unwrap();
        node.propose(Command::UpsertJob(Job { name: "b".to_owned(), ..Job::default() })).unwrap();
        assert_eq!(node.committed_index(), 2);
        assert!(node.state().get_job("a").is_some());
        assert!(node.state().get_job("b").is_some());
    }
}
