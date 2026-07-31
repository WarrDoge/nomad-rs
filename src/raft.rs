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
    /// Address of the current leader, if known. Set by the cluster membership
    /// layer; a follower that knows the leader address includes it in
    /// `NotLeader` responses so the caller can auto-forward.
    leader_addr: Option<String>,
}

impl RaftNode {
    /// Create a follower node that will join an existing cluster.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self { id: id.to_owned(), role: RaftRole::Follower, log: Vec::new(), fsm: Fsm::new(), leader_addr: None }
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
        // Quorum of one: append == commit. Append to the log first so the FSM
        // can never get ahead of the log (a half-applied command would be
        // unreplayable). Roll the entry back if the FSM rejects it, so the log
        // never holds a command the FSM wouldn't accept.
        self.log.push(command.clone());
        if let Err(e) = self.fsm.apply(command) {
            self.log.pop();
            return Err(e);
        }
        Ok(())
    }

    /// Borrow the committed state for reads.
    #[must_use]
    pub const fn state(&self) -> &StateStore {
        self.fsm.state()
    }

    /// Number of committed log entries.
    #[must_use]
    pub const fn committed_index(&self) -> usize {
        self.log.len()
    }

    /// This node's current [`RaftRole`].
    #[must_use]
    pub const fn role(&self) -> RaftRole {
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
        self.leader_addr.clone()
    }

    /// Set the known leader address (used by the cluster membership layer).
    pub fn set_leader_addr(&mut self, addr: Option<String>) {
        self.leader_addr = addr;
    }

    /// Mutable reference to the state store (for test setup).
    #[must_use]
    pub fn state_mut(&mut self) -> &mut StateStore {
        self.fsm.state_mut()
    }

    /// Record a heartbeat from a node. In a full implementation this updates
    /// the node's last-heard timestamp; for the single-node in-memory raft
    /// this is a no-op that ensures the node is known to state.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not registered in state.
    pub fn heartbeat(&self, node_id: &crate::id::NodeId) -> Result<()> {
        if self.state().get_node(node_id.as_str()).is_none() {
            return Err(crate::error::Error::Runtime(format!("heartbeat from unknown node: {node_id}")));
        }
        Ok(())
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
    fn rejected_proposal_leaves_no_log_entry() {
        // A command the FSM rejects must not pollute the log: the log is the
        // source of truth and must never hold an entry the FSM wouldn't accept.
        let mut node = RaftNode::bootstrap("n1");
        assert!(node.propose(Command::DeregisterJob("ghost".to_owned())).is_err());
        assert_eq!(node.committed_index(), 0, "rejected command is not committed");
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
