// SPDX-License-Identifier: Apache-2.0

//! Consensus contract (Raft) — dependency-agnostic.
//!
//! Defines the replication interface the servers rely on: propose a committed
//! [`Command`], learn the current role/leader. The concrete transport and
//! election (a Raft crate or a hand-rolled implementation) live behind
//! [`Consensus`]. [`RaftNode`] is the in-tree implementation whose behaviour is
//! specified by the tests and is unimplemented.

use crate::error::Result;
use crate::fsm::Command;

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

/// The replication interface the servers depend on.
pub trait Consensus {
    /// Propose a command for replication. Valid only on the leader.
    ///
    /// # Errors
    ///
    /// Returns an error if this node is not the leader or the entry fails to
    /// commit.
    fn propose(&mut self, command: Command) -> Result<()>;

    /// This node's current [`RaftRole`].
    fn role(&self) -> RaftRole;

    /// Whether this node is currently the leader.
    fn is_leader(&self) -> bool;

    /// Address of the current leader, if one is known.
    fn leader_addr(&self) -> Option<String>;
}

/// The in-tree consensus node.
#[derive(Debug)]
pub struct RaftNode {
    /// This node's identifier within the consensus group.
    #[allow(dead_code, reason = "read once the consensus implementation lands")]
    id: String,
}

impl RaftNode {
    /// Create a consensus node with the given id.
    #[must_use]
    pub fn new(id: &str) -> Self {
        Self { id: id.to_owned() }
    }
}

impl Consensus for RaftNode {
    #[allow(clippy::needless_pass_by_value, reason = "command is appended to the log once implemented")]
    fn propose(&mut self, command: Command) -> Result<()> {
        let _ = command;
        todo!("append the command to the replicated log and wait for commit")
    }

    fn role(&self) -> RaftRole {
        todo!("report the node's current raft role")
    }

    fn is_leader(&self) -> bool {
        todo!("true when role is Leader")
    }

    fn leader_addr(&self) -> Option<String> {
        todo!("return the known leader address, if any")
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
}
