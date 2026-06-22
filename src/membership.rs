// SPDX-License-Identifier: Apache-2.0

//! Cluster membership / gossip contract — dependency-agnostic.
//!
//! Tracks which servers are in the cluster and their liveness. The concrete
//! gossip implementation (a `memberlist`/serf-style crate) lives behind
//! [`Membership`]. [`GossipMembership`] is the in-tree implementation whose
//! behaviour is specified by the tests and is unimplemented.

use crate::error::Result;

/// Liveness of a cluster member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
    /// Reachable and healthy.
    Alive,
    /// Gracefully leaving.
    Leaving,
    /// Has left the cluster.
    Left,
    /// Unreachable / failed.
    Failed,
}

/// A member of the cluster.
#[derive(Debug, Clone)]
pub struct Member {
    /// Member name.
    pub name: String,
    /// Advertised gossip address (`host:port`).
    pub addr: String,
    /// Current liveness.
    pub status: MemberStatus,
}

/// Cluster membership operations.
pub trait Membership {
    /// Join the cluster by contacting one or more peer addresses; returns the
    /// number of peers successfully reached.
    ///
    /// # Errors
    ///
    /// Returns an error if none of the peers could be reached.
    fn join(&mut self, addrs: &[String]) -> Result<usize>;

    /// The currently known members.
    fn members(&self) -> Vec<Member>;

    /// Gracefully leave the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the leave broadcast fails.
    fn leave(&mut self) -> Result<()>;
}

/// The in-tree gossip-based membership.
#[derive(Debug)]
pub struct GossipMembership {
    /// This member's name.
    #[allow(dead_code, reason = "read once the gossip implementation lands")]
    name: String,
}

impl GossipMembership {
    /// Create a membership handle for a node named `name`.
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self { name: name.to_owned() }
    }
}

impl Membership for GossipMembership {
    fn join(&mut self, addrs: &[String]) -> Result<usize> {
        todo!("contact peers {addrs:?} and merge their member lists")
    }

    fn members(&self) -> Vec<Member> {
        todo!("return the current membership view")
    }

    fn leave(&mut self) -> Result<()> {
        todo!("broadcast intent to leave and drain")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn new_membership_lists_self_only() {
        // Before joining, a fresh node knows only itself.
        assert_eq!(GossipMembership::new("s1").members().len(), 1);
    }

    #[test]
    fn join_reports_peers_reached() {
        let mut m = GossipMembership::new("s1");
        let reached = m.join(&["10.0.0.2:4648".to_owned()]).unwrap();
        assert_eq!(reached, 1);
    }

    #[test]
    fn leave_succeeds() {
        let mut m = GossipMembership::new("s1");
        assert!(m.leave().is_ok());
    }
}
