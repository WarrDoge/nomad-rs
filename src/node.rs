// SPDX-License-Identifier: Apache-2.0

//! Cluster node (client) representation and scheduling eligibility.
//!
//! Mirrors the subset of upstream Nomad's `structs.Node` the scheduler needs:
//! identity, advertised resources, lifecycle status, and eligibility/drain
//! state. Behaviour is specified by the tests; the methods are unimplemented.

use std::collections::HashMap;

use crate::error::Result;
use crate::jobspec::Resources;

/// Lifecycle status of a node as tracked by the servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    /// Registering; not yet ready for work.
    Init,
    /// Healthy and ready to receive allocations.
    Ready,
    /// Missed heartbeats; considered down.
    Down,
    /// Partitioned from the servers; allocations may still run.
    Disconnected,
}

impl NodeStatus {
    /// Lowercase wire string for the status (matches upstream constants),
    /// e.g. [`NodeStatus::Ready`] renders as `"ready"`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        todo!("map Init/Ready/Down/Disconnected to initializing/ready/down/disconnected")
    }
}

/// Whether the scheduler may place new work on a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingEligibility {
    /// New allocations may be placed.
    Eligible,
    /// Operator marked the node ineligible; running allocs are untouched.
    Ineligible,
}

/// A client node advertised to the cluster.
#[derive(Debug, Clone)]
pub struct Node {
    /// Unique node identifier (UUID).
    pub id: String,
    /// Human-readable node name.
    pub name: String,
    /// Datacenter the node belongs to.
    pub datacenter: String,
    /// Operator-assigned class used for constraint matching.
    pub node_class: String,
    /// Total resources advertised for scheduling.
    pub resources: Resources,
    /// Current lifecycle status.
    pub status: NodeStatus,
    /// Scheduling eligibility.
    pub eligibility: SchedulingEligibility,
    /// Whether the node is draining its allocations.
    pub draining: bool,
    /// Fingerprinted attributes, e.g. `"os.name" => "linux"`.
    pub attributes: HashMap<String, String>,
    /// Driver name to healthy flag, e.g. `"docker" => true`.
    pub drivers: HashMap<String, bool>,
}

impl Node {
    /// Validate the node registration.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `id`, `name`, or `datacenter`
    /// is empty, or if advertised [`Resources`] are invalid.
    pub fn validate(&self) -> Result<()> {
        todo!("require non-empty id/name/datacenter and valid advertised resources")
    }

    /// Whether the node can receive new allocations: status
    /// [`NodeStatus::Ready`], eligibility [`SchedulingEligibility::Eligible`],
    /// and not draining.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        todo!("true iff Ready && Eligible && !draining")
    }

    /// Whether `driver` is present and healthy on this node.
    #[must_use]
    pub fn has_healthy_driver(&self, driver: &str) -> bool {
        todo!("look up {driver:?} in the drivers map and return its health flag")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn ready_node() -> Node {
        Node {
            id: "11111111-1111-1111-1111-111111111111".to_owned(),
            name: "node1".to_owned(),
            datacenter: "dc1".to_owned(),
            node_class: String::new(),
            resources: Resources::default(),
            status: NodeStatus::Ready,
            eligibility: SchedulingEligibility::Eligible,
            draining: false,
            attributes: HashMap::new(),
            drivers: HashMap::from([("docker".to_owned(), true)]),
        }
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn status_strings_match_upstream() {
        assert_eq!(NodeStatus::Init.as_str(), "initializing");
        assert_eq!(NodeStatus::Ready.as_str(), "ready");
        assert_eq!(NodeStatus::Down.as_str(), "down");
        assert_eq!(NodeStatus::Disconnected.as_str(), "disconnected");
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn valid_node_passes() {
        assert!(ready_node().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn node_rejects_empty_id() {
        let mut n = ready_node();
        n.id = String::new();
        assert!(n.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn node_rejects_empty_datacenter() {
        let mut n = ready_node();
        n.datacenter = String::new();
        assert!(n.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn ready_node_is_ready() {
        assert!(ready_node().is_ready());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn draining_node_not_ready() {
        let mut n = ready_node();
        n.draining = true;
        assert!(!n.is_ready());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn ineligible_node_not_ready() {
        let mut n = ready_node();
        n.eligibility = SchedulingEligibility::Ineligible;
        assert!(!n.is_ready());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn down_node_not_ready() {
        let mut n = ready_node();
        n.status = NodeStatus::Down;
        assert!(!n.is_ready());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn detects_healthy_driver() {
        assert!(ready_node().has_healthy_driver("docker"));
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn missing_driver_is_not_healthy() {
        assert!(!ready_node().has_healthy_driver("qemu"));
    }
}
