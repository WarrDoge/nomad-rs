// SPDX-License-Identifier: Apache-2.0

//! Allocations: a task group placed on a specific node by the scheduler.
//!
//! Mirrors the subset of upstream Nomad's `structs.Allocation` needed to track
//! placement and lifecycle. Behaviour is specified by the tests; the methods
//! are unimplemented.

use crate::error::Result;
use crate::jobspec::Resources;

/// Operator/scheduler intent for an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesiredStatus {
    /// The allocation should be running.
    Run,
    /// The allocation should be stopped.
    Stop,
    /// The allocation should be evicted (preempted) from its node.
    Evict,
}

impl DesiredStatus {
    /// Lowercase wire string, e.g. [`DesiredStatus::Run`] is `"run"`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Stop => "stop",
            Self::Evict => "evict",
        }
    }
}

/// Last reported client-side status of an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStatus {
    /// Accepted but not yet started.
    Pending,
    /// Running on the node.
    Running,
    /// Finished successfully.
    Complete,
    /// Finished with a failure.
    Failed,
    /// The node was lost while the allocation was running.
    Lost,
}

impl ClientStatus {
    /// Lowercase wire string, e.g. [`ClientStatus::Running`] is `"running"`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Lost => "lost",
        }
    }

    /// Whether this is a terminal status: [`ClientStatus::Complete`],
    /// [`ClientStatus::Failed`], or [`ClientStatus::Lost`].
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Lost)
    }
}

/// A task group instance placed on a node.
#[derive(Debug, Clone)]
pub struct Allocation {
    /// Unique allocation identifier (UUID).
    pub id: String,
    /// Evaluation that produced this allocation.
    pub eval_id: String,
    /// Node the allocation is placed on.
    pub node_id: String,
    /// Job the allocation belongs to.
    pub job_id: String,
    /// Task group name within the job.
    pub task_group: String,
    /// Desired status (scheduler/operator intent).
    pub desired_status: DesiredStatus,
    /// Last reported client status.
    pub client_status: ClientStatus,
    /// Resources reserved on the node for this allocation.
    pub resources: Resources,
}

impl Allocation {
    /// Validate the allocation's required linkage and resources.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `id`, `node_id`, `job_id`, or
    /// `task_group` is empty, or if [`Resources`] are invalid.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() {
            return Err(crate::error::Error::Validation("alloc id cannot be empty".to_owned()));
        }
        if self.node_id.is_empty() {
            return Err(crate::error::Error::Validation("alloc node_id cannot be empty".to_owned()));
        }
        if self.job_id.is_empty() {
            return Err(crate::error::Error::Validation("alloc job_id cannot be empty".to_owned()));
        }
        if self.task_group.is_empty() {
            return Err(crate::error::Error::Validation("alloc task_group cannot be empty".to_owned()));
        }
        Ok(())
    }

    /// Whether the allocation has reached a terminal client status.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.client_status.is_terminal()
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn running_alloc() -> Allocation {
        Allocation {
            id: "22222222-2222-2222-2222-222222222222".to_owned(),
            eval_id: "eval-1".to_owned(),
            node_id: "node-1".to_owned(),
            job_id: "redis".to_owned(),
            task_group: "cache".to_owned(),
            desired_status: DesiredStatus::Run,
            client_status: ClientStatus::Running,
            resources: Resources::default(),
        }
    }

    #[test]
    fn desired_status_strings() {
        assert_eq!(DesiredStatus::Run.as_str(), "run");
        assert_eq!(DesiredStatus::Stop.as_str(), "stop");
        assert_eq!(DesiredStatus::Evict.as_str(), "evict");
    }

    #[test]
    fn client_status_strings() {
        assert_eq!(ClientStatus::Pending.as_str(), "pending");
        assert_eq!(ClientStatus::Running.as_str(), "running");
        assert_eq!(ClientStatus::Complete.as_str(), "complete");
        assert_eq!(ClientStatus::Failed.as_str(), "failed");
        assert_eq!(ClientStatus::Lost.as_str(), "lost");
    }

    #[test]
    fn terminal_statuses() {
        assert!(ClientStatus::Complete.is_terminal());
        assert!(ClientStatus::Failed.is_terminal());
        assert!(ClientStatus::Lost.is_terminal());
    }

    #[test]
    fn non_terminal_statuses() {
        assert!(!ClientStatus::Pending.is_terminal());
        assert!(!ClientStatus::Running.is_terminal());
    }

    #[test]
    fn valid_alloc_passes() {
        assert!(running_alloc().validate().is_ok());
    }

    #[test]
    fn alloc_rejects_empty_node() {
        let mut a = running_alloc();
        a.node_id = String::new();
        assert!(a.validate().is_err());
    }

    #[test]
    fn alloc_rejects_empty_job() {
        let mut a = running_alloc();
        a.job_id = String::new();
        assert!(a.validate().is_err());
    }

    #[test]
    fn running_alloc_is_not_terminal() {
        assert!(!running_alloc().is_terminal());
    }

    #[test]
    fn completed_alloc_is_terminal() {
        let mut a = running_alloc();
        a.client_status = ClientStatus::Complete;
        assert!(a.is_terminal());
    }
}
