// SPDX-License-Identifier: Apache-2.0

//! Evaluations: the scheduler's unit of work.
//!
//! An evaluation is created whenever the desired state may have diverged from
//! the actual state (a job is registered, a node fails, etc.). The scheduler
//! processes evaluations to produce allocation plans. Mirrors the subset of
//! upstream Nomad's `structs.Evaluation` the scheduler needs. Behaviour is
//! specified by the tests; the methods are unimplemented.

use crate::error::Result;
use crate::id::{EvalId, JobId};

/// Why an evaluation was created (upstream `TriggeredBy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EvalTrigger {
    /// A job was registered or updated.
    JobRegister,
    /// A job was deregistered.
    JobDeregister,
    /// A node's status or resources changed.
    NodeUpdate,
    /// A node began draining.
    NodeDrain,
    /// An allocation failed and may need rescheduling.
    AllocFailure,
    /// The scheduler hit the max plan submission attempts.
    MaxPlanAttempts,
}

impl EvalTrigger {
    /// Lowercase, hyphenated wire string, e.g. [`EvalTrigger::JobRegister`] is
    /// `"job-register"`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::JobRegister => "job-register",
            Self::JobDeregister => "job-deregister",
            Self::NodeUpdate => "node-update",
            Self::NodeDrain => "node-drain",
            Self::AllocFailure => "alloc-failure",
            Self::MaxPlanAttempts => "max-plan-attempts",
        }
    }
}

/// Lifecycle status of an evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EvalStatus {
    /// Waiting on resources/another eval before it can run.
    Blocked,
    /// Queued for processing.
    Pending,
    /// Processed successfully.
    Complete,
    /// Processing failed.
    Failed,
    /// Superseded or cancelled before processing.
    Canceled,
}

impl EvalStatus {
    /// Lowercase wire string, e.g. `EvalStatus::Pending` is `"pending"`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Blocked => "blocked",
            Self::Pending => "pending",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    /// Whether this is a terminal status: [`EvalStatus::Complete`],
    /// [`EvalStatus::Failed`], or [`EvalStatus::Canceled`].
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Canceled)
    }
}

/// A scheduler evaluation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Evaluation {
    /// Unique evaluation identifier (UUID).
    pub id: EvalId,
    /// Job this evaluation concerns.
    pub job_id: JobId,
    /// Scheduling priority, inherited from the job (1..=100).
    pub priority: i32,
    /// Why the evaluation was created.
    pub trigger: EvalTrigger,
    /// Current lifecycle status.
    pub status: EvalStatus,
}

impl Evaluation {
    /// Validate the evaluation.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Validation`] if `id` or `job_id` is empty,
    /// or if `priority` is outside the job priority range.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() {
            return Err(crate::error::Error::Validation("eval id cannot be empty".to_owned()));
        }
        if self.job_id.is_empty() {
            return Err(crate::error::Error::Validation("eval job_id cannot be empty".to_owned()));
        }
        if !(crate::jobspec::JOB_MIN_PRIORITY..=crate::jobspec::JOB_MAX_PRIORITY).contains(&self.priority) {
            return Err(crate::error::Error::Validation(format!(
                "priority {} out of range ({}-{})",
                self.priority,
                crate::jobspec::JOB_MIN_PRIORITY,
                crate::jobspec::JOB_MAX_PRIORITY,
            )));
        }
        Ok(())
    }

    /// Whether the evaluation is ready to be dequeued and processed: status is
    /// `EvalStatus::Pending`.
    #[must_use]
    pub fn is_schedulable(&self) -> bool {
        self.status == EvalStatus::Pending
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::jobspec::{JOB_DEFAULT_PRIORITY, JOB_MAX_PRIORITY};

    fn pending_eval() -> Evaluation {
        Evaluation {
            id: "33333333-3333-3333-3333-333333333333".into(),
            job_id: "redis".into(),
            priority: JOB_DEFAULT_PRIORITY,
            trigger: EvalTrigger::JobRegister,
            status: EvalStatus::Pending,
        }
    }

    #[test]
    fn trigger_strings() {
        assert_eq!(EvalTrigger::JobRegister.as_str(), "job-register");
        assert_eq!(EvalTrigger::JobDeregister.as_str(), "job-deregister");
        assert_eq!(EvalTrigger::NodeUpdate.as_str(), "node-update");
        assert_eq!(EvalTrigger::NodeDrain.as_str(), "node-drain");
        assert_eq!(EvalTrigger::AllocFailure.as_str(), "alloc-failure");
        assert_eq!(EvalTrigger::MaxPlanAttempts.as_str(), "max-plan-attempts");
    }

    #[test]
    fn status_strings() {
        assert_eq!(EvalStatus::Blocked.as_str(), "blocked");
        assert_eq!(EvalStatus::Pending.as_str(), "pending");
        assert_eq!(EvalStatus::Complete.as_str(), "complete");
        assert_eq!(EvalStatus::Failed.as_str(), "failed");
        assert_eq!(EvalStatus::Canceled.as_str(), "canceled");
    }

    #[test]
    fn terminal_statuses() {
        assert!(EvalStatus::Complete.is_terminal());
        assert!(EvalStatus::Failed.is_terminal());
        assert!(EvalStatus::Canceled.is_terminal());
    }

    #[test]
    fn non_terminal_statuses() {
        assert!(!EvalStatus::Blocked.is_terminal());
        assert!(!EvalStatus::Pending.is_terminal());
    }

    #[test]
    fn valid_eval_passes() {
        assert!(pending_eval().validate().is_ok());
    }

    #[test]
    fn eval_rejects_empty_job_id() {
        let mut e = pending_eval();
        e.job_id = JobId::default();
        assert!(e.validate().is_err());
    }

    #[test]
    fn eval_rejects_out_of_range_priority() {
        let mut e = pending_eval();
        e.priority = JOB_MAX_PRIORITY + 1;
        assert!(e.validate().is_err());
    }

    #[test]
    fn pending_eval_is_schedulable() {
        assert!(pending_eval().is_schedulable());
    }

    #[test]
    fn blocked_eval_is_not_schedulable() {
        let mut e = pending_eval();
        e.status = EvalStatus::Blocked;
        assert!(!e.is_schedulable());
    }
}
