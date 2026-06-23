// SPDX-License-Identifier: Apache-2.0

//! Deployments: orchestrated rollouts with canaries and health gating.
//!
//! Tracks a job version's rollout against its [`crate::update::UpdateStrategy`].
//! Mirrors the subset of upstream Nomad's `structs.Deployment`. Behaviour is
//! specified by the tests and is unimplemented.

use crate::error::Result;

/// Lifecycle status of a deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentStatus {
    /// Actively rolling out.
    Running,
    /// Completed successfully.
    Successful,
    /// Failed (may auto-revert).
    Failed,
    /// Cancelled by a newer deployment.
    Cancelled,
    /// Paused awaiting manual promotion.
    Paused,
}

/// A job rollout in progress.
#[derive(Debug, Clone)]
pub struct Deployment {
    /// Unique deployment id.
    pub id: String,
    /// Job being rolled out.
    pub job_id: String,
    /// Current status.
    pub status: DeploymentStatus,
    /// Canaries the strategy asked for.
    pub desired_canaries: u32,
    /// Allocations placed so far.
    pub placed: u32,
    /// Allocations currently healthy.
    pub healthy: u32,
}

impl Deployment {
    /// Validate the deployment counters.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `id`/`job_id` are empty or
    /// `healthy > placed`.
    pub fn validate(&self) -> Result<()> {
        todo!("require ids and healthy <= placed")
    }

    /// Whether canaries are all healthy and the deployment can be promoted.
    #[must_use]
    pub fn is_promotable(&self) -> bool {
        todo!("true when healthy >= desired_canaries and canaries were requested")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn deployment() -> Deployment {
        Deployment {
            id: "d1".to_owned(),
            job_id: "redis".to_owned(),
            status: DeploymentStatus::Running,
            desired_canaries: 2,
            placed: 2,
            healthy: 2,
        }
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn valid_deployment_passes() {
        assert!(deployment().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_healthy_above_placed() {
        let mut d = deployment();
        d.healthy = 5;
        assert!(d.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn promotable_when_canaries_healthy() {
        assert!(deployment().is_promotable());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn not_promotable_when_canaries_unhealthy() {
        let mut d = deployment();
        d.healthy = 1;
        assert!(!d.is_promotable());
    }
}
