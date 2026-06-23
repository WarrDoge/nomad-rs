// SPDX-License-Identifier: Apache-2.0

//! Rolling update / deployment strategy for a task group.
//!
//! Controls how new versions roll out: parallelism, canaries, health gating,
//! and automatic revert/promote. Mirrors the subset of upstream Nomad's
//! `structs.UpdateStrategy`. Behaviour is specified by the tests and is
//! unimplemented.

use crate::error::Result;

/// How allocation health is determined during a deployment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthCheck {
    /// Use registered service checks.
    Checks,
    /// Use task running/healthy states only.
    TaskStates,
    /// Health is set manually by an operator.
    Manual,
}

/// The rolling-update strategy.
#[derive(Debug, Clone)]
pub struct UpdateStrategy {
    /// Max allocations to update simultaneously (>= 1).
    pub max_parallel: u32,
    /// Number of canary allocations to deploy before the rest (0 = none).
    pub canary: u32,
    /// How health is judged.
    pub health_check: HealthCheck,
    /// Minimum time an alloc must stay healthy to count, in seconds.
    pub min_healthy_secs: u64,
    /// Deadline for an alloc to become healthy, in seconds (> `min_healthy_secs`).
    pub healthy_deadline_secs: u64,
    /// Roll back automatically if the deployment fails.
    pub auto_revert: bool,
    /// Promote canaries automatically once healthy.
    pub auto_promote: bool,
}

impl UpdateStrategy {
    /// Validate the update strategy.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `max_parallel` is zero or
    /// `healthy_deadline_secs <= min_healthy_secs`.
    pub fn validate(&self) -> Result<()> {
        if self.max_parallel == 0 {
            return Err(crate::error::Error::Config("update max_parallel must be >= 1".to_owned()));
        }
        if self.healthy_deadline_secs <= self.min_healthy_secs {
            return Err(crate::error::Error::Config(format!(
                "update healthy_deadline_secs ({}) must exceed min_healthy_secs ({})",
                self.healthy_deadline_secs, self.min_healthy_secs
            )));
        }
        Ok(())
    }

    /// Whether this strategy deploys canaries first.
    #[must_use]
    pub fn uses_canary(&self) -> bool {
        self.canary > 0
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn strategy() -> UpdateStrategy {
        UpdateStrategy {
            max_parallel: 1,
            canary: 0,
            health_check: HealthCheck::Checks,
            min_healthy_secs: 10,
            healthy_deadline_secs: 300,
            auto_revert: true,
            auto_promote: false,
        }
    }

    #[test]
    fn valid_strategy_passes() {
        assert!(strategy().validate().is_ok());
    }

    #[test]
    fn rejects_zero_max_parallel() {
        let mut s = strategy();
        s.max_parallel = 0;
        assert!(s.validate().is_err());
    }

    #[test]
    fn rejects_deadline_not_after_min_healthy() {
        let mut s = strategy();
        s.min_healthy_secs = 300;
        s.healthy_deadline_secs = 300;
        assert!(s.validate().is_err());
    }

    #[test]
    fn detects_canary() {
        let mut s = strategy();
        s.canary = 2;
        assert!(s.uses_canary());
    }

    #[test]
    fn no_canary_by_default() {
        assert!(!strategy().uses_canary());
    }
}
