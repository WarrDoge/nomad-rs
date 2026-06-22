// SPDX-License-Identifier: Apache-2.0

//! Node drain orchestration.
//!
//! Draining migrates allocations off a node within a deadline. Mirrors the
//! subset of upstream Nomad's drain spec plus a progress helper. Behaviour is
//! specified by the tests and is unimplemented.

use crate::error::Result;

/// How a node should be drained.
#[derive(Debug, Clone)]
pub struct DrainSpec {
    /// Time allowed to migrate allocations, in seconds (0 means none unless
    /// `force`).
    pub deadline_secs: u64,
    /// Drain immediately, ignoring the deadline.
    pub force: bool,
    /// Leave system jobs running.
    pub ignore_system_jobs: bool,
}

impl DrainSpec {
    /// Validate the drain spec.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `deadline_secs` is zero and
    /// `force` is false (a non-forced drain needs a deadline).
    pub fn validate(&self) -> Result<()> {
        todo!("require a positive deadline unless force is set")
    }
}

/// Snapshot of how far a drain has progressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainProgress {
    /// Allocations still to migrate.
    pub remaining: u32,
    /// Whether the drain is complete.
    pub complete: bool,
}

/// Compute drain progress from totals.
#[must_use]
pub fn drain_progress(total: u32, migrated: u32) -> DrainProgress {
    todo!("remaining = total-migrated ({total}-{migrated}), complete when migrated>=total")
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn deadline_drain_passes() {
        let d = DrainSpec { deadline_secs: 60, force: false, ignore_system_jobs: true };
        assert!(d.validate().is_ok());
    }

    #[test]
    fn zero_deadline_without_force_errors() {
        let d = DrainSpec { deadline_secs: 0, force: false, ignore_system_jobs: true };
        assert!(d.validate().is_err());
    }

    #[test]
    fn forced_drain_needs_no_deadline() {
        let d = DrainSpec { deadline_secs: 0, force: true, ignore_system_jobs: true };
        assert!(d.validate().is_ok());
    }

    #[test]
    fn progress_complete_when_all_migrated() {
        assert_eq!(drain_progress(5, 5), DrainProgress { remaining: 0, complete: true });
    }

    #[test]
    fn progress_counts_remaining() {
        assert_eq!(drain_progress(5, 2), DrainProgress { remaining: 3, complete: false });
    }
}
