// SPDX-License-Identifier: Apache-2.0

//! Restart and reschedule policies.
//!
//! A restart policy governs in-place task restarts on the same node; a
//! reschedule policy governs moving a failed allocation to another node.
//! Mirrors the subset of upstream Nomad's `structs.RestartPolicy`/
//! `ReschedulePolicy`. Behaviour is specified by the tests and is unimplemented.

use crate::error::Result;

/// What happens when a task exhausts its restart attempts within an interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartMode {
    /// Wait out the interval, then resume restarting.
    Delay,
    /// Fail the allocation (let the scheduler reschedule it).
    Fail,
}

/// In-place restart policy for a task.
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    /// Allowed restarts within `interval_secs`.
    pub attempts: u32,
    /// Sliding window for counting attempts, in seconds.
    pub interval_secs: u64,
    /// Delay between restarts, in seconds.
    pub delay_secs: u64,
    /// Behaviour once attempts are exhausted.
    pub mode: RestartMode,
}

impl RestartPolicy {
    /// Validate the restart policy.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `attempts > 0` but
    /// `interval_secs` is zero, or `delay_secs` exceeds `interval_secs`.
    pub fn validate(&self) -> Result<()> {
        todo!("require a non-zero interval when attempts>0 and delay<=interval")
    }
}

/// How reschedule delay grows across successive failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayFunction {
    /// Fixed delay every time.
    Constant,
    /// Doubles each attempt up to a cap.
    Exponential,
    /// Fibonacci growth up to a cap.
    Fibonacci,
}

/// Policy for moving a failed allocation to a new node.
#[derive(Debug, Clone)]
pub struct ReschedulePolicy {
    /// Allowed reschedules within `interval_secs` (ignored if `unlimited`).
    pub attempts: u32,
    /// Sliding window for counting attempts, in seconds.
    pub interval_secs: u64,
    /// Base delay before the first reschedule, in seconds.
    pub delay_secs: u64,
    /// How the delay grows.
    pub delay_function: DelayFunction,
    /// Maximum delay cap, in seconds (required for growing functions).
    pub max_delay_secs: u64,
    /// Reschedule without an attempt limit.
    pub unlimited: bool,
}

impl ReschedulePolicy {
    /// Validate the reschedule policy.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if a growing `delay_function`
    /// has `max_delay_secs <= delay_secs`, or a bounded policy has zero
    /// `attempts`.
    pub fn validate(&self) -> Result<()> {
        todo!("require max_delay>delay for growing functions and attempts>0 when bounded")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn restart() -> RestartPolicy {
        RestartPolicy { attempts: 3, interval_secs: 300, delay_secs: 15, mode: RestartMode::Fail }
    }

    fn reschedule() -> ReschedulePolicy {
        ReschedulePolicy {
            attempts: 5,
            interval_secs: 3600,
            delay_secs: 30,
            delay_function: DelayFunction::Exponential,
            max_delay_secs: 3600,
            unlimited: false,
        }
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn valid_restart_passes() {
        assert!(restart().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn restart_rejects_delay_above_interval() {
        let mut r = restart();
        r.delay_secs = 400;
        assert!(r.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn valid_reschedule_passes() {
        assert!(reschedule().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn reschedule_rejects_cap_not_above_delay() {
        let mut r = reschedule();
        r.delay_secs = 3600;
        r.max_delay_secs = 3600;
        assert!(r.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn bounded_reschedule_rejects_zero_attempts() {
        let mut r = reschedule();
        r.unlimited = false;
        r.attempts = 0;
        assert!(r.validate().is_err());
    }
}
