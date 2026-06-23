// SPDX-License-Identifier: Apache-2.0

//! Scaling policies for task groups.
//!
//! Bounds and gates a task group's count for manual or autoscaler-driven
//! changes. Mirrors the subset of upstream Nomad's `structs.ScalingPolicy`.
//! Behaviour is specified by the tests and is unimplemented.

use crate::error::Result;

/// Min/max bounds for a task group's count.
#[derive(Debug, Clone)]
pub struct ScalingPolicy {
    /// Lower bound (inclusive).
    pub min: u32,
    /// Upper bound (inclusive).
    pub max: u32,
    /// Whether the autoscaler may act on this policy.
    pub enabled: bool,
}

impl ScalingPolicy {
    /// Validate the bounds.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `min > max`.
    pub fn validate(&self) -> Result<()> {
        todo!("require min <= max")
    }

    /// Clamp a desired count into `[min, max]`.
    #[must_use]
    pub fn clamp(&self, desired: u32) -> u32 {
        todo!("clamp {desired} into [{}, {}]", self.min, self.max)
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn policy() -> ScalingPolicy {
        ScalingPolicy { min: 1, max: 10, enabled: true }
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn valid_policy_passes() {
        assert!(policy().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_min_above_max() {
        let mut p = policy();
        p.min = 20;
        assert!(p.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn clamps_above_max_to_max() {
        assert_eq!(policy().clamp(99), 10);
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn clamps_below_min_to_min() {
        assert_eq!(policy().clamp(0), 1);
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn leaves_in_range_untouched() {
        assert_eq!(policy().clamp(5), 5);
    }
}
