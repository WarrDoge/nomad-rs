// SPDX-License-Identifier: Apache-2.0

//! Placement constraints, affinities, and spread.
//!
//! These shape where the scheduler may (constraint) or prefers to (affinity,
//! spread) place a task group. Mirrors the subset of upstream Nomad's
//! `structs.Constraint`/`Affinity`/`Spread`. Behaviour is specified by the
//! tests and is unimplemented.

use std::collections::HashMap;

use crate::error::Result;

/// A hard placement rule: a node must satisfy it to be feasible.
#[derive(Debug, Clone)]
pub struct Constraint {
    /// Left-hand target, e.g. `"${attr.os.name}"`.
    pub left: String,
    /// Right-hand target, e.g. `"linux"`.
    pub right: String,
    /// Operator: one of `=`, `!=`, `>`, `>=`, `<`, `<=`, `regexp`, `version`,
    /// `set_contains`, `distinct_hosts`, `distinct_property`.
    pub operand: String,
}

impl Constraint {
    /// Validate that the operand is recognised and targets are present.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] for an unknown operand or empty
    /// target where the operand requires one.
    pub fn validate(&self) -> Result<()> {
        todo!("reject unknown operands and missing targets")
    }

    /// Whether a node with the given `attributes` satisfies this constraint.
    #[must_use]
    pub fn satisfied_by(&self, attributes: &HashMap<String, String>) -> bool {
        let _ = attributes;
        todo!("evaluate {:?} against the resolved attribute value", self.operand)
    }
}

/// A soft placement preference with a weight; nudges ranking, never excludes.
#[derive(Debug, Clone)]
pub struct Affinity {
    /// Left-hand target.
    pub left: String,
    /// Right-hand target.
    pub right: String,
    /// Operator (same vocabulary as [`Constraint::operand`]).
    pub operand: String,
    /// Weight in `-100..=100`; negative repels, positive attracts.
    pub weight: i8,
}

impl Affinity {
    /// Validate the affinity.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `weight` is outside
    /// `-100..=100` (and not zero-meaningless) or the operand is unknown.
    pub fn validate(&self) -> Result<()> {
        todo!("require weight in -100..=100 (non-zero) and a known operand")
    }
}

/// A single spread target: a desired share of allocations for an attribute value.
#[derive(Debug, Clone)]
pub struct SpreadTarget {
    /// Attribute value this target applies to, e.g. a datacenter name.
    pub value: String,
    /// Desired percent of allocations on this value (`0..=100`).
    pub percent: u8,
}

/// Spreads allocations across the values of an attribute.
#[derive(Debug, Clone)]
pub struct Spread {
    /// Attribute to spread over, e.g. `"${node.datacenter}"`.
    pub attribute: String,
    /// Per-value targets; percents must not exceed 100 in total.
    pub targets: Vec<SpreadTarget>,
}

impl Spread {
    /// Validate the spread.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `attribute` is empty or the
    /// target percents sum above 100.
    pub fn validate(&self) -> Result<()> {
        todo!("require an attribute and target percents summing to <= 100")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn attrs() -> HashMap<String, String> {
        HashMap::from([("os".to_owned(), "linux".to_owned())])
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn equality_constraint_validates() {
        let c = Constraint { left: "${attr.os}".to_owned(), right: "linux".to_owned(), operand: "=".to_owned() };
        assert!(c.validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn unknown_operand_rejected() {
        let c = Constraint { left: "a".to_owned(), right: "b".to_owned(), operand: "bogus".to_owned() };
        assert!(c.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn equality_satisfied_when_attr_matches() {
        let c = Constraint { left: "os".to_owned(), right: "linux".to_owned(), operand: "=".to_owned() };
        assert!(c.satisfied_by(&attrs()));
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn equality_unsatisfied_when_attr_differs() {
        let c = Constraint { left: "os".to_owned(), right: "windows".to_owned(), operand: "=".to_owned() };
        assert!(!c.satisfied_by(&attrs()));
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn affinity_accepts_weight_in_range() {
        let a = Affinity { left: "a".to_owned(), right: "b".to_owned(), operand: "=".to_owned(), weight: 50 };
        assert!(a.validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn spread_rejects_over_100_percent() {
        let s = Spread {
            attribute: "${node.datacenter}".to_owned(),
            targets: vec![
                SpreadTarget { value: "dc1".to_owned(), percent: 80 },
                SpreadTarget { value: "dc2".to_owned(), percent: 40 },
            ],
        };
        assert!(s.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn spread_accepts_within_100_percent() {
        let s = Spread {
            attribute: "${node.datacenter}".to_owned(),
            targets: vec![SpreadTarget { value: "dc1".to_owned(), percent: 60 }],
        };
        assert!(s.validate().is_ok());
    }
}
