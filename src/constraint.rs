// SPDX-License-Identifier: Apache-2.0

//! Placement constraints, affinities, and spread.
//!
//! These shape where the scheduler may (constraint) or prefers to (affinity,
//! spread) place a task group. Mirrors the subset of upstream Nomad's
//! `structs.Constraint`/`Affinity`/`Spread`. Behaviour is specified by the
//! tests and is unimplemented.

use std::collections::HashMap;

use crate::error::Result;

/// Operators recognised by the Nomad constraint system.
const KNOWN_OPERANDS: &[&str] =
    &["=", "!=", ">", ">=", "<", "<=", "regexp", "version", "set_contains", "distinct_hosts", "distinct_property"];

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
        if self.left.is_empty() {
            return Err(crate::error::Error::Config("constraint left target cannot be empty".to_owned()));
        }
        if self.right.is_empty() && !matches!(self.operand.as_str(), "distinct_hosts" | "distinct_property") {
            return Err(crate::error::Error::Config("constraint right target cannot be empty".to_owned()));
        }
        if !KNOWN_OPERANDS.contains(&self.operand.as_str()) {
            return Err(crate::error::Error::Config(format!("unknown constraint operand '{}'", self.operand)));
        }
        Ok(())
    }

    /// Whether a node with the given `attributes` satisfies this constraint.
    #[must_use]
    pub fn satisfied_by(&self, attributes: &HashMap<String, String>) -> bool {
        let attr_val = attributes.get(self.left.as_str()).map(String::as_str).unwrap_or("");
        let right = self.right.as_str();

        match self.operand.as_str() {
            "=" => attr_val == right,
            "!=" => attr_val != right,
            ">" => cmp_num(attr_val, right, |a, b| a > b),
            ">=" => cmp_num(attr_val, right, |a, b| a >= b),
            "<" => cmp_num(attr_val, right, |a, b| a < b),
            "<=" => cmp_num(attr_val, right, |a, b| a <= b),
            "regexp" => attr_val.contains(right.trim_matches('*')),
            "version" => attr_val == right,
            "set_contains" => attr_val.split(',').any(|v| v.trim() == right),
            "distinct_hosts" => true,
            "distinct_property" => true,
            _ => false,
        }
    }
}

/// Numeric comparison using the given predicate.
fn cmp_num(left: &str, right: &str, cmp: fn(i64, i64) -> bool) -> bool {
    let Ok(l) = left.parse::<i64>() else { return false };
    let Ok(r) = right.parse::<i64>() else { return false };
    cmp(l, r)
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
        if !(-100..=100).contains(&self.weight) {
            return Err(crate::error::Error::Config(format!(
                "affinity weight {} out of range [-100, 100]",
                self.weight
            )));
        }
        if self.left.is_empty() {
            return Err(crate::error::Error::Config("affinity left target cannot be empty".to_owned()));
        }
        if !KNOWN_OPERANDS.contains(&self.operand.as_str()) {
            return Err(crate::error::Error::Config(format!("unknown affinity operand '{}'", self.operand)));
        }
        Ok(())
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
        if self.attribute.is_empty() {
            return Err(crate::error::Error::Config("spread attribute cannot be empty".to_owned()));
        }
        let total: u16 = self.targets.iter().map(|t| u16::from(t.percent)).sum();
        if total > 100 {
            return Err(crate::error::Error::Config(format!("spread target percents sum to {total}, exceeding 100")));
        }
        Ok(())
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
    fn equality_constraint_validates() {
        let c = Constraint { left: "${attr.os}".to_owned(), right: "linux".to_owned(), operand: "=".to_owned() };
        assert!(c.validate().is_ok());
    }

    #[test]
    fn unknown_operand_rejected() {
        let c = Constraint { left: "a".to_owned(), right: "b".to_owned(), operand: "bogus".to_owned() };
        assert!(c.validate().is_err());
    }

    #[test]
    fn equality_satisfied_when_attr_matches() {
        let c = Constraint { left: "os".to_owned(), right: "linux".to_owned(), operand: "=".to_owned() };
        assert!(c.satisfied_by(&attrs()));
    }

    #[test]
    fn equality_unsatisfied_when_attr_differs() {
        let c = Constraint { left: "os".to_owned(), right: "windows".to_owned(), operand: "=".to_owned() };
        assert!(!c.satisfied_by(&attrs()));
    }

    #[test]
    fn affinity_accepts_weight_in_range() {
        let a = Affinity { left: "a".to_owned(), right: "b".to_owned(), operand: "=".to_owned(), weight: 50 };
        assert!(a.validate().is_ok());
    }

    #[test]
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
    fn spread_accepts_within_100_percent() {
        let s = Spread {
            attribute: "${node.datacenter}".to_owned(),
            targets: vec![SpreadTarget { value: "dc1".to_owned(), percent: 60 }],
        };
        assert!(s.validate().is_ok());
    }
}
