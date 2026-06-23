// SPDX-License-Identifier: Apache-2.0

//! Node fingerprinting.
//!
//! Fingerprinters detect a node's resources, drivers, and attributes so the
//! scheduler can match constraints. Mirrors the subset of upstream Nomad's
//! client fingerprint package. The [`Fingerprinter`](crate::fingerprint::Fingerprinter) trait is the contract;
//! [`CpuFingerprinter`](crate::fingerprint::CpuFingerprinter) is one implementation whose behaviour is specified by
//! the tests and is unimplemented.

use std::collections::HashMap;

use crate::error::Result;

/// Detects a slice of a node's attributes.
pub trait Fingerprinter {
    /// Stable name of this fingerprinter, e.g. `"cpu"`.
    fn name(&self) -> &'static str;

    /// Detect attributes for the node (e.g. `"cpu.totalcompute" => "4000"`).
    ///
    /// # Errors
    ///
    /// Returns an error if detection fails (e.g. a probe cannot be read).
    fn detect(&self) -> Result<HashMap<String, String>>;
}

/// Fingerprints CPU capacity and topology.
#[derive(Debug, Default)]
pub struct CpuFingerprinter;

impl Fingerprinter for CpuFingerprinter {
    fn name(&self) -> &'static str {
        todo!("return the static name \"cpu\"")
    }

    fn detect(&self) -> Result<HashMap<String, String>> {
        todo!("probe CPU and return cpu.* attributes including total compute")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn cpu_fingerprinter_is_named_cpu() {
        assert_eq!(CpuFingerprinter.name(), "cpu");
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn cpu_detects_total_compute() {
        let attrs = CpuFingerprinter.detect().unwrap();
        assert!(attrs.contains_key("cpu.totalcompute"));
    }
}
