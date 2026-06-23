// SPDX-License-Identifier: Apache-2.0

//! Namespaces: tenancy boundaries for jobs and variables.
//!
//! Mirrors the subset of upstream Nomad's `structs.Namespace`. Behaviour is
//! specified by the tests and is unimplemented.

use crate::error::Result;

/// A tenancy namespace.
#[derive(Debug, Clone)]
pub struct Namespace {
    /// Namespace name (DNS-label-ish: lowercase alphanumerics and dashes).
    pub name: String,
    /// Human-readable description.
    pub description: String,
}

impl Namespace {
    /// Validate the namespace name.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `name` is empty or contains
    /// characters outside `[a-z0-9-]`.
    pub fn validate(&self) -> Result<()> {
        todo!("require a non-empty name matching [a-z0-9-]+")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn default_namespace_passes() {
        let ns = Namespace { name: "default".to_owned(), description: String::new() };
        assert!(ns.validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_empty_name() {
        let ns = Namespace { name: String::new(), description: String::new() };
        assert!(ns.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_invalid_characters() {
        let ns = Namespace { name: "Prod Env".to_owned(), description: String::new() };
        assert!(ns.validate().is_err());
    }
}
