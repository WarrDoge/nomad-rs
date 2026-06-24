// SPDX-License-Identifier: Apache-2.0

//! Access control: tokens, policies, and authorization.
//!
//! Mirrors the subset of upstream Nomad's ACL model: a token carries policies,
//! a policy grants capabilities on resources, and a management token bypasses
//! checks. Behaviour is specified by the tests and is unimplemented.

use std::collections::HashMap;

use crate::error::Result;

/// A capability level on a resource.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Capability {
    /// May read the resource.
    Read,
    /// May read and modify the resource.
    Write,
    /// Explicitly denied (overrides grants).
    Deny,
}

/// A named policy granting capabilities per resource key.
#[derive(Debug, Clone)]
pub struct Policy {
    /// Policy name.
    pub name: String,
    /// Resource key (e.g. `"namespace:default"`) to granted capability.
    pub rules: HashMap<String, Capability>,
}

/// An ACL token.
#[derive(Debug, Clone)]
pub struct Token {
    /// Public accessor id.
    pub accessor: String,
    /// Secret id presented on requests.
    pub secret: String,
    /// Names of policies attached to this token.
    pub policies: Vec<String>,
    /// Management tokens bypass all policy checks.
    pub management: bool,
}

impl Token {
    /// Validate the token.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `accessor`/`secret` are empty,
    /// or a non-management token has no policies.
    pub fn validate(&self) -> Result<()> {
        if self.accessor.is_empty() {
            return Err(crate::error::Error::Config("token accessor cannot be empty".to_owned()));
        }
        if self.secret.is_empty() {
            return Err(crate::error::Error::Config("token secret cannot be empty".to_owned()));
        }
        if !self.management && self.policies.is_empty() {
            return Err(crate::error::Error::Config("non-management token requires at least one policy".to_owned()));
        }
        Ok(())
    }

    /// Whether this token grants `capability` on `resource`, resolving against
    /// the given `policies`. Management tokens always allow; an explicit
    /// [`Capability::Deny`] always wins.
    #[must_use]
    pub fn allows(&self, resource: &str, capability: Capability, policies: &[Policy]) -> bool {
        if self.management {
            return true;
        }
        for policy in policies {
            if !self.policies.contains(&policy.name) {
                continue;
            }
            match policy.rules.get(resource) {
                Some(Capability::Deny) => return false,
                Some(grant) if grant >= &capability => return true,
                _ => {},
            }
        }
        false
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn policy() -> Policy {
        Policy { name: "dev".to_owned(), rules: HashMap::from([("namespace:default".to_owned(), Capability::Write)]) }
    }

    fn token() -> Token {
        Token {
            accessor: "acc-1".to_owned(),
            secret: "sec-1".to_owned(),
            policies: vec!["dev".to_owned()],
            management: false,
        }
    }

    fn management() -> Token {
        Token { accessor: "acc-0".to_owned(), secret: "sec-0".to_owned(), policies: vec![], management: true }
    }

    #[test]
    fn valid_token_passes() {
        assert!(token().validate().is_ok());
    }

    #[test]
    fn non_management_without_policies_errors() {
        let mut t = token();
        t.policies.clear();
        assert!(t.validate().is_err());
    }

    #[test]
    fn management_token_allows_anything() {
        assert!(management().allows("namespace:prod", Capability::Write, &[]));
    }

    #[test]
    fn policy_grants_write() {
        assert!(token().allows("namespace:default", Capability::Write, &[policy()]));
    }

    #[test]
    fn missing_grant_denies() {
        assert!(!token().allows("namespace:other", Capability::Write, &[policy()]));
    }
}
