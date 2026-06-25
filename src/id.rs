// SPDX-License-Identifier: Apache-2.0

//! Strongly-typed identifiers for the core domain entities.
//!
//! Every id was historically a bare [`String`], so nothing stopped a node id
//! being passed where a job id was wanted. These newtypes make that a compile
//! error while staying byte-for-byte wire-compatible: each is
//! `#[serde(transparent)]`, so on-disk JSON, the raft log, and RPC frames are
//! unchanged.
//!
//! The types deliberately expose `Borrow<str>`, `PartialEq<&str>`, `is_empty`,
//! and `as_str` so that `HashMap` lookups by `&str`, equality against string
//! literals, and the existing `validate()` emptiness checks keep working
//! without ceremony — the type only guards *cross-id* confusion, which is the
//! bug that actually happens.

// Item names (`JobId`, `NodeId`, …) intentionally echo the `id` module.
#![allow(clippy::module_name_repetitions, reason = "id newtypes belong in the id module")]

/// Generate a `String`-backed id newtype with the trait surface every call
/// site needs. One macro instead of four hand-copied structs.
macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Debug, Clone, PartialEq, Eq, Hash, Default,
            serde::Serialize, serde::Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// View the id as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Whether the id is the empty string.
            #[must_use]
            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_owned())
            }
        }

        // Lets `HashMap<$name, V>` be queried with a plain `&str`.
        impl std::borrow::Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        // Keeps `id == "literal"` and `a.node_id == node_id_str` compiling.
        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.0 == *other
            }
        }
    };
}

id_type! {
    /// Identifies a job by its (unique) name.
    JobId
}
id_type! {
    /// Identifies a cluster node.
    NodeId
}
id_type! {
    /// Identifies an allocation.
    AllocId
}
id_type! {
    /// Identifies a scheduler evaluation.
    EvalId
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn serde_is_transparent() {
        let id = JobId::from("redis");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"redis\"");
        let back: JobId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn borrow_enables_str_lookup() {
        let mut m = std::collections::HashMap::new();
        m.insert(NodeId::from("n1"), 7);
        assert_eq!(m.get("n1"), Some(&7));
    }

    #[test]
    fn eq_against_str_literal() {
        let a = AllocId::from("a1");
        assert_eq!(a, "a1"); // exercises PartialEq<&str>
        assert_eq!(EvalId::from("e1").as_str(), "e1");
        assert!(JobId::default().is_empty());
    }
}
