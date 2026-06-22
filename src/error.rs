// SPDX-License-Identifier: Apache-2.0

//! Common error types for the Nomad project.
//!
//! Uses `thiserror` for ergonomic error definitions with
//! automatic `Display` and `Error` trait implementations.

/// A specialised `Result` type for nomad-rs operations.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for all nomad-rs operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An IO error occurred.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// A serialisation error occurred.
    #[error("serialisation error: {0}")]
    Serialize(#[from] serde_json::Error),

    /// A configuration error occurred.
    #[error("config error: {0}")]
    Config(String),

    /// A runtime error occurred.
    #[error("runtime error: {0}")]
    Runtime(String),
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn config_error_displays_with_prefix() {
        let err = Error::Config("bad bind addr".to_owned());
        assert_eq!(err.to_string(), "config error: bad bind addr");
    }

    #[test]
    fn runtime_error_displays_with_prefix() {
        let err = Error::Runtime("raft down".to_owned());
        assert_eq!(err.to_string(), "runtime error: raft down");
    }

    #[test]
    fn io_error_converts_via_from() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "nope");
        let err: Error = io.into();
        // The "io error:" prefix is unique to the `Io` variant, so it both
        // proves `From` routed correctly and exercises the Display arm.
        assert!(err.to_string().starts_with("io error:"));
    }

    #[test]
    fn serde_error_converts_via_from() {
        let serde_err = serde_json::from_str::<i32>("not json").unwrap_err();
        let err: Error = serde_err.into();
        assert!(err.to_string().starts_with("serialisation error:"));
    }
}
