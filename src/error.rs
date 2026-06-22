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

    /// A validation error occurred.
    #[error("validation error: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_io() {
        let err = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"));
        assert!(err.to_string().contains("io error"));
        assert!(err.to_string().contains("file not found"));
    }

    #[test]
    fn test_error_display_config() {
        let err = Error::Config("invalid port range".to_owned());
        assert_eq!(err.to_string(), "config error: invalid port range");
    }

    #[test]
    fn test_error_display_runtime() {
        let err = Error::Runtime("raft leader election failed".to_owned());
        assert_eq!(err.to_string(), "runtime error: raft leader election failed");
    }

    #[test]
    fn test_error_display_serialize() {
        let inner = serde_json::from_str::<String>("not valid json").unwrap_err();
        let err = Error::Serialize(inner);
        assert!(err.to_string().contains("serialisation error"));
    }

    #[test]
    fn test_error_display_validation() {
        let err = Error::Validation("job name cannot be empty".to_owned());
        assert_eq!(err.to_string(), "validation error: job name cannot be empty");
    }

    #[test]
    fn test_error_io_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let err: Error = io_err.into();
        assert!(matches!(err, Error::Io(_)));
    }

    #[test]
    fn test_error_debug() {
        let err = Error::Runtime("test".to_owned());
        let debug = format!("{err:?}");
        assert!(debug.contains("Runtime"));
    }

    #[test]
    fn test_error_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Error>();
    }

    #[test]
    fn test_result_type() {
        let ok: Result<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: Result<i32> = Err(Error::Runtime("fail".to_owned()));
        assert!(err.is_err());
    }

    #[test]
    fn test_error_is_error_trait() {
        fn is_std_error(_e: &dyn std::error::Error) {}
        is_std_error(&Error::Config("test".to_owned()));
    }
}
