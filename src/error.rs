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
