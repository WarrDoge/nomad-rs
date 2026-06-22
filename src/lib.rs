//! `nomad-rs` — HashiCorp Nomad rewrite in Rust under Apache License 2.0.
//!
//! This crate is a from-scratch reimplementation of the Nomad scheduler,
//! client agent, and server components in idiomatic Rust.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

/// Nomad client configuration and lifecycle.
pub mod client;

/// Nomad server configuration and lifecycle.
pub mod server;

/// Shared configuration types.
pub mod config;

/// Nomad job specification types.
pub mod jobspec;

/// Scheduler and evaluation engine.
pub mod scheduler;

/// Error types used across the project.
pub mod error;

/// Prelude — re-exports common traits and types.
pub mod prelude {
    pub use crate::error::Result;
    pub use crate::config::{Config, LogLevel};
}
