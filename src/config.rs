// SPDX-License-Identifier: Apache-2.0

//! Configuration types for Nomad client and server agents.
//!
//! These types mirror the configuration structure from the original Nomad
//! project, adapted for idiomatic Rust.

use std::path::PathBuf;

/// Log verbosity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Error-level messages only.
    Error,
    /// Warning-level and above.
    Warn,
    /// Informational messages and above.
    Info,
    /// Debug messages and above.
    Debug,
    /// Trace-level messages (most verbose).
    Trace,
}

impl LogLevel {
    /// Return the string representation of this log level.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Top-level configuration for a Nomad agent (client, server, or both).
#[derive(Debug, Clone)]
pub struct Config {
    /// Directory for storing persistent data.
    pub data_dir: PathBuf,
    /// Directory for storing logs.
    pub log_dir: PathBuf,
    /// Log verbosity level.
    pub log_level: LogLevel,
    /// The network address to bind to.
    pub bind_addr: String,
    /// The datacenter this node belongs to.
    pub datacenter: String,
    /// The node name (defaults to hostname).
    pub node_name: String,
    /// The region this node belongs to.
    pub region: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/opt/nomad/data"),
            log_dir: PathBuf::from("/opt/nomad/log"),
            log_level: LogLevel::Info,
            bind_addr: "0.0.0.0:4646".to_owned(),
            datacenter: "dc1".to_owned(),
            node_name: String::new(),
            region: "global".to_owned(),
        }
    }
}
