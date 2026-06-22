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

    /// Parse a log level from a string, returning `None` if the string
    /// does not match a known level.
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "error" => Some(Self::Error),
            "warn" | "warning" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            "trace" => Some(Self::Trace),
            _ => None,
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Top-level configuration for a Nomad agent (client, server, or both).
#[derive(Debug, Clone, PartialEq, Eq)]
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
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "localhost".to_owned());

        Self {
            data_dir: PathBuf::from("/opt/nomad/data"),
            log_dir: PathBuf::from("/opt/nomad/log"),
            log_level: LogLevel::Info,
            bind_addr: "0.0.0.0:4646".to_owned(),
            datacenter: "dc1".to_owned(),
            node_name: hostname,
            region: "global".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_as_str() {
        assert_eq!(LogLevel::Error.as_str(), "error");
        assert_eq!(LogLevel::Warn.as_str(), "warn");
        assert_eq!(LogLevel::Info.as_str(), "info");
        assert_eq!(LogLevel::Debug.as_str(), "debug");
        assert_eq!(LogLevel::Trace.as_str(), "trace");
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(format!("{}", LogLevel::Error), "error");
        assert_eq!(format!("{}", LogLevel::Trace), "trace");
    }

    #[test]
    fn test_log_level_from_str_exact() {
        assert_eq!(LogLevel::from_str("error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::from_str("warn"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::from_str("info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::from_str("debug"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::from_str("trace"), Some(LogLevel::Trace));
    }

    #[test]
    fn test_log_level_from_str_case_insensitive() {
        assert_eq!(LogLevel::from_str("ERROR"), Some(LogLevel::Error));
        assert_eq!(LogLevel::from_str("Info"), Some(LogLevel::Info));
    }

    #[test]
    fn test_log_level_from_str_variant() {
        assert_eq!(LogLevel::from_str("warning"), Some(LogLevel::Warn));
    }

    #[test]
    fn test_log_level_from_str_invalid() {
        assert_eq!(LogLevel::from_str("invalid"), None);
        assert_eq!(LogLevel::from_str(""), None);
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.data_dir, PathBuf::from("/opt/nomad/data"));
        assert_eq!(config.log_dir, PathBuf::from("/opt/nomad/log"));
        assert_eq!(config.log_level, LogLevel::Info);
        assert_eq!(config.bind_addr, "0.0.0.0:4646");
        assert_eq!(config.datacenter, "dc1");
        assert_eq!(config.region, "global");
        assert!(!config.node_name.is_empty());
    }

    #[test]
    fn test_config_equality() {
        let a = Config {
            node_name: "test-node".to_owned(),
            ..Config::default()
        };
        let b = Config {
            node_name: "test-node".to_owned(),
            ..Config::default()
        };
        assert_eq!(a, b);

        let c = Config {
            node_name: "other-node".to_owned(),
            ..Config::default()
        };
        assert_ne!(a, c);
    }

    #[test]
    fn test_log_level_debug() {
        assert_eq!(format!("{:?}", LogLevel::Info), "Info");
    }

    #[test]
    fn test_log_level_clone() {
        let level = LogLevel::Debug;
        let cloned = level;
        assert_eq!(level, cloned);
    }

    #[test]
    fn test_log_level_copy() {
        let level = LogLevel::Warn;
        let copied = level;
        assert_eq!(level, copied);
    }

    #[test]
    fn test_log_level_from_str_unknown() {
        assert!(LogLevel::from_str("verbose").is_none());
        assert!(LogLevel::from_str("silent").is_none());
    }
}
