// SPDX-License-Identifier: Apache-2.0

//! Configuration types for Nomad client and server agents.
//!
//! These types mirror the configuration structure from the original Nomad
//! project, adapted for idiomatic Rust.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::error::{Error, Result};

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
        let hostname =
            std::env::var("HOSTNAME").or_else(|_| std::env::var("HOST")).unwrap_or_else(|_| "localhost".to_owned());

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

impl Config {
    /// Validate the agent configuration before an agent starts.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if the bind address cannot be parsed as a
    /// `host:port` socket address, or if a required field (data directory,
    /// datacenter, region, node name) is empty.
    pub fn validate(&self) -> Result<()> {
        if self.bind_addr.parse::<SocketAddr>().is_err() {
            return Err(Error::Config(format!("invalid bind address: {:?}", self.bind_addr)));
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err(Error::Config("missing data directory".to_owned()));
        }
        if self.datacenter.is_empty() {
            return Err(Error::Config("missing datacenter".to_owned()));
        }
        if self.region.is_empty() {
            return Err(Error::Config("missing region".to_owned()));
        }
        if self.node_name.is_empty() {
            return Err(Error::Config("missing node name".to_owned()));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        Config::default()
    }

    #[test]
    fn default_config_matches_nomad_conventions() {
        let cfg = Config::default();
        assert_eq!(cfg.bind_addr, "0.0.0.0:4646");
        assert_eq!(cfg.datacenter, "dc1");
        assert_eq!(cfg.region, "global");
        assert_eq!(cfg.log_level, LogLevel::Info);
        assert_eq!(cfg.data_dir, PathBuf::from("/opt/nomad/data"));
        assert_eq!(cfg.log_dir, PathBuf::from("/opt/nomad/log"));
    }

    #[test]
    fn default_node_name_is_non_empty() {
        assert!(!Config::default().node_name.is_empty());
    }

    #[test]
    fn log_level_as_str_round_trips() {
        assert_eq!(LogLevel::Error.as_str(), "error");
        assert_eq!(LogLevel::Warn.as_str(), "warn");
        assert_eq!(LogLevel::Info.as_str(), "info");
        assert_eq!(LogLevel::Debug.as_str(), "debug");
        assert_eq!(LogLevel::Trace.as_str(), "trace");
    }

    #[test]
    fn log_level_display_matches_as_str() {
        for level in [LogLevel::Error, LogLevel::Warn, LogLevel::Info, LogLevel::Debug, LogLevel::Trace] {
            assert_eq!(level.to_string(), level.as_str());
        }
    }

    // ---- Config::validate ----

    #[test]
    fn default_config_validates() {
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn validate_accepts_custom_bind_addr() {
        let mut cfg = valid_config();
        cfg.bind_addr = "127.0.0.1:8080".to_owned();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_unparseable_bind_addr() {
        let mut cfg = valid_config();
        cfg.bind_addr = "not-an-address".to_owned();
        assert!(cfg.validate().unwrap_err().to_string().contains("bind"));
    }

    #[test]
    fn validate_rejects_bind_addr_without_port() {
        let mut cfg = valid_config();
        cfg.bind_addr = "0.0.0.0".to_owned();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_data_dir() {
        let mut cfg = valid_config();
        cfg.data_dir = PathBuf::new();
        assert!(cfg.validate().unwrap_err().to_string().contains("data"));
    }

    #[test]
    fn validate_rejects_empty_datacenter() {
        let mut cfg = valid_config();
        cfg.datacenter = String::new();
        assert!(cfg.validate().unwrap_err().to_string().contains("datacenter"));
    }

    #[test]
    fn validate_rejects_empty_region() {
        let mut cfg = valid_config();
        cfg.region = String::new();
        assert!(cfg.validate().unwrap_err().to_string().contains("region"));
    }

    #[test]
    fn validate_rejects_empty_node_name() {
        let mut cfg = valid_config();
        cfg.node_name = String::new();
        assert!(cfg.validate().unwrap_err().to_string().contains("node name"));
    }
}
