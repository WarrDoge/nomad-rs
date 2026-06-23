// SPDX-License-Identifier: Apache-2.0

//! Configuration types for Nomad client and server agents.
//!
//! These types mirror the configuration structure from the original Nomad
//! project, adapted for idiomatic Rust.  Config can be read from a TOML file,
//! overridden by environment variables, and overridden again by CLI flags.

use std::path::PathBuf;

use serde::Deserialize;

use crate::error::Result;

/// Log verbosity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
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
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "error" => Some(Self::Error),
            "warn" | "warning" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            "trace" => Some(Self::Trace),
            _ => None,
        }
    }

    /// Return the env-filter directive for this level.
    #[must_use]
    pub const fn filter_directive(&self) -> &'static str {
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
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
    /// Path to the configuration file (used for SIGHUP reload).
    #[serde(skip)]
    pub config_file: Option<PathBuf>,
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
            config_file: None,
        }
    }
}

impl Config {
    /// Read a TOML config from a file path and merge with defaults.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let mut config: Self = toml::from_str(&contents)?;
        config.config_file = Some(path.to_owned());
        Ok(config)
    }

    /// Merge environment variable overrides into this config.
    ///
    /// Supported env vars:
    /// - `NOMAD_DATA_DIR`
    /// - `NOMAD_LOG_DIR`
    /// - `NOMAD_LOG_LEVEL`
    /// - `NOMAD_BIND_ADDR`
    /// - `NOMAD_DATACENTER`
    /// - `NOMAD_NODE_NAME`
    /// - `NOMAD_REGION`
    #[must_use]
    pub fn merge_env(mut self) -> Self {
        if let Ok(v) = std::env::var("NOMAD_DATA_DIR") {
            self.data_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("NOMAD_LOG_DIR") {
            self.log_dir = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("NOMAD_LOG_LEVEL") {
            if let Some(level) = LogLevel::parse(&v) {
                self.log_level = level;
            }
        }
        if let Ok(v) = std::env::var("NOMAD_BIND_ADDR") {
            self.bind_addr = v;
        }
        if let Ok(v) = std::env::var("NOMAD_DATACENTER") {
            self.datacenter = v;
        }
        if let Ok(v) = std::env::var("NOMAD_NODE_NAME") {
            self.node_name = v;
        }
        if let Ok(v) = std::env::var("NOMAD_REGION") {
            self.region = v;
        }
        self
    }

    /// Merge CLI flag overrides into this config.
    ///
    /// Each field is set only when the corresponding `Option` is `Some`.
    #[must_use]
    pub fn merge_cli(
        self,
        data_dir: Option<PathBuf>,
        log_dir: Option<PathBuf>,
        log_level: Option<LogLevel>,
        bind_addr: Option<String>,
        node_name: Option<String>,
        region: Option<String>,
    ) -> Self {
        Self {
            data_dir: data_dir.unwrap_or(self.data_dir),
            log_dir: log_dir.unwrap_or(self.log_dir),
            log_level: log_level.unwrap_or(self.log_level),
            bind_addr: bind_addr.unwrap_or(self.bind_addr),
            node_name: node_name.unwrap_or(self.node_name),
            region: region.unwrap_or(self.region),
            ..self
        }
    }

    /// Validate the config, returning `Ok(())` or a [`crate::error::Error::Validation`].
    ///
    /// # Errors
    ///
    /// Returns `Validation` if `bind_addr` is empty or `data_dir` is empty.
    pub fn validate(&self) -> Result<()> {
        if self.bind_addr.trim().is_empty() {
            return Err(crate::error::Error::Validation("bind_addr must not be empty".to_owned()));
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err(crate::error::Error::Validation("data_dir must not be empty".to_owned()));
        }
        Ok(())
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
        assert_eq!(LogLevel::parse("error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse("warn"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse("info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::parse("debug"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::parse("trace"), Some(LogLevel::Trace));
    }

    #[test]
    fn test_log_level_from_str_case_insensitive() {
        assert_eq!(LogLevel::parse("ERROR"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse("Info"), Some(LogLevel::Info));
    }

    #[test]
    fn test_log_level_from_str_variant() {
        assert_eq!(LogLevel::parse("warning"), Some(LogLevel::Warn));
    }

    #[test]
    fn test_log_level_from_str_invalid() {
        assert_eq!(LogLevel::parse("invalid"), None);
        assert_eq!(LogLevel::parse(""), None);
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
        assert!(config.config_file.is_none());
    }

    #[test]
    fn test_config_equality() {
        let a = Config { node_name: "test-node".to_owned(), ..Config::default() };
        let b = Config { node_name: "test-node".to_owned(), ..Config::default() };
        assert_eq!(a, b);

        let c = Config { node_name: "other-node".to_owned(), ..Config::default() };
        assert_ne!(a, c);
    }

    #[test]
    fn test_log_level_from_str_unknown() {
        assert!(LogLevel::parse("verbose").is_none());
        assert!(LogLevel::parse("silent").is_none());
    }

    #[test]
    fn test_config_from_file() {
        let dir = std::env::temp_dir().join("nomad-rs-test-config");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            r#"
data_dir = "/tmp/nomad-data"
bind_addr = "127.0.0.1:4646"
datacenter = "us-east-1"
region = "us"
node_name = "test-node"
log_level = "debug"
log_dir = "/tmp/nomad-logs"
"#,
        )
        .unwrap();

        let config = Config::from_file(&path).unwrap();
        assert_eq!(config.data_dir, PathBuf::from("/tmp/nomad-data"));
        assert_eq!(config.bind_addr, "127.0.0.1:4646");
        assert_eq!(config.datacenter, "us-east-1");
        assert_eq!(config.region, "us");
        assert_eq!(config.node_name, "test-node");
        assert_eq!(config.log_level, LogLevel::Debug);
        assert_eq!(config.log_dir, PathBuf::from("/tmp/nomad-logs"));
        assert_eq!(config.config_file, Some(path));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_config_validate_ok() {
        let config = Config::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validate_empty_bind_addr() {
        let config = Config { bind_addr: "".to_owned(), ..Config::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validate_empty_data_dir() {
        let config = Config { data_dir: PathBuf::new(), ..Config::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_merge_cli() {
        let base = Config::default();
        let merged = base.merge_cli(
            Some(PathBuf::from("/cli/data")),
            None,
            Some(LogLevel::Trace),
            None,
            Some("cli-node".to_owned()),
            Some("cli-region".to_owned()),
        );
        assert_eq!(merged.data_dir, PathBuf::from("/cli/data"));
        assert_eq!(merged.log_level, LogLevel::Trace);
        assert_eq!(merged.node_name, "cli-node");
        assert_eq!(merged.region, "cli-region");
        assert_eq!(merged.bind_addr, "0.0.0.0:4646"); // unchanged
    }

    #[test]
    fn test_filter_directive() {
        assert_eq!(LogLevel::Error.filter_directive(), "error");
        assert_eq!(LogLevel::Trace.filter_directive(), "trace");
    }
}
