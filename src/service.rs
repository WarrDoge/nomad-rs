// SPDX-License-Identifier: Apache-2.0

//! Service registration and health checks.
//!
//! Task groups can register services (with Consul or Nomad's native registry)
//! and attach health checks. Mirrors the subset of upstream Nomad's
//! `structs.Service`/`ServiceCheck`. Behaviour is specified by the tests and is
//! unimplemented.

use crate::error::Result;

/// How a health check probes a service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckType {
    /// TCP connect check.
    Tcp,
    /// HTTP GET against a path.
    Http,
    /// Run a script in the task and use its exit code.
    Script,
    /// gRPC health check.
    Grpc,
}

/// Where a service is registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceProvider {
    /// Consul service catalog.
    Consul,
    /// Nomad's native service registry.
    Nomad,
}

/// A health check attached to a service.
#[derive(Debug, Clone)]
pub struct ServiceCheck {
    /// Check name.
    pub name: String,
    /// Probe kind.
    pub check_type: CheckType,
    /// How often to run the check, in seconds (> 0).
    pub interval_secs: u64,
    /// Per-probe timeout, in seconds (> 0 and < `interval_secs`).
    pub timeout_secs: u64,
    /// Path to request for [`CheckType::Http`]; required for HTTP checks.
    pub path: Option<String>,
}

impl ServiceCheck {
    /// Validate the check timing and type-specific requirements.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if interval/timeout are zero,
    /// timeout >= interval, or an HTTP check is missing its path.
    pub fn validate(&self) -> Result<()> {
        todo!("require interval>0, 0<timeout<interval, and a path for HTTP checks")
    }
}

/// A service exposed by a task group.
#[derive(Debug, Clone)]
pub struct Service {
    /// Service name registered in the catalog.
    pub name: String,
    /// Port label this service is advertised on.
    pub port_label: String,
    /// Registry provider.
    pub provider: ServiceProvider,
    /// Tags applied to the registration.
    pub tags: Vec<String>,
    /// Health checks for the service.
    pub checks: Vec<ServiceCheck>,
}

impl Service {
    /// Validate the service and all of its checks.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if the name is empty or any
    /// check is invalid.
    pub fn validate(&self) -> Result<()> {
        todo!("require a non-empty name and validate every check")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn http_check() -> ServiceCheck {
        ServiceCheck {
            name: "alive".to_owned(),
            check_type: CheckType::Http,
            interval_secs: 10,
            timeout_secs: 2,
            path: Some("/health".to_owned()),
        }
    }

    fn service() -> Service {
        Service {
            name: "web".to_owned(),
            port_label: "http".to_owned(),
            provider: ServiceProvider::Nomad,
            tags: vec!["v1".to_owned()],
            checks: vec![http_check()],
        }
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn valid_service_passes() {
        assert!(service().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn service_rejects_empty_name() {
        let mut s = service();
        s.name = String::new();
        assert!(s.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn http_check_passes() {
        assert!(http_check().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn http_check_requires_path() {
        let mut c = http_check();
        c.path = None;
        assert!(c.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn check_rejects_timeout_ge_interval() {
        let mut c = http_check();
        c.timeout_secs = 10;
        c.interval_secs = 10;
        assert!(c.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn check_rejects_zero_interval() {
        let mut c = http_check();
        c.interval_secs = 0;
        assert!(c.validate().is_err());
    }
}
