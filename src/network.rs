// SPDX-License-Identifier: Apache-2.0

//! Network resources: modes and port allocations.
//!
//! Describes a task group's networking: the network mode and its reserved
//! (static) and dynamic ports. Mirrors the subset of upstream Nomad's
//! `structs.NetworkResource`/`Port`. Behaviour is specified by the tests and is
//! unimplemented.

use crate::error::Result;

/// The network isolation mode for a task group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkMode {
    /// Share the host network namespace.
    Host,
    /// Nomad-managed bridge with port mapping.
    Bridge,
    /// No networking.
    None,
    /// A named CNI network (`cni/<name>`).
    Cni(String),
}

/// A port mapping request.
#[derive(Debug, Clone)]
pub struct Port {
    /// Port label, referenced by services (unique within the network).
    pub label: String,
    /// Static host port; `None` means assign a dynamic port.
    pub static_value: Option<u16>,
    /// Container-side port to map to; `None` means same as host.
    pub to: Option<u16>,
}

/// A task group's network resource.
#[derive(Debug, Clone)]
pub struct NetworkResource {
    /// Network mode.
    pub mode: NetworkMode,
    /// Statically reserved ports.
    pub reserved_ports: Vec<Port>,
    /// Dynamically assigned ports.
    pub dynamic_ports: Vec<Port>,
}

impl NetworkResource {
    /// Validate the network resource.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if any port label is empty,
    /// labels collide, a reserved port lacks a static value, or a static value
    /// is zero.
    pub fn validate(&self) -> Result<()> {
        let mut labels = std::collections::HashSet::new();
        for port in self.reserved_ports.iter().chain(self.dynamic_ports.iter()) {
            if port.label.is_empty() {
                return Err(crate::error::Error::Config("port label cannot be empty".to_owned()));
            }
            if !labels.insert(&port.label) {
                return Err(crate::error::Error::Config(format!("duplicate port label '{}'", port.label)));
            }
        }
        for port in &self.reserved_ports {
            if port.static_value.is_none() || port.static_value == Some(0) {
                return Err(crate::error::Error::Config(format!(
                    "reserved port '{}' requires a non-zero static value",
                    port.label
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn net() -> NetworkResource {
        NetworkResource {
            mode: NetworkMode::Bridge,
            reserved_ports: vec![Port { label: "http".to_owned(), static_value: Some(8080), to: Some(80) }],
            dynamic_ports: vec![Port { label: "metrics".to_owned(), static_value: None, to: Some(9000) }],
        }
    }

    #[test]
    fn valid_network_passes() {
        assert!(net().validate().is_ok());
    }

    #[test]
    fn rejects_empty_label() {
        let mut n = net();
        n.reserved_ports[0].label = String::new();
        assert!(n.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_labels() {
        let mut n = net();
        n.dynamic_ports[0].label = "http".to_owned();
        assert!(n.validate().is_err());
    }

    #[test]
    fn rejects_reserved_without_static_value() {
        let mut n = net();
        n.reserved_ports[0].static_value = None;
        assert!(n.validate().is_err());
    }

    #[test]
    fn cni_mode_carries_name() {
        assert_eq!(NetworkMode::Cni("weave".to_owned()), NetworkMode::Cni("weave".to_owned()));
    }
}
