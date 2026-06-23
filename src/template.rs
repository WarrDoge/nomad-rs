// SPDX-License-Identifier: Apache-2.0

//! Templates rendered into a task's filesystem.
//!
//! A template renders dynamic content (from Consul/Vault/Nomad variables) to a
//! destination file and reacts to changes. Mirrors the subset of upstream
//! Nomad's `structs.Template`. Behaviour is specified by the tests and is
//! unimplemented.

use crate::error::Result;

/// What to do when a rendered template changes on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeMode {
    /// Do nothing.
    Noop,
    /// Restart the task.
    Restart,
    /// Send a signal to the task.
    Signal,
}

/// A template rendered into the task.
#[derive(Debug, Clone)]
pub struct Template {
    /// Path to a template file in the task dir; mutually exclusive with `embedded`.
    pub source: Option<String>,
    /// Inline template body; mutually exclusive with `source`.
    pub embedded: Option<String>,
    /// Destination path the rendered output is written to.
    pub destination: String,
    /// Reaction when the rendered content changes.
    pub change_mode: ChangeMode,
    /// Signal to send when `change_mode` is [`ChangeMode::Signal`].
    pub change_signal: Option<String>,
}

impl Template {
    /// Validate the template.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `destination` is empty, if
    /// neither or both of `source`/`embedded` are set, or if `change_mode` is
    /// [`ChangeMode::Signal`] without a `change_signal`.
    pub fn validate(&self) -> Result<()> {
        todo!("require a destination, exactly one source, and a signal for Signal mode")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn template() -> Template {
        Template {
            source: None,
            embedded: Some("{{ key \"x\" }}".to_owned()),
            destination: "local/config".to_owned(),
            change_mode: ChangeMode::Restart,
            change_signal: None,
        }
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn valid_template_passes() {
        assert!(template().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_empty_destination() {
        let mut t = template();
        t.destination = String::new();
        assert!(t.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_both_sources() {
        let mut t = template();
        t.source = Some("local/in.tpl".to_owned());
        assert!(t.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_neither_source() {
        let mut t = template();
        t.embedded = None;
        assert!(t.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn signal_mode_requires_signal() {
        let mut t = template();
        t.change_mode = ChangeMode::Signal;
        t.change_signal = None;
        assert!(t.validate().is_err());
    }
}
