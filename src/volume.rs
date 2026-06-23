// SPDX-License-Identifier: Apache-2.0

//! Volume requests and mounts.
//!
//! A task group requests volumes (host or CSI); tasks mount them at a path.
//! Mirrors the subset of upstream Nomad's `structs.VolumeRequest`/
//! `VolumeMount`. Behaviour is specified by the tests and is unimplemented.

use crate::error::Result;

/// The backing kind of a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeType {
    /// A host path made available by the node.
    Host,
    /// A CSI-provisioned volume.
    Csi,
}

/// A task group's request for a volume.
#[derive(Debug, Clone)]
pub struct VolumeRequest {
    /// Group-local name, referenced by mounts.
    pub name: String,
    /// Volume kind.
    pub volume_type: VolumeType,
    /// Source: host volume name or CSI volume id.
    pub source: String,
    /// Whether the request is read-only.
    pub read_only: bool,
}

impl VolumeRequest {
    /// Validate the volume request.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `name` or `source` is empty.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(crate::error::Error::Config("volume request name cannot be empty".to_owned()));
        }
        if self.source.is_empty() {
            return Err(crate::error::Error::Config("volume request source cannot be empty".to_owned()));
        }
        Ok(())
    }
}

/// A task's mount of a requested volume.
#[derive(Debug, Clone)]
pub struct VolumeMount {
    /// Name of the [`VolumeRequest`] to mount.
    pub volume: String,
    /// Absolute destination path inside the task.
    pub destination: String,
    /// Whether the mount is read-only.
    pub read_only: bool,
}

impl VolumeMount {
    /// Validate the mount.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `volume` is empty or
    /// `destination` is not an absolute path.
    pub fn validate(&self) -> Result<()> {
        if self.volume.is_empty() {
            return Err(crate::error::Error::Config("volume mount volume name cannot be empty".to_owned()));
        }
        if !self.destination.starts_with('/') {
            return Err(crate::error::Error::Config(format!(
                "volume mount destination '{}' is not an absolute path",
                self.destination
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn request() -> VolumeRequest {
        VolumeRequest {
            name: "data".to_owned(),
            volume_type: VolumeType::Host,
            source: "host-data".to_owned(),
            read_only: false,
        }
    }

    fn mount() -> VolumeMount {
        VolumeMount { volume: "data".to_owned(), destination: "/var/data".to_owned(), read_only: false }
    }

    #[test]
    fn valid_request_passes() {
        assert!(request().validate().is_ok());
    }

    #[test]
    fn request_rejects_empty_source() {
        let mut r = request();
        r.source = String::new();
        assert!(r.validate().is_err());
    }

    #[test]
    fn valid_mount_passes() {
        assert!(mount().validate().is_ok());
    }

    #[test]
    fn mount_rejects_relative_destination() {
        let mut m = mount();
        m.destination = "var/data".to_owned();
        assert!(m.validate().is_err());
    }

    #[test]
    fn mount_rejects_empty_volume() {
        let mut m = mount();
        m.volume = String::new();
        assert!(m.validate().is_err());
    }
}
