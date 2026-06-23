// SPDX-License-Identifier: Apache-2.0

//! Task artifacts: files fetched into a task before it starts.
//!
//! Mirrors the subset of upstream Nomad's `structs.TaskArtifact` plus the
//! getter abstraction. The [`Getter`](crate::artifact::Getter) trait is the download contract;
//! [`UrlGetter`](crate::artifact::UrlGetter) is one implementation whose behaviour is specified by the
//! tests and is unimplemented.

use crate::error::Result;

/// A file (or archive) to fetch into the task directory.
#[derive(Debug, Clone)]
pub struct Artifact {
    /// Source URL (`http(s)://`, `s3::`, `git::`, `gcs::`, ...).
    pub source: String,
    /// Relative destination within the task's `local/` dir.
    pub destination: String,
    /// Optional `type:hash` checksum to verify after download.
    pub checksum: Option<String>,
}

impl Artifact {
    /// Validate the artifact spec.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `source` is empty or
    /// `checksum` is present but not in `type:value` form.
    pub fn validate(&self) -> Result<()> {
        todo!("require a source and a well-formed type:value checksum if present")
    }
}

/// Fetches an [`Artifact`] into a destination directory.
pub trait Getter {
    /// Download `artifact` rooted at `task_dir`.
    ///
    /// # Errors
    ///
    /// Returns an error if the fetch fails or the checksum does not match.
    fn get(&self, artifact: &Artifact, task_dir: &str) -> Result<()>;
}

/// Getter backed by go-getter-style URL schemes.
#[derive(Debug, Default)]
pub struct UrlGetter;

impl Getter for UrlGetter {
    fn get(&self, artifact: &Artifact, task_dir: &str) -> Result<()> {
        todo!("fetch {:?} into {task_dir:?} and verify its checksum", artifact.source)
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn artifact() -> Artifact {
        Artifact {
            source: "https://example.com/app.tar.gz".to_owned(),
            destination: "local/app".to_owned(),
            checksum: Some("sha256:abc123".to_owned()),
        }
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn valid_artifact_passes() {
        assert!(artifact().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_empty_source() {
        let mut a = artifact();
        a.source = String::new();
        assert!(a.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_malformed_checksum() {
        let mut a = artifact();
        a.checksum = Some("deadbeef".to_owned());
        assert!(a.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn getter_fetches_artifact() {
        assert!(UrlGetter.get(&artifact(), "/tmp/alloc/task").is_ok());
    }
}
