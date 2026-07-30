// SPDX-License-Identifier: Apache-2.0

//! Task artifacts: files fetched into a task before it starts.
//!
//! Mirrors the subset of upstream Nomad's `structs.TaskArtifact` plus the
//! getter abstraction. The [`Getter`] trait is the download contract;
//! [`HttpGetter`] is one implementation that downloads via HTTPS.

use std::path::Path;

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

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
    /// Returns [`Error::Config`] if `source` is empty or
    /// `checksum` is present but not in `type:value` form.
    pub fn validate(&self) -> Result<()> {
        if self.source.is_empty() {
            return Err(Error::Config("artifact source cannot be empty".to_owned()));
        }
        if let Some(ref checksum) = self.checksum
            && !checksum.contains(':')
        {
            return Err(Error::Config("checksum must be in type:value format".to_owned()));
        }
        Ok(())
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

/// Getter backed by HTTP(S) downloads.
///
/// Supports checksum verification (SHA-256) after download.
#[derive(Debug, Default)]
pub struct HttpGetter;

impl HttpGetter {
    /// Parse a `type:hex` checksum string and verify the given bytes.
    ///
    /// Currently only `sha256` is supported.
    fn verify_checksum(data: &[u8], checksum: &str) -> Result<()> {
        let Some((algo, expected)) = checksum.split_once(':') else {
            return Err(Error::Config(format!("invalid checksum format: {checksum}")));
        };

        let actual_hex = match algo {
            "sha256" => {
                let mut hasher = Sha256::new();
                hasher.update(data);
                hex::encode(hasher.finalize())
            }
            other => return Err(Error::Config(format!("unsupported checksum algorithm: {other}"))),
        };

        if actual_hex == expected {
            Ok(())
        } else {
            Err(Error::ChecksumMismatch {
                expected: expected.to_owned(),
                actual: actual_hex,
            })
        }
    }
}

impl Getter for HttpGetter {
    fn get(&self, artifact: &Artifact, task_dir: &str) -> Result<()> {
        artifact.validate()?;

        if !artifact.source.starts_with("http://") && !artifact.source.starts_with("https://") {
            return Err(Error::Download(format!(
                "unsupported scheme for HttpGetter: {}",
                artifact.source
            )));
        }

        let client = reqwest::blocking::Client::builder()
            .user_agent("nomad-rs-artifact/0.1.0")
            .build()
            .map_err(|e| Error::Download(format!("failed to create HTTP client: {e}")))?;

        let response = client
            .get(&artifact.source)
            .send()
            .map_err(|e| Error::Download(format!("failed to download {}: {e}", artifact.source)))?;

        let status = response.status();
        if !status.is_success() {
            return Err(Error::Download(format!(
                "download of {} returned HTTP {status}",
                artifact.source
            )));
        }

        let body = response
            .bytes()
            .map_err(|e| Error::Download(format!("failed to read response body: {e}")))?;

        // Verify checksum if provided
        if let Some(ref checksum) = artifact.checksum {
            Self::verify_checksum(&body, checksum)?;
        }

        // Write to destination
        let dest = Path::new(task_dir).join(&artifact.destination);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Io(std::io::Error::new(e.kind(), format!("{e}: {}", parent.display()))))?;
        }

        std::fs::write(&dest, &body)?;

        Ok(())
    }
}

/// Getter backed by go-getter-style URL schemes (stub/shim for now).
#[derive(Debug, Default)]
pub struct UrlGetter;

impl Getter for UrlGetter {
    fn get(&self, artifact: &Artifact, task_dir: &str) -> Result<()> {
        // Delegate HTTP(S) artifacts to HttpGetter for real downloads.
        if artifact.source.starts_with("http://") || artifact.source.starts_with("https://") {
            return HttpGetter.get(artifact, task_dir);
        }
        // ponytail: no-op stub for non-HTTP schemes; real S3/Git download
        //           added when those artifact sources are wired up.
        let _ = (artifact, task_dir);
        Ok(())
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
    fn valid_artifact_passes() {
        assert!(artifact().validate().is_ok());
    }

    #[test]
    fn rejects_empty_source() {
        let mut a = artifact();
        a.source = String::new();
        assert!(a.validate().is_err());
    }

    #[test]
    fn rejects_malformed_checksum() {
        let mut a = artifact();
        a.checksum = Some("deadbeef".to_owned());
        assert!(a.validate().is_err());
    }

    #[test]
    fn getter_fetches_artifact() {
        // The existing test: UrlGetter on http URL delegates to HttpGetter
        // which will fail because example.com/app.tar.gz doesn't exist.
        // Validate that we get a proper error rather than Ok(()).
        let result = UrlGetter.get(&artifact(), "/tmp/alloc/task");
        assert!(result.is_err(), "expected download error for example.com URL");
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("download error")
                || msg.contains("failed to download")
                || msg.contains("HTTP"),
            "expected download-related error, got: {msg}"
        );
    }

    #[test]
    fn http_getter_rejects_non_http_scheme() {
        let artifact = Artifact {
            source: "s3://bucket/key".to_owned(),
            destination: "local/data".to_owned(),
            checksum: None,
        };
        let result = HttpGetter.get(&artifact, "/tmp/alloc");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unsupported scheme"));
    }

    #[test]
    fn http_getter_rejects_empty_source() {
        let artifact = Artifact {
            source: String::new(),
            destination: "local/data".to_owned(),
            checksum: None,
        };
        let result = HttpGetter.get(&artifact, "/tmp/alloc");
        assert!(result.is_err());
    }

    #[test]
    fn verify_checksum_matches() {
        let data = b"hello world";
        let expected_hex = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        let checksum = format!("sha256:{expected_hex}");
        assert!(HttpGetter::verify_checksum(data, &checksum).is_ok());
    }

    #[test]
    fn verify_checksum_mismatch() {
        let data = b"hello world";
        let checksum = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        let result = HttpGetter::verify_checksum(data, checksum);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch { .. }));
    }

    #[test]
    fn verify_checksum_unsupported_algo() {
        let data = b"hello";
        let checksum = "md5:d41d8cd98f00b204e9800998ecf8427e";
        let result = HttpGetter::verify_checksum(data, checksum);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unsupported checksum algorithm"));
    }

    #[test]
    fn verify_checksum_no_algo() {
        let data = b"hello";
        let checksum = "justahash";
        let result = HttpGetter::verify_checksum(data, checksum);
        assert!(result.is_err());
    }
}
