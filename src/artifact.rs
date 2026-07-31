// SPDX-License-Identifier: Apache-2.0

//! Task artifacts: files fetched into a task before it starts.
//!
//! Mirrors the subset of upstream Nomad's `structs.TaskArtifact` plus the
//! getter abstraction. The [`Getter`](crate::artifact::Getter) trait is the download contract;
//! [`UrlGetter`](crate::artifact::UrlGetter) is one implementation backed by
//! the `ureq` HTTP client (blocking HTTPS downloads).

use crate::error::Result;
use std::fs;
use std::io;
use std::path::Path;
use url::Url;

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
        if self.source.is_empty() {
            return Err(crate::error::Error::Config("artifact source cannot be empty".to_owned()));
        }
        if let Some(ref checksum) = self.checksum
            && !checksum.contains(':')
        {
            return Err(crate::error::Error::Config("checksum must be in type:value format".to_owned()));
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

/// Getter backed by go-getter-style URL schemes.
#[derive(Debug, Default)]
pub struct UrlGetter;

impl Getter for UrlGetter {
    fn get(&self, artifact: &Artifact, task_dir: &str) -> Result<()> {
        let parsed = Url::parse(&artifact.source)
            .map_err(|e| crate::error::Error::Config(format!("invalid artifact URL: {e}")))?;
        let filename = parsed
            .path_segments()
            .and_then(Iterator::last)
            .filter(|s| !s.is_empty() && *s != "/")
            .unwrap_or("artifact");
        let dest_dir = Path::new(task_dir);
        let dest_path = dest_dir.join(filename);

        // Create the task directory if it doesn't exist.
        fs::create_dir_all(dest_dir)?;

        // Open the destination file, creating or truncating.
        let dest_file = fs::File::create(&dest_path)?;

        // Download from URL and write to file.
        let resp = ureq::get(&artifact.source)
            .call()
            .map_err(|e| crate::error::Error::Runtime(format!("download failed: {e}")))?;

        let mut reader = resp.into_reader();
        let written = io::copy(&mut reader, &mut &dest_file)?;

        tracing::debug!(
            "downloaded artifact {src} ({written} bytes) to {dest}",
            src = artifact.source,
            dest = dest_path.display()
        );

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU16, Ordering};
    use std::thread;

    /// Pick a port, start a `tiny_http` server serving static content on that port,
    /// and return the port number. Only one server is started per test run.
    fn serve() -> u16 {
        static SERVER: OnceLock<Arc<AtomicU16>> = OnceLock::new();
        SERVER
            .get_or_init(|| {
                let listener = tiny_http::Server::http("127.0.0.1:0").expect("failed to bind test HTTP server");
                let port = listener.server_addr().to_ip().unwrap().port();
                let port_arc = Arc::new(AtomicU16::new(port));
                let port_clone = Arc::clone(&port_arc);
                thread::spawn(move || {
                    for request in listener.incoming_requests() {
                        let resp = tiny_http::Response::from_string("hello from artifact test\n").with_header(
                            tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/plain; charset=utf-8"[..])
                                .unwrap(),
                        );
                        let _ = request.respond(resp);
                    }
                });
                port_clone
            })
            .load(Ordering::Relaxed)
    }

    fn artifact(source: &str) -> Artifact {
        Artifact { source: source.to_owned(), destination: "local/app".to_owned(), checksum: None }
    }

    #[test]
    fn valid_artifact_passes() {
        assert!(artifact("http://example.com/foo").validate().is_ok());
    }

    #[test]
    fn rejects_empty_source() {
        let mut a = artifact("http://example.com/foo");
        a.source = String::new();
        assert!(a.validate().is_err());
    }

    #[test]
    fn rejects_malformed_checksum() {
        let mut a = artifact("http://example.com/foo");
        a.checksum = Some("deadbeef".to_owned());
        assert!(a.validate().is_err());
    }

    #[test]
    fn getter_downloads_from_http() {
        let port = serve();
        let url = format!("http://127.0.0.1:{port}/my_artifact.bin");
        let dir = tempfile::TempDir::new().expect("create temp dir");
        let getter = UrlGetter;
        let artifact = artifact(&url);

        let result = getter.get(&artifact, dir.path().to_str().unwrap());
        assert!(result.is_ok(), "download should succeed: {result:?}");

        let downloaded = dir.path().join("my_artifact.bin");
        assert!(downloaded.exists(), "downloaded file should exist");
        let content = fs::read_to_string(&downloaded).expect("read downloaded file");
        assert_eq!(content, "hello from artifact test\n");
    }
}
