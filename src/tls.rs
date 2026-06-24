// SPDX-License-Identifier: Apache-2.0

//! mTLS configuration for Nomad RPC and HTTP transports.
//!
//! Provides [`TlsConfig`] for loading and validating X.509 certificates,
//! private keys, and CA roots.  The module exposes helper functions that
//! build [`rustls::ServerConfig`] / [`rustls::ClientConfig`] instances
//! from a [`TlsConfig`]; these can be plugged into a Tokio TCP listener
//! or connector (e.g. via `tokio-rustls`).

use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};

/// TLS / mTLS configuration for a Nomad agent.
///
/// All paths are filesystem paths that must exist at the time
/// [`TlsConfig::validate`] is called.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the PEM-encoded X.509 certificate chain.
    pub cert_path: String,
    /// Path to the PEM-encoded private key.
    pub key_path: String,
    /// Path to the PEM-encoded CA certificate bundle.
    pub ca_path: String,
}

impl TlsConfig {
    /// Create a new [`TlsConfig`] with the given certificate, key, and CA paths.
    #[must_use]
    pub fn new(cert_path: String, key_path: String, ca_path: String) -> Self {
        Self { cert_path, key_path, ca_path }
    }

    /// Validate that all certificate files exist and are readable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if any file cannot be opened, or
    /// [`Error::Validation`] if a path is empty.
    pub fn validate(&self) -> Result<()> {
        validate_path(&self.cert_path, "cert_path")?;
        validate_path(&self.key_path, "key_path")?;
        validate_path(&self.ca_path, "ca_path")?;
        Ok(())
    }

    /// Build a [`rustls::ServerConfig`] for mutual TLS.
    ///
    /// The server presents its own certificate and verifies client
    /// certificates against the configured CA.
    ///
    /// # Errors
    ///
    /// Returns an IO or parse error if the certificate or key files cannot
    /// be read or are malformed.
    pub fn server_config(&self) -> Result<rustls::ServerConfig> {
        self.validate()?;

        let certs = load_certs(&self.cert_path)?;
        let key = load_private_key(&self.key_path)?;
        let root_store = load_ca(&self.ca_path)?;

        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| Error::Runtime(format!("failed to build client verifier: {e}")))?;

        let mut config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| Error::Runtime(format!("failed to set server certificate: {e}")))?;

        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(config)
    }

    /// Build a [`rustls::ClientConfig`] for mutual TLS.
    ///
    /// The client presents its own certificate and verifies the server
    /// against the configured CA.
    ///
    /// # Errors
    ///
    /// Returns an IO or parse error if the certificate or key files cannot
    /// be read or are malformed.
    pub fn client_config(&self) -> Result<rustls::ClientConfig> {
        self.validate()?;

        let certs = load_certs(&self.cert_path)?;
        let key = load_private_key(&self.key_path)?;
        let root_store = load_ca(&self.ca_path)?;

        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_client_auth_cert(certs, key)
            .map_err(|e| Error::Runtime(format!("failed to set client auth: {e}")))?;

        Ok(config)
    }
}

// ---- helpers ----------------------------------------------------------------

/// Ensure `path` is non-empty and the file exists.
fn validate_path(path: &str, label: &str) -> Result<()> {
    if path.trim().is_empty() {
        return Err(Error::Validation(format!("{label} must not be empty")));
    }
    if !Path::new(path).try_exists().map_err(Error::Io)? {
        return Err(Error::Validation(format!("{label} file not found: {path}")));
    }
    Ok(())
}

/// Load PEM-encoded certificate chain from `path`.
fn load_certs(path: &str) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let data = std::fs::read(path).map_err(Error::Io)?;
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(&mut &data[..])
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Runtime(format!("failed to parse certificates: {e}")))?;

    if certs.is_empty() {
        return Err(Error::Validation(format!("no certificates found in {path}")));
    }
    Ok(certs)
}

/// Load PEM-encoded private key from `path`.
fn load_private_key(path: &str) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let data = std::fs::read(path).map_err(Error::Io)?;

    match rustls_pemfile::private_key(&mut &data[..]) {
        Ok(Some(key)) => Ok(key),
        Ok(None) => Err(Error::Validation(format!("no private key found in {path}"))),
        Err(e) => Err(Error::Runtime(format!("failed to parse private key: {e}"))),
    }
}

/// Load PEM-encoded CA certificates into a root store.
fn load_ca(path: &str) -> Result<rustls::RootCertStore> {
    let data = std::fs::read(path).map_err(Error::Io)?;
    let mut root_store = rustls::RootCertStore::empty();

    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = rustls_pemfile::certs(&mut &data[..])
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Runtime(format!("failed to parse CA certificates: {e}")))?;

    if certs.is_empty() {
        return Err(Error::Validation(format!("no CA certificates found in {path}")));
    }

    root_store.add_parsable_certificates(certs);
    Ok(root_store)
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_validate_empty_paths() {
        let cfg = TlsConfig::new(String::new(), "key.pem".to_owned(), "ca.pem".to_owned());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_tls_config_new_and_validate() {
        let cfg = TlsConfig::new("cert.pem".to_owned(), "key.pem".to_owned(), "ca.pem".to_owned());
        assert_eq!(cfg.cert_path, "cert.pem");
        assert_eq!(cfg.key_path, "key.pem");
        assert_eq!(cfg.ca_path, "ca.pem");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_validate_path_empty() {
        assert!(validate_path("", "test").is_err());
    }

    #[test]
    fn test_validate_path_not_found() {
        assert!(validate_path("/nonexistent/cert.pem", "test").is_err());
    }
}
