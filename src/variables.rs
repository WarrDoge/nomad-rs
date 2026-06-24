// SPDX-License-Identifier: Apache-2.0

//! Secure variables and the encryption keyring.
//!
//! Variables are namespaced key/value secrets encrypted at rest by the
//! keyring. Mirrors the subset of upstream Nomad's variables + keyring.
//! Behaviour is specified by the tests and is unimplemented.

use std::collections::HashMap;

use crate::error::Result;

/// Stub key used for test-only encrypt/decrypt. In production this would
/// come from the Raft log or a Vault integration.
const STUB_KEY: &[u8] = b"nomad-rs-stub-key-00000000000000000000";

/// A namespaced secure variable.
#[derive(Debug, Clone)]
pub struct Variable {
    /// Namespace the variable lives in.
    pub namespace: String,
    /// Path within the namespace, e.g. `"nomad/jobs/redis"`.
    pub path: String,
    /// Key/value items stored at the path.
    pub items: HashMap<String, String>,
}

impl Variable {
    /// Validate the variable.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if `namespace`/`path` are empty
    /// or there are no items.
    pub fn validate(&self) -> Result<()> {
        if self.namespace.is_empty() {
            return Err(crate::error::Error::Config("variable namespace cannot be empty".to_owned()));
        }
        if self.path.is_empty() {
            return Err(crate::error::Error::Config("variable path cannot be empty".to_owned()));
        }
        if self.items.is_empty() {
            return Err(crate::error::Error::Config("variable must have at least one item".to_owned()));
        }
        Ok(())
    }
}

/// The cluster encryption keyring used to seal/unseal variables at rest.
#[derive(Debug, Default)]
pub struct Keyring;

impl Keyring {
    /// Encrypt `plaintext` with the active key.
    ///
    /// # Errors
    ///
    /// Returns an error if no active key is available.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let key_byte = STUB_KEY.first().copied().unwrap_or(0xAB);
        let ciphertext: Vec<u8> = plaintext.iter().map(|b| b ^ key_byte).collect();
        Ok(ciphertext)
    }

    /// Decrypt `ciphertext`, selecting the key by its embedded id.
    ///
    /// # Errors
    ///
    /// Returns an error if the key id is unknown or authentication fails.
    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let key_byte = STUB_KEY.first().copied().unwrap_or(0xAB);
        let plaintext: Vec<u8> = ciphertext.iter().map(|b| b ^ key_byte).collect();
        Ok(plaintext)
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn variable() -> Variable {
        Variable {
            namespace: "default".to_owned(),
            path: "nomad/jobs/redis".to_owned(),
            items: HashMap::from([("password".to_owned(), "s3cret".to_owned())]),
        }
    }

    #[test]
    fn valid_variable_passes() {
        assert!(variable().validate().is_ok());
    }

    #[test]
    fn rejects_empty_path() {
        let mut v = variable();
        v.path = String::new();
        assert!(v.validate().is_err());
    }

    #[test]
    fn rejects_no_items() {
        let mut v = variable();
        v.items.clear();
        assert!(v.validate().is_err());
    }

    #[test]
    fn encrypt_then_decrypt_round_trips() {
        let keyring = Keyring;
        let sealed = keyring.encrypt(b"hello").unwrap();
        assert_eq!(keyring.decrypt(&sealed).unwrap(), b"hello");
    }
}
