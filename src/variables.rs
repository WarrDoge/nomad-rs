// SPDX-License-Identifier: Apache-2.0

//! Secure variables and the encryption keyring.
//!
//! Variables are namespaced key/value secrets encrypted at rest by the
//! keyring. Mirrors the subset of upstream Nomad's variables + keyring.
//! Behaviour is specified by the tests and is unimplemented.

use std::collections::HashMap;

use crate::error::Result;

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
        todo!("require namespace, path, and at least one item")
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
        todo!("seal {} bytes with the active key", plaintext.len())
    }

    /// Decrypt `ciphertext`, selecting the key by its embedded id.
    ///
    /// # Errors
    ///
    /// Returns an error if the key id is unknown or authentication fails.
    pub fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        todo!("unseal {} bytes using the embedded key id", ciphertext.len())
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
    #[ignore = "red spec: implement to unignore"]
    fn valid_variable_passes() {
        assert!(variable().validate().is_ok());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_empty_path() {
        let mut v = variable();
        v.path = String::new();
        assert!(v.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn rejects_no_items() {
        let mut v = variable();
        v.items.clear();
        assert!(v.validate().is_err());
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn encrypt_then_decrypt_round_trips() {
        let keyring = Keyring;
        let sealed = keyring.encrypt(b"hello").unwrap();
        assert_eq!(keyring.decrypt(&sealed).unwrap(), b"hello");
    }
}
