// SPDX-License-Identifier: Apache-2.0

//! Parameterized jobs and dispatch.
//!
//! A parameterized job is a template dispatched with metadata and an optional
//! payload. Mirrors the subset of upstream Nomad's `structs.ParameterizedJobConfig`
//! plus the dispatch request. Behaviour is specified by the tests and is
//! unimplemented.

use std::collections::HashMap;

use crate::error::Result;

/// Whether a dispatch payload is allowed/required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadMode {
    /// Payload may be supplied.
    Optional,
    /// Payload must be supplied.
    Required,
    /// Payload must not be supplied.
    Forbidden,
}

/// The parameterized-job template configuration.
#[derive(Debug, Clone)]
pub struct ParameterizedJob {
    /// Metadata keys that every dispatch must provide.
    pub meta_required: Vec<String>,
    /// Metadata keys a dispatch may optionally provide.
    pub meta_optional: Vec<String>,
    /// Payload requirement.
    pub payload: PayloadMode,
}

/// A single dispatch invocation.
#[derive(Debug, Clone)]
pub struct DispatchRequest {
    /// Supplied metadata.
    pub meta: HashMap<String, String>,
    /// Supplied payload bytes, if any.
    pub payload: Option<Vec<u8>>,
}

impl ParameterizedJob {
    /// Validate `request` against this template.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if a required meta key is
    /// missing, an unknown meta key is supplied, or the payload presence
    /// violates [`PayloadMode`].
    pub fn validate_dispatch(&self, request: &DispatchRequest) -> Result<()> {
        todo!("check required/allowed meta keys and payload mode for {} keys", request.meta.len())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn template() -> ParameterizedJob {
        ParameterizedJob {
            meta_required: vec!["region".to_owned()],
            meta_optional: vec!["note".to_owned()],
            payload: PayloadMode::Forbidden,
        }
    }

    fn request() -> DispatchRequest {
        DispatchRequest { meta: HashMap::from([("region".to_owned(), "eu".to_owned())]), payload: None }
    }

    #[test]
    fn valid_dispatch_passes() {
        assert!(template().validate_dispatch(&request()).is_ok());
    }

    #[test]
    fn missing_required_meta_errors() {
        let mut r = request();
        r.meta.clear();
        assert!(template().validate_dispatch(&r).is_err());
    }

    #[test]
    fn forbidden_payload_errors() {
        let mut r = request();
        r.payload = Some(vec![1, 2, 3]);
        assert!(template().validate_dispatch(&r).is_err());
    }
}
