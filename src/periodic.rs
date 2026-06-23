// SPDX-License-Identifier: Apache-2.0

//! Periodic (cron) job configuration.
//!
//! A periodic job launches children on a cron schedule. Mirrors the subset of
//! upstream Nomad's `structs.PeriodicConfig`. Behaviour is specified by the
//! tests and is unimplemented.

use crate::error::Result;

/// A periodic launch schedule.
#[derive(Debug, Clone)]
pub struct PeriodicConfig {
    /// Cron spec, e.g. `"*/5 * * * *"`.
    pub spec: String,
    /// IANA time zone the spec is evaluated in, e.g. `"UTC"`.
    pub time_zone: String,
    /// Skip a launch if the previous child is still running.
    pub prohibit_overlap: bool,
}

impl PeriodicConfig {
    /// Validate the schedule.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if the cron `spec` does not parse
    /// or `time_zone` is unknown.
    pub fn validate(&self) -> Result<()> {
        use cron::Schedule;
        use std::str::FromStr;
        Schedule::from_str(&self.spec)
            .map_err(|e| crate::error::Error::Config(format!("invalid cron spec '{}': {e}", self.spec)))?;
        if self.time_zone.is_empty() {
            return Err(crate::error::Error::Config("periodic time_zone cannot be empty".to_owned()));
        }
        // Accept "UTC" (case-insensitive) or IANA time zone names (contain '/').
        if !self.time_zone.eq_ignore_ascii_case("UTC") && !self.time_zone.contains('/') {
            return Err(crate::error::Error::Config(format!("unrecognised time zone '{}'", self.time_zone)));
        }
        Ok(())
    }

    /// Next launch time (Unix seconds) strictly after `after_unix`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::Error::Config`] if the spec cannot be evaluated.
    pub fn next(&self, after_unix: i64) -> Result<i64> {
        use cron::Schedule;
        use std::str::FromStr;
        let schedule = Schedule::from_str(&self.spec)
            .map_err(|e| crate::error::Error::Config(format!("invalid cron spec: {e}")))?;
        let after = chrono::DateTime::from_timestamp(after_unix, 0)
            .ok_or_else(|| crate::error::Error::Runtime("invalid unix timestamp".to_owned()))?;
        let next = schedule
            .upcoming(chrono::Utc)
            .find(|t| *t > after)
            .ok_or_else(|| crate::error::Error::Runtime("no upcoming firing time".to_owned()))?;
        Ok(next.timestamp())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn periodic() -> PeriodicConfig {
        PeriodicConfig { spec: "0 */5 * * * *".to_owned(), time_zone: "UTC".to_owned(), prohibit_overlap: true }
    }

    #[test]
    fn valid_spec_passes() {
        assert!(periodic().validate().is_ok());
    }

    #[test]
    fn rejects_bad_spec() {
        let mut p = periodic();
        p.spec = "not a cron".to_owned();
        assert!(p.validate().is_err());
    }

    #[test]
    fn next_is_after_reference() {
        let now = 1_000_000_000;
        assert!(periodic().next(now).unwrap() > now);
    }
}
