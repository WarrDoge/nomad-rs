// SPDX-License-Identifier: Apache-2.0

//! Telemetry sink contract — dependency-agnostic.
//!
//! Defines the metric shape and the sink that emits it. The concrete backend
//! (Prometheus, statsd, ...) lives behind [`MetricSink`]. [`InMemorySink`] is
//! the in-tree sink whose behaviour is specified by the tests and is
//! unimplemented.

use crate::error::Result;

/// The kind of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    /// Monotonic counter.
    Counter,
    /// Point-in-time value.
    Gauge,
    /// Duration sample.
    Timer,
}

/// A single metric sample.
#[derive(Debug, Clone)]
pub struct Metric {
    /// Dotted metric key, e.g. `"nomad.scheduler.evals"`.
    pub key: String,
    /// Sample value.
    pub value: f64,
    /// Metric kind.
    pub kind: MetricKind,
}

/// Receives metric samples.
pub trait MetricSink {
    /// Emit one metric sample.
    ///
    /// # Errors
    ///
    /// Returns an error if the sample cannot be recorded/forwarded.
    fn emit(&self, metric: &Metric) -> Result<()>;
}

/// A sink that retains samples in memory (for tests and the `/metrics` view).
#[derive(Debug, Default)]
pub struct InMemorySink;

impl MetricSink for InMemorySink {
    fn emit(&self, metric: &Metric) -> Result<()> {
        todo!("record sample {:?} in the in-memory ring", metric.key)
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn emits_a_counter() {
        let m = Metric { key: "nomad.evals".to_owned(), value: 1.0, kind: MetricKind::Counter };
        assert!(InMemorySink.emit(&m).is_ok());
    }
}
