// SPDX-License-Identifier: Apache-2.0

//! Telemetry sink.
//!
//! Defines the metric shape and the in-tree [`InMemorySink`](crate::metrics::InMemorySink)
//! that records samples. A real backend (Prometheus, statsd, ...) replaces its
//! body later. Behaviour is specified by the tests and is unimplemented.

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

/// A sink that retains samples in memory (for tests and the `/metrics` view).
#[derive(Debug, Default)]
pub struct InMemorySink {
    /// Retained metric samples.
    samples: std::sync::Mutex<Vec<Metric>>,
}

impl InMemorySink {
    /// Emit one metric sample.
    ///
    /// # Errors
    ///
    /// Returns an error if the sample cannot be recorded/forwarded.
    pub fn emit(&self, metric: &Metric) -> Result<()> {
        match self.samples.lock() {
            Ok(mut guard) => guard.push(metric.clone()),
            Err(_) => return Err(crate::error::Error::Runtime("metrics mutex poisoned".to_owned())),
        }
        Ok(())
    }

    /// Return all recorded samples.
    #[must_use]
    pub fn drain(&self) -> Vec<Metric> {
        self.samples.lock().map_or_else(|_| vec![], |mut guard| guard.drain(..).collect())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    #[test]
    fn emits_a_counter() {
        let m = Metric { key: "nomad.evals".to_owned(), value: 1.0, kind: MetricKind::Counter };
        assert!(InMemorySink::default().emit(&m).is_ok());
    }
}
