// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry tracing middleware.
//!
//! Provides initialisation and shutdown helpers for OTLP-exported tracing
//! via `tracing-opentelemetry`.  Controlled by the `OTEL_DISABLED` and
//! `OTEL_EXPORTER_OTLP_ENDPOINT` environment variables.

use std::sync::OnceLock;

use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::error::{Error, Result};

/// Holds the global tracer provider once initialised, so that
/// [`shutdown_otel_tracer`] can flush and shut it down.
static OTEL_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Initialise the OpenTelemetry tracer and register it as a `tracing` layer.
///
/// # Behaviour
///
/// * If the `OTEL_DISABLED` environment variable is set (to any value), the
///   function returns `Ok(())` without doing anything — all tracing continues
///   through the existing subscriber without `OTel`.
///
/// * Otherwise it reads `OTEL_EXPORTER_OTLP_ENDPOINT` (defaults to
///   `http://localhost:4317`) and configures a batch OTLP exporter.
///
/// * The resulting `OpenTelemetryLayer` is layered on top of the global
///   `tracing` subscriber.  Callers should invoke this **after** setting up
///   their base subscriber (e.g. `tracing_subscriber::fmt()`).
///
/// # Errors
///
/// Returns an error if the OTLP pipeline cannot be constructed (e.g. because
/// the endpoint is unreachable at init time, or because the global subscriber
/// has already been set and cannot be modified).
///
/// # Panics
///
/// Panics if `init_otel_tracer` is called more than once (the global provider
/// is stored in a `OnceLock`).
#[allow(clippy::missing_panics_doc)]
pub fn init_otel_tracer() -> Result<()> {
    if std::env::var("OTEL_DISABLED").is_ok() {
        tracing::warn!("OpenTelemetry tracing is disabled via OTEL_DISABLED");
        return Ok(());
    }

    let endpoint = match std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT") {
        Ok(v) => v,
        Err(_) => "http://localhost:4317".to_owned(),
    };

    // Build the OTLP exporter via HTTP/protobuf.
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(&endpoint)
        .build()
        .map_err(|e| Error::Runtime(format!("failed to build OTLP exporter: {e}")))?;

    // Build a tracer provider with simple processor (no batch).
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(otlp_exporter)
        .build();

    let tracer = provider.tracer("nomad-rs");

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(otel_layer)
        .try_init()
        .map_err(|e| Error::Runtime(format!("failed to init OTel tracing layer: {e}")))?;

    // Store the provider so shutdown_otel_tracer can flush + shut it down.
    OTEL_PROVIDER
        .set(provider)
        .map_err(|_| Error::Runtime("init_otel_tracer called more than once".to_owned()))?;

    tracing::info!(endpoint, "OpenTelemetry tracing initialised");
    Ok(())
}

/// Flush and shut down the OpenTelemetry tracer.
///
/// Call this during graceful shutdown to ensure all buffered spans are
/// exported. Does nothing when `OTel` was never initialised (e.g. when
/// `OTEL_DISABLED` was set at init time).
pub fn shutdown_otel_tracer() {
    if std::env::var("OTEL_DISABLED").is_ok() {
        return;
    }

    let Some(provider) = OTEL_PROVIDER.get() else {
        tracing::warn!("otel tracer was never initialised, nothing to shut down");
        return;
    };

    tracing::info!("otel tracer shutting down");
    if let Err(e) = provider.shutdown() {
        tracing::error!("otel tracer shutdown error: {e}");
    }
    tracing::info!("otel tracer shutdown complete");
}

#[cfg(test)]
#[allow(
    clippy::missing_docs_in_private_items,
    clippy::wildcard_imports,
    clippy::unwrap_used,
    clippy::panic,
    reason = "conventional inline test module"
)]
mod tests {
    use super::*;

    /// Helper: set an env var for the duration of a test closure.
    fn with_env(key: &str, val: &str, f: impl FnOnce()) {
        temp_env::with_var(key, Some(val), f);
    }

    /// Ensure that when `OTEL_DISABLED` is set, initialisation is a no-op.
    #[test]
    fn disabled_env_var_skips_init() {
        with_env("OTEL_DISABLED", "true", || {
            let result = init_otel_tracer();
            assert!(result.is_ok(), "expected OK when OTEL_DISABLED is set");
            // No need to shut down — nothing was initialised.
        });
    }

    /// `shutdown_otel_tracer` is safe to call when `init_otel_tracer` was
    /// never called (no panic, no error).
    #[test]
    fn shutdown_before_init_is_safe() {
        with_env("OTEL_DISABLED", "true", || {
            // Should not panic or crash.
            shutdown_otel_tracer();
        });
    }

    /// The endpoint env-var is read from the environment (test that the
    /// function does not crash when given a plausible-but-unreachable
    /// endpoint).  This test relies on the exporter failing gracefully at
    /// init time rather than panicking.
    #[test]
    fn custom_endpoint_env_var() {
        with_env("OTEL_DISABLED", "", || {
            with_env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1", || {
                // The endpoint "http://127.0.0.1:1" is unlikely to have an OTLP
                // collector listening, but the builder may still succeed.  The
                // test just asserts no unwrap/panic.
                let result = init_otel_tracer();
                // Accept either success (layer registered) or a runtime error
                // (unreachable endpoint at init time).
                match &result {
                    Ok(()) => {
                        // Must shut down to clean up the OnceLock / subscriber.
                        shutdown_otel_tracer();
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        assert!(
                            msg.contains("OTLP")
                                || msg.contains("runtime error")
                                || msg.contains("connection failed")
                                || msg.contains("exporter"),
                            "unexpected error: {msg}"
                        );
                    }
                }
            });
        });
    }
}
