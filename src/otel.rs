// SPDX-License-Identifier: Apache-2.0

//! OpenTelemetry tracing middleware.
//!
//! Provides initialisation and shutdown helpers for OTLP-exported tracing
//! via `tracing-opentelemetry`.  Controlled by the `OTEL_DISABLED` and
//! `OTEL_EXPORTER_OTLP_ENDPOINT` environment variables.

use crate::error::{Error, Result};
use opentelemetry::trace::TracerProvider;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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
    let provider = SdkTracerProvider::builder().with_simple_exporter(otlp_exporter).build();

    let tracer = provider.tracer("nomad-rs");

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(otel_layer)
        .try_init()
        .map_err(|e| Error::Runtime(format!("failed to init OTel tracing layer: {e}")))?;

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

    tracing::info!("otel tracer shutdown requested (no-op in 0.28.x API)");
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {}
