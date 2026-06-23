// SPDX-License-Identifier: Apache-2.0

//! Logging subsystem initialisation.
//!
//! Sets up a `tracing` subscriber with env-filter support and optional
//! file rotation.  Call [`crate::logging::init`] once at process start.

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::{Directive, EnvFilter};
use tracing_subscriber::prelude::*;

use crate::config::Config;

/// Build an `EnvFilter` from a static log-level directive.
///
/// `level` is a `LogLevel::as_str()` output ("error".."trace"); if it somehow
/// fails to parse, the filter defaults to `info`. Also respects `RUST_LOG`.
fn build_filter(level: &str) -> EnvFilter {
    let directive: Directive = level.parse().unwrap_or_default();
    EnvFilter::builder().with_default_directive(directive).from_env_lossy()
}

/// Initialise the tracing subscriber.
///
/// When `log_dir` is set and non-empty, logs are written to both stderr
/// and a daily-rotating file (`nomad-rs.log`).  The returned
/// [`WorkerGuard`] must be kept alive for the lifetime of the process.
///
/// # Panics
///
/// Panics if the subscriber has already been set.
#[must_use]
pub fn init(config: &Config) -> Option<WorkerGuard> {
    let filter = build_filter(config.log_level.as_str());

    if config.log_dir.as_os_str().is_empty() {
        let subscriber = tracing_subscriber::registry().with(filter).with(tracing_subscriber::fmt::layer());
        subscriber.init();
        return None;
    }

    let file_appender = tracing_appender::rolling::daily(&config.log_dir, "nomad-rs.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking).with_ansi(false));
    subscriber.init();
    Some(guard)
}
