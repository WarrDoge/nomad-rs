// SPDX-License-Identifier: Apache-2.0

//! Logging subsystem initialisation.
//!
//! Sets up a `tracing` subscriber with env-filter support and optional
//! file rotation.  Call [`init`] once at process start.

use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::prelude::*;

use crate::config::Config;

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
    let level = config.log_level.filter_directive();
    let filter =
        EnvFilter::builder().with_default_directive(level.parse().expect("valid log level directive")).from_env_lossy();

    if config.log_dir.as_os_str().is_empty() || config.log_dir == Path::new("") {
        // Stderr only.
        let subscriber = tracing_subscriber::registry().with(filter).with(tracing_subscriber::fmt::layer());
        subscriber.init();
        return None;
    }

    // Stderr + daily-rotating file.
    let file_appender = tracing_appender::rolling::daily(&config.log_dir, "nomad-rs.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::fmt::layer().with_writer(non_blocking).with_ansi(false));
    subscriber.init();
    Some(guard)
}
