// SPDX-License-Identifier: Apache-2.0

//! Shared agent lifecycle status.
//!
//! The client, server, and scheduler share one lifecycle shape, so they share
//! one enum. Each re-exports it under a domain-specific alias (`ClientStatus`,
//! `ServerStatus`, `SchedulerStatus`).

/// Lifecycle status of a long-running agent component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentStatus {
    /// Created but not running.
    Initialized,
    /// Actively running.
    Running,
    /// Stopped.
    Stopped,
    /// Hit a terminal error.
    Failed,
}
