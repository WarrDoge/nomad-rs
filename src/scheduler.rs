// SPDX-License-Identifier: Apache-2.0

//! Nomad scheduler — evaluation, ranking, and placement logic.
//!
//! The scheduler watches for pending evaluations, calculates placement
//! scores across candidate nodes, and produces allocation plans.

use crate::error::Result;

/// The possible states a scheduler can be in.
pub use crate::agent::AgentStatus as SchedulerStatus;

/// The core scheduler responsible for placing tasks onto nodes.
#[derive(Debug)]
pub struct Scheduler {
    /// Whether the scheduler is currently running.
    status: SchedulerStatus,
}

impl Scheduler {
    /// Create a new scheduler instance.
    #[must_use]
    pub fn new() -> Self {
        Self { status: SchedulerStatus::Initialized }
    }

    /// Returns the current status of the scheduler.
    #[must_use]
    pub fn status(&self) -> SchedulerStatus {
        self.status
    }

    /// Returns `true` if the scheduler is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == SchedulerStatus::Running
    }

    /// Run the scheduler evaluation loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the scheduling loop encounters a fatal error.
    #[allow(clippy::unused_async)]
    pub async fn run(&mut self) -> Result<()> {
        if self.status == SchedulerStatus::Running {
            return Ok(());
        }
        self.status = SchedulerStatus::Running;
        tracing::info!("scheduler starting");
        // TODO: implement the bin-packing scheduler loop
        Ok(())
    }

    /// Gracefully stop the scheduler.
    pub fn stop(&mut self) {
        self.status = SchedulerStatus::Stopped;
        tracing::info!("scheduler stopped");
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::block_on;

    #[test]
    fn test_scheduler_new() {
        let scheduler = Scheduler::new();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_scheduler_default() {
        let scheduler = Scheduler::default();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
    }

    #[test]
    fn test_scheduler_run() {
        let mut scheduler = Scheduler::new();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
        let result = block_on(scheduler.run());
        assert!(result.is_ok());
        assert!(scheduler.is_running());
        assert_eq!(scheduler.status(), SchedulerStatus::Running);
    }

    #[test]
    fn test_scheduler_run_idempotent() {
        let mut scheduler = Scheduler::new();
        let _ = block_on(scheduler.run());
        assert!(scheduler.is_running());
        let result = block_on(scheduler.run());
        assert!(result.is_ok());
        assert!(scheduler.is_running());
    }

    #[test]
    fn test_scheduler_stop() {
        let mut scheduler = Scheduler::new();
        let _ = block_on(scheduler.run());
        assert!(scheduler.is_running());
        scheduler.stop();
        assert_eq!(scheduler.status(), SchedulerStatus::Stopped);
        assert!(!scheduler.is_running());
    }

    #[test]
    fn test_scheduler_stop_before_run() {
        let mut scheduler = Scheduler::new();
        assert_eq!(scheduler.status(), SchedulerStatus::Initialized);
        scheduler.stop();
        assert_eq!(scheduler.status(), SchedulerStatus::Stopped);
    }
}
