// SPDX-License-Identifier: Apache-2.0

//! Task runner: drives a single task through its lifecycle.
//!
//! Owns one task, starts it via a driver, applies the restart policy on exit,
//! and tracks state. Mirrors the subset of upstream Nomad's client
//! `taskrunner`. Behaviour is specified by the tests and is unimplemented.

use crate::driver::TaskState;
use crate::error::Result;
use crate::jobspec::Task;

/// Drives one task's lifecycle on a node.
#[derive(Debug)]
pub struct TaskRunner {
    /// The task being run.
    #[allow(dead_code, reason = "read once the runner lifecycle is implemented")]
    task: Task,
    /// Current driver-reported state of the task.
    state: TaskState,
    /// How many times the task has been restarted.
    restart_count: u32,
}

impl TaskRunner {
    /// Create a runner for `task`.
    #[must_use]
    pub fn new(task: Task) -> Self {
        Self { task, state: TaskState::Pending, restart_count: 0 }
    }

    /// Current driver-reported state of the task.
    #[must_use]
    pub fn state(&self) -> TaskState {
        self.state
    }

    /// How many times the task has been restarted.
    #[must_use]
    pub fn restart_count(&self) -> u32 {
        self.restart_count
    }

    /// Start the task via its driver.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver fails to start the task.
    pub fn start(&mut self) -> Result<()> {
        self.state = TaskState::Running;
        Ok(())
    }

    /// Handle task exit; returns whether it will be restarted per the restart
    /// policy. `success` is whether the task exited zero.
    #[must_use]
    pub fn handle_exit(&mut self, success: bool) -> bool {
        // ponytail: simple policy — restart on failure, terminal on success.
        // Full restart-policy evaluation (attempts, interval, delay) added when needed.
        if success {
            self.state = TaskState::Exited;
            false
        } else {
            self.restart_count += 1;
            true
        }
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::jobspec::Resources;
    use std::collections::HashMap;

    fn runner() -> TaskRunner {
        TaskRunner::new(Task {
            name: "web".to_owned(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources::default(),
        })
    }

    #[test]
    fn new_runner_is_pending() {
        assert_eq!(runner().state(), TaskState::Pending);
    }

    #[test]
    fn new_runner_has_zero_restarts() {
        assert_eq!(runner().restart_count(), 0);
    }

    #[test]
    fn failed_exit_triggers_restart() {
        assert!(runner().handle_exit(false));
    }

    #[test]
    fn successful_exit_is_terminal() {
        assert!(!runner().handle_exit(true));
    }
}
