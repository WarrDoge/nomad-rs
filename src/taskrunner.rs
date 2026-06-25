// SPDX-License-Identifier: Apache-2.0

//! Task runner: drives a single task through its lifecycle.
//!
//! Owns one task, starts it via a driver, applies the restart policy on exit,
//! and tracks state. Mirrors the subset of upstream Nomad's client
//! `taskrunner`. Behaviour is specified by the tests and is unimplemented.

use crate::driver::{ExecDriver, TaskDriver, TaskHandle, TaskState};
use crate::error::{Error, Result};
use crate::jobspec::Task;

/// Drives one task's lifecycle on a node.
///
/// ponytail: only the `exec` driver is wired. Other driver names fall through
/// to `exec` for now — add a real driver registry when `docker`/`raw_exec`
/// gain real backends.
#[derive(Debug)]
pub struct TaskRunner {
    /// The task being run.
    task: Task,
    /// The execution backend.
    driver: ExecDriver,
    /// Handle to the running task, once started.
    handle: Option<TaskHandle>,
    /// Current driver-reported state of the task.
    state: TaskState,
    /// How many times the task has been restarted.
    restart_count: u32,
}

impl TaskRunner {
    /// Create a runner for `task`.
    #[must_use]
    pub fn new(task: Task) -> Self {
        Self { task, driver: ExecDriver::default(), handle: None, state: TaskState::Pending, restart_count: 0 }
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
        if self.handle.is_some() {
            return Err(Error::Runtime(format!("task '{}' already started", self.task.name)));
        }
        // Only the exec driver is wired; reject other driver names rather than
        // silently running them under exec.
        if self.task.driver != self.driver.name() {
            return Err(Error::Runtime(format!("unsupported task driver '{}'", self.task.driver)));
        }
        let handle = self.driver.start_task(&self.task)?;
        self.state = handle.state;
        self.handle = Some(handle);
        Ok(())
    }

    /// Refresh and return the task's current state from the driver.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver cannot inspect the task.
    pub fn poll(&mut self) -> Result<TaskState> {
        if let Some(handle) = &self.handle {
            self.state = self.driver.inspect_task(handle)?;
        }
        Ok(self.state)
    }

    /// Stop the task via its driver.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver fails to stop the task.
    pub fn stop(&mut self) -> Result<()> {
        if let Some(handle) = &self.handle {
            self.driver.stop_task(handle)?;
        }
        self.state = TaskState::Exited;
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

    fn runner_cmd(command: &str, args: &[&str]) -> TaskRunner {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!(command));
        config.insert("args".to_owned(), serde_json::json!(args));
        TaskRunner::new(Task {
            name: "web".to_owned(),
            driver: "exec".to_owned(),
            config,
            resources: Resources::default(),
        })
    }

    #[test]
    fn start_spawns_real_process_and_stop_terminates_it() {
        let mut r = runner_cmd("sleep", &["30"]);
        r.start().unwrap();
        assert_eq!(r.state(), TaskState::Running);
        assert_eq!(r.poll().unwrap(), TaskState::Running);
        r.stop().unwrap();
        assert_eq!(r.poll().unwrap(), TaskState::Exited);
    }

    #[test]
    fn poll_reports_exit_after_short_process_completes() {
        let mut r = runner_cmd("true", &[]);
        r.start().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(r.poll().unwrap(), TaskState::Exited);
    }

    #[test]
    fn start_with_missing_command_errors() {
        assert!(runner().start().is_err());
    }

    #[test]
    fn double_start_is_rejected() {
        let mut r = runner_cmd("sleep", &["30"]);
        r.start().unwrap();
        assert!(r.start().is_err(), "second start must not spawn a second process");
        r.stop().unwrap();
    }

    #[test]
    fn unsupported_driver_errors() {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("sleep"));
        config.insert("args".to_owned(), serde_json::json!(["30"]));
        let mut r = TaskRunner::new(Task {
            name: "web".to_owned(),
            driver: "docker".to_owned(),
            config,
            resources: Resources::default(),
        });
        assert!(r.start().is_err());
    }
}
