// SPDX-License-Identifier: Apache-2.0

//! Task runner: drives a single task through its lifecycle.
//!
//! Owns one task, starts it via a driver, applies the restart policy on exit,
//! and tracks state. Mirrors the subset of upstream Nomad's client
//! `taskrunner`. Behaviour is specified by the tests and is unimplemented.

use crate::driver::{ExecDriver, TaskDriver, TaskHandle, TaskState};
use crate::error::{Error, Result};
use crate::jobspec::Task;
use crate::reschedule::{RestartMode, RestartPolicy};

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
    /// Governs in-place restarts on failure.
    restart_policy: RestartPolicy,
}

impl TaskRunner {
    /// Create a runner for `task` with the default restart policy (upstream
    /// Nomad's default: 2 attempts in 30 min, then fail the alloc).
    #[must_use]
    pub fn new(task: Task) -> Self {
        Self {
            task,
            driver: ExecDriver::default(),
            handle: None,
            state: TaskState::Pending,
            restart_count: 0,
            restart_policy: RestartPolicy { attempts: 2, interval_secs: 1800, delay_secs: 15, mode: RestartMode::Fail },
        }
    }

    /// Override the restart policy (builder style).
    ///
    /// ponytail: plumb this from the task group's `restart` block once jobspec
    /// carries one; today every runner uses the default.
    #[must_use]
    pub fn with_restart_policy(mut self, policy: RestartPolicy) -> Self {
        self.restart_policy = policy;
        self
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
        // Terminal states are sticky: a reaped child is dropped from the driver,
        // so re-inspecting it would report `Exited` and silently downgrade a
        // recorded `Failed`. Don't re-inspect once terminal.
        if matches!(self.state, TaskState::Exited | TaskState::Failed) {
            return Ok(self.state);
        }
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
    ///
    /// A clean exit is terminal. A failure restarts while under the policy's
    /// `attempts` cap; once exhausted the task is marked [`TaskState::Failed`]
    /// (the alloc then fails and the scheduler can reschedule it).
    ///
    /// ponytail: counts lifetime attempts, not a sliding `interval_secs` window,
    /// and ignores `delay_secs`/`RestartMode::Delay` (no timer yet). Add a clock
    /// when restart timing matters.
    #[must_use]
    pub fn handle_exit(&mut self, success: bool) -> bool {
        if success {
            self.state = TaskState::Exited;
            return false;
        }
        if self.restart_count < self.restart_policy.attempts {
            self.restart_count += 1;
            self.state = TaskState::Pending; // about to be restarted
            self.handle = None; // drop the dead handle so start() can relaunch
            true
        } else {
            self.state = TaskState::Failed;
            false
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

    #[test]
    fn restart_stops_and_fails_after_attempts_exhausted() {
        use crate::reschedule::{RestartMode, RestartPolicy};
        let mut r = runner().with_restart_policy(RestartPolicy {
            attempts: 1,
            interval_secs: 60,
            delay_secs: 0,
            mode: RestartMode::Fail,
        });
        assert!(r.handle_exit(false), "first failure restarts (within attempts)");
        assert_eq!(r.restart_count(), 1);
        assert!(!r.handle_exit(false), "attempts exhausted → no more restarts");
        assert_eq!(r.state(), TaskState::Failed);
        assert_eq!(r.restart_count(), 1, "exhausted restart is not counted");
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
    fn poll_keeps_failed_sticky() {
        let mut r = runner_cmd("false", &[]);
        r.start().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(r.poll().unwrap(), TaskState::Failed);
        assert_eq!(r.poll().unwrap(), TaskState::Failed, "terminal failure must not downgrade to Exited");
    }

    #[test]
    fn restart_clears_handle_so_task_can_relaunch() {
        let mut r = runner_cmd("true", &[]);
        r.start().unwrap();
        assert!(r.handle_exit(false), "first failure restarts");
        // Handle cleared → start() is allowed again (no "already started").
        r.start().expect("restart must be able to relaunch");
        r.stop().unwrap();
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
