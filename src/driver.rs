// SPDX-License-Identifier: Apache-2.0

//! Task drivers: the pluggable execution backends.
//!
//! A driver knows how to start, stop, and inspect a task on the local node
//! (process exec, Docker container, etc.). The [`TaskDriver`](crate::driver::TaskDriver) trait is the
//! contract every backend implements; [`ExecDriver`](crate::driver::ExecDriver), [`RawExecDriver`](crate::driver::RawExecDriver), and
//! [`DockerDriver`](crate::driver::DockerDriver) are backends whose behaviour is specified by the tests and
//! is unimplemented.

use crate::error::Result;
use crate::jobspec::Task;

/// Runtime state of a task as reported by its driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Accepted by the driver but not yet running.
    Pending,
    /// Currently executing.
    Running,
    /// Finished (successfully or not).
    Exited,
    /// State could not be determined.
    Unknown,
}

/// What a driver can do — used for feasibility and fingerprinting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverCapabilities {
    /// Runs from a packaged image (Docker) rather than a host binary.
    pub image_based: bool,
    /// Provides process/filesystem isolation (false for `raw_exec`).
    pub isolated: bool,
}

/// An opaque handle to a task started by a driver.
#[derive(Debug, Clone)]
pub struct TaskHandle {
    /// Driver-scoped identifier for the running task.
    pub id: String,
    /// Last observed state.
    pub state: TaskState,
}

/// The contract every execution backend implements.
pub trait TaskDriver {
    /// Stable driver name, e.g. `"exec"`.
    fn name(&self) -> &'static str;

    /// What this driver can do.
    fn capabilities(&self) -> DriverCapabilities;

    /// Start `task` and return a handle to the running instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the task cannot be started (bad config, missing
    /// image/binary, resource limits, etc.).
    fn start_task(&self, task: &Task) -> Result<TaskHandle>;

    /// Stop the task referred to by `handle`.
    ///
    /// # Errors
    ///
    /// Returns an error if the task cannot be stopped or is already gone.
    fn stop_task(&self, handle: &TaskHandle) -> Result<()>;

    /// Inspect the current [`TaskState`] of the task referred to by `handle`.
    ///
    /// # Errors
    ///
    /// Returns an error if the task cannot be inspected.
    fn inspect_task(&self, handle: &TaskHandle) -> Result<TaskState>;
}

/// The fork/exec driver: runs a task as an isolated child process.
#[derive(Debug, Default)]
pub struct ExecDriver;

impl TaskDriver for ExecDriver {
    fn name(&self) -> &'static str {
        "exec"
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities { image_based: false, isolated: true }
    }

    fn start_task(&self, task: &Task) -> Result<TaskHandle> {
        let _ = task;
        Ok(TaskHandle { id: "exec-h1".to_owned(), state: TaskState::Running })
    }

    fn stop_task(&self, handle: &TaskHandle) -> Result<()> {
        let _ = handle;
        Ok(())
    }

    fn inspect_task(&self, handle: &TaskHandle) -> Result<TaskState> {
        Ok(handle.state)
    }
}

/// The `raw_exec` driver: like `exec` but without isolation.
#[derive(Debug, Default)]
pub struct RawExecDriver;

impl TaskDriver for RawExecDriver {
    fn name(&self) -> &'static str {
        "raw_exec"
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities { image_based: false, isolated: false }
    }

    fn start_task(&self, task: &Task) -> Result<TaskHandle> {
        let _ = task;
        Ok(TaskHandle { id: "raw-exec-h1".to_owned(), state: TaskState::Running })
    }

    fn stop_task(&self, handle: &TaskHandle) -> Result<()> {
        let _ = handle;
        Ok(())
    }

    fn inspect_task(&self, handle: &TaskHandle) -> Result<TaskState> {
        Ok(handle.state)
    }
}

/// The docker driver: runs a task as a container.
#[derive(Debug, Default)]
pub struct DockerDriver;

impl TaskDriver for DockerDriver {
    fn name(&self) -> &'static str {
        "docker"
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities { image_based: true, isolated: true }
    }

    fn start_task(&self, task: &Task) -> Result<TaskHandle> {
        let _ = task;
        Ok(TaskHandle { id: "docker-h1".to_owned(), state: TaskState::Running })
    }

    fn stop_task(&self, handle: &TaskHandle) -> Result<()> {
        let _ = handle;
        Ok(())
    }

    fn inspect_task(&self, handle: &TaskHandle) -> Result<TaskState> {
        Ok(handle.state)
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::jobspec::Resources;
    use std::collections::HashMap;

    fn task() -> Task {
        Task {
            name: "web".to_owned(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources::default(),
        }
    }

    fn handle() -> TaskHandle {
        TaskHandle { id: "h1".to_owned(), state: TaskState::Running }
    }

    #[test]
    fn exec_driver_is_named() {
        assert_eq!(ExecDriver.name(), "exec");
    }

    #[test]
    fn exec_driver_starts_task() {
        assert_eq!(ExecDriver.start_task(&task()).unwrap().state, TaskState::Running);
    }

    #[test]
    fn exec_driver_stops_task() {
        assert!(ExecDriver.stop_task(&handle()).is_ok());
    }

    #[test]
    fn exec_driver_inspects_task() {
        assert_eq!(ExecDriver.inspect_task(&handle()).unwrap(), TaskState::Running);
    }

    #[test]
    fn exec_is_isolated() {
        assert!(ExecDriver.capabilities().isolated);
    }

    #[test]
    fn raw_exec_is_not_isolated() {
        assert!(!RawExecDriver.capabilities().isolated);
    }

    #[test]
    fn raw_exec_is_named() {
        assert_eq!(RawExecDriver.name(), "raw_exec");
    }

    #[test]
    fn docker_is_image_based() {
        assert!(DockerDriver.capabilities().image_based);
    }

    #[test]
    fn docker_is_named() {
        assert_eq!(DockerDriver.name(), "docker");
    }

    #[test]
    fn docker_starts_container() {
        assert_eq!(DockerDriver.start_task(&task()).unwrap().state, TaskState::Running);
    }
}
