// SPDX-License-Identifier: Apache-2.0

//! Task drivers: the pluggable execution backends.
//!
//! A driver knows how to start, stop, and inspect a task on the local node
//! (process exec, Docker container, etc.). The [`TaskDriver`](crate::driver::TaskDriver) trait is the
//! contract every backend implements; [`ExecDriver`](crate::driver::ExecDriver), [`RawExecDriver`](crate::driver::RawExecDriver), and
//! [`DockerDriver`](crate::driver::DockerDriver) are backends whose behaviour is specified by the tests and
//! is unimplemented.

use crate::error::{Error, Result};
use crate::jobspec::Task;
use std::collections::HashMap;
use std::process::{Child, Command};
use std::sync::Mutex;

/// Runtime state of a task as reported by its driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Accepted by the driver but not yet running.
    Pending,
    /// Currently executing.
    Running,
    /// Finished cleanly (exit code 0).
    Exited,
    /// Finished unsuccessfully (non-zero exit or killed by signal).
    Failed,
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

/// The fork/exec driver: runs a task as a child process.
///
/// Task config keys: `command` (string, required) and `args` (array of
/// strings, optional). The handle id is the child pid.
///
/// ponytail: real fork/exec via stdlib, but NOT yet isolated — no cgroups,
/// namespaces, or chroot despite `capabilities().isolated`. Add isolation
/// (cgroup v2 + namespaces, Linux-only) before trusting this as a security
/// boundary; today it is functionally `raw_exec`.
#[derive(Debug, Default)]
pub struct ExecDriver {
    /// Live children keyed by pid string, so stop/inspect can reach them.
    running: Mutex<HashMap<String, Child>>,
}

impl TaskDriver for ExecDriver {
    fn name(&self) -> &'static str {
        "exec"
    }

    fn capabilities(&self) -> DriverCapabilities {
        // Not isolated yet: a bare child process is functionally raw_exec. Flip
        // to true only once cgroups/namespaces land (see struct doc comment).
        DriverCapabilities { image_based: false, isolated: false }
    }

    fn start_task(&self, task: &Task) -> Result<TaskHandle> {
        let command = task
            .config
            .get("command")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Runtime("exec driver: missing `command` in task config".to_owned()))?;
        // Reject malformed args rather than silently dropping non-string entries
        // (which would launch a different command line).
        let args: Vec<String> = match task.config.get("args") {
            None => Vec::new(),
            Some(serde_json::Value::Array(values)) => values
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(ToOwned::to_owned)
                        .ok_or_else(|| Error::Runtime("exec driver: `args` entries must be strings".to_owned()))
                })
                .collect::<Result<Vec<_>>>()?,
            Some(_) => return Err(Error::Runtime("exec driver: `args` must be an array".to_owned())),
        };

        let mut cmd = Command::new(command);
        cmd.args(&args);
        // Put the child in its own process group so stop_task can kill the whole
        // tree (forked grandchildren), not just the direct child.
        #[cfg(unix)]
        std::os::unix::process::CommandExt::process_group(&mut cmd, 0);
        let child = cmd.spawn()?;
        let id = child.id().to_string();
        self.running.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(id.clone(), child);
        Ok(TaskHandle { id, state: TaskState::Running })
    }

    fn stop_task(&self, handle: &TaskHandle) -> Result<()> {
        if let Some(mut child) =
            self.running.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&handle.id)
        {
            kill_tree(&child);
            child.kill()?;
            let _ = child.wait();
        }
        Ok(())
    }

    fn inspect_task(&self, handle: &TaskHandle) -> Result<TaskState> {
        let mut running = self.running.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(child) = running.get_mut(&handle.id) else {
            // Unknown id, or already reaped by stop_task → treat as finished.
            return Ok(TaskState::Exited);
        };
        if let Some(status) = child.try_wait()? {
            // Reaped: drop the entry so long-lived agents don't accumulate
            // stale handles. Repeat inspects hit the `None` branch above.
            running.remove(&handle.id);
            Ok(if status.success() { TaskState::Exited } else { TaskState::Failed })
        } else {
            Ok(TaskState::Running)
        }
    }
}

/// Best-effort SIGKILL of the child's whole process group (set in `start_task`).
/// Reaps grandchildren the direct `child.kill()` would miss. Shelling out to
/// `kill -<pgid>` keeps the crate `forbid(unsafe_code)`-clean (no `libc::killpg`).
#[cfg(unix)]
fn kill_tree(child: &Child) {
    // Negative pid targets the whole group; group id == child pid because we
    // spawned with process_group(0). Errors (group already gone) are ignored.
    let _ = Command::new("kill").arg("-KILL").arg(format!("-{}", child.id())).status();
}

/// No process-group support off unix; the direct `child.kill()` is all we have.
#[cfg(not(unix))]
fn kill_tree(_child: &Child) {}

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

    fn task_cmd(command: &str, args: &[&str]) -> Task {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!(command));
        config.insert("args".to_owned(), serde_json::json!(args));
        Task { name: "web".to_owned(), driver: "exec".to_owned(), config, resources: Resources::default() }
    }

    #[test]
    fn exec_driver_is_named() {
        assert_eq!(ExecDriver::default().name(), "exec");
    }

    #[test]
    fn exec_driver_spawns_real_process() {
        let driver = ExecDriver::default();
        let h = driver.start_task(&task_cmd("sleep", &["30"])).unwrap();
        assert_eq!(h.state, TaskState::Running);
        // Real pid, not the old "exec-h1" stub sentinel.
        assert!(h.id.parse::<u32>().is_ok(), "handle id should be a pid, got {}", h.id);
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Running);
        driver.stop_task(&h).unwrap();
    }

    #[test]
    fn exec_driver_missing_command_errors() {
        assert!(ExecDriver::default().start_task(&task()).is_err());
    }

    #[test]
    fn exec_driver_inspect_reports_exited_after_completion() {
        let driver = ExecDriver::default();
        let h = driver.start_task(&task_cmd("true", &[])).unwrap();
        // Give the short-lived process a moment to exit.
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Exited);
    }

    #[test]
    fn exec_driver_inspect_reports_failed_after_nonzero_exit() {
        let driver = ExecDriver::default();
        let h = driver.start_task(&task_cmd("false", &[])).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Failed);
    }

    #[test]
    fn exec_driver_stop_kills_running_process() {
        let driver = ExecDriver::default();
        let h = driver.start_task(&task_cmd("sleep", &["30"])).unwrap();
        driver.stop_task(&h).unwrap();
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Exited);
    }

    #[test]
    fn exec_is_not_isolated_yet() {
        // Honest until cgroups/namespaces land — a bare child is not sandboxed.
        assert!(!ExecDriver::default().capabilities().isolated);
    }

    #[test]
    fn exec_driver_rejects_non_string_args() {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("echo"));
        config.insert("args".to_owned(), serde_json::json!(["--port", 8080]));
        let task = Task { name: "x".to_owned(), driver: "exec".to_owned(), config, resources: Resources::default() };
        assert!(ExecDriver::default().start_task(&task).is_err());
    }

    #[test]
    fn exec_driver_rejects_non_array_args() {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("echo"));
        config.insert("args".to_owned(), serde_json::json!("oops"));
        let task = Task { name: "x".to_owned(), driver: "exec".to_owned(), config, resources: Resources::default() };
        assert!(ExecDriver::default().start_task(&task).is_err());
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
