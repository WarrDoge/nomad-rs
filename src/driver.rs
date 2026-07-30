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
pub trait TaskDriver: std::fmt::Debug + Send {
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
///
/// Task config keys: `command` (string, required) and `args` (array of
/// strings, optional). The handle id is the child pid.
///
/// Unlike [`ExecDriver`], this driver advertises `isolated: false` because
/// it does not (and will not) set up cgroups, namespaces, or a chroot. It
/// is a plain `std::process::Command::new(...).spawn()` — exactly what
/// upstream Nomad's `raw_exec` is.
#[derive(Debug, Default)]
pub struct RawExecDriver {
    /// Live children keyed by pid string, so stop/inspect can reach them.
    running: Mutex<HashMap<String, Child>>,
}

impl TaskDriver for RawExecDriver {
    fn name(&self) -> &'static str {
        "raw_exec"
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities { image_based: false, isolated: false }
    }

    fn start_task(&self, task: &Task) -> Result<TaskHandle> {
        let command = task
            .config
            .get("command")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Runtime("raw_exec driver: missing `command` in task config".to_owned()))?;
        let args: Vec<String> = match task.config.get("args") {
            None => Vec::new(),
            Some(serde_json::Value::Array(values)) => values
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(ToOwned::to_owned)
                        .ok_or_else(|| Error::Runtime("raw_exec driver: `args` entries must be strings".to_owned()))
                })
                .collect::<Result<Vec<_>>>()?,
            Some(_) => return Err(Error::Runtime("raw_exec driver: `args` must be an array".to_owned())),
        };

        let mut cmd = Command::new(command);
        cmd.args(&args);
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
            return Ok(TaskState::Exited);
        };
        if let Some(status) = child.try_wait()? {
            running.remove(&handle.id);
            Ok(if status.success() { TaskState::Exited } else { TaskState::Failed })
        } else {
            Ok(TaskState::Running)
        }
    }
}

/// The docker driver: runs a task as a container.
///
/// Task config keys: `image` (string, required), `args` (array of strings,
/// optional), and `command` (string, optional). The handle id is the
/// container name.
///
/// # Implementation
///
/// Shells out to the Docker CLI (`docker`). A future iteration should use
/// the `bollard` crate for a native Rust client with streaming logs, but
/// the CLI approach keeps `forbid(unsafe_code)` clean and avoids a large
/// dependency for what is fundamentally `docker run`/`docker stop`/`docker
/// inspect`.
///
/// # Test gating
///
/// Real container tests require a running Docker daemon. Use
/// `#[cfg(feature = "docker_test")]` — not `#[cfg(test)]` — so CI without
/// Docker stays green.
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
        let image = task
            .config
            .get("image")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::Runtime("docker driver: missing `image` in task config".to_owned()))?;

        let mut cmd = Command::new("docker");
        cmd.arg("run").arg("--rm").arg("-d");

        // Optional command override
        if let Some(command) = task
            .config
            .get("command")
            .and_then(serde_json::Value::as_str)
        {
            cmd.arg("--entrypoint").arg(command);
        }

        // Optional args
        if let Some(serde_json::Value::Array(values)) = task.config.get("args") {
            for v in values {
                cmd.arg(
                    v.as_str()
                        .ok_or_else(|| Error::Runtime("docker driver: `args` entries must be strings".to_owned()))?,
                );
            }
        }

        cmd.arg(image);

        let output = cmd.output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Runtime(format!("docker run failed: {stderr}")));
        }
        let container_id = String::from_utf8(output.stdout)
            .map_err(|_| Error::Runtime("docker run output is not valid UTF-8".to_owned()))?
            .trim()
            .to_owned();

        if container_id.is_empty() {
            return Err(Error::Runtime("docker run produced no container id".to_owned()));
        }

        Ok(TaskHandle { id: container_id, state: TaskState::Running })
    }

    fn stop_task(&self, handle: &TaskHandle) -> Result<()> {
        let status = Command::new("docker")
            .args(["stop", "--time", "5", &handle.id])
            .status()?;
        if !status.success() {
            // Container may already have exited; that's fine.
            tracing::debug!("docker stop for {} exited with {}", handle.id, status);
        }
        Ok(())
    }

    fn inspect_task(&self, handle: &TaskHandle) -> Result<TaskState> {
        let output = Command::new("docker")
            .args(["inspect", "--format", "{{.State.Status}}", &handle.id])
            .output()?;
        if !output.status.success() {
            // Container gone or never existed.
            return Ok(TaskState::Exited);
        }
        let status = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        match status.as_str() {
            "running" => Ok(TaskState::Running),
            "exited" | "dead" => {
                // Check exit code for Failed vs Exited distinction.
                let exit_output = Command::new("docker")
                    .args(["inspect", "--format", "{{.State.ExitCode}}", &handle.id])
                    .output()?;
                if exit_output.status.success() {
                    let code = String::from_utf8_lossy(&exit_output.stdout).trim().to_owned();
                    if code == "0" {
                        return Ok(TaskState::Exited);
                    }
                }
                Ok(TaskState::Failed)
            },
            _ => Ok(TaskState::Unknown),
        }
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
    fn raw_exec_spawns_real_process() {
        let driver = RawExecDriver::default();
        let h = driver.start_task(&task_cmd("sleep", &["30"])).unwrap();
        assert_eq!(h.state, TaskState::Running);
        assert!(h.id.parse::<u32>().is_ok(), "handle id should be a pid, got {}", h.id);
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Running);
        driver.stop_task(&h).unwrap();
    }

    #[test]
    fn raw_exec_missing_command_errors() {
        assert!(RawExecDriver::default().start_task(&task()).is_err());
    }

    #[test]
    fn raw_exec_inspect_reports_exited_after_completion() {
        let driver = RawExecDriver::default();
        let h = driver.start_task(&task_cmd("true", &[])).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Exited);
    }

    #[test]
    fn raw_exec_inspect_reports_failed_after_nonzero_exit() {
        let driver = RawExecDriver::default();
        let h = driver.start_task(&task_cmd("false", &[])).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Failed);
    }

    #[test]
    fn raw_exec_stop_kills_running_process() {
        let driver = RawExecDriver::default();
        let h = driver.start_task(&task_cmd("sleep", &["30"])).unwrap();
        driver.stop_task(&h).unwrap();
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Exited);
    }

    #[test]
    fn raw_exec_rejects_non_string_args() {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("echo"));
        config.insert("args".to_owned(), serde_json::json!(["--port", 8080]));
        let task = Task { name: "x".to_owned(), driver: "raw_exec".to_owned(), config, resources: Resources::default() };
        assert!(RawExecDriver::default().start_task(&task).is_err());
    }

    #[test]
    fn raw_exec_rejects_non_array_args() {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("echo"));
        config.insert("args".to_owned(), serde_json::json!("oops"));
        let task = Task { name: "x".to_owned(), driver: "raw_exec".to_owned(), config, resources: Resources::default() };
        assert!(RawExecDriver::default().start_task(&task).is_err());
    }

    #[test]
    fn raw_exec_is_not_isolated() {
        assert!(!RawExecDriver::default().capabilities().isolated);
    }

    #[test]
    fn raw_exec_is_named() {
        assert_eq!(RawExecDriver::default().name(), "raw_exec");
    }

    #[test]
    fn docker_missing_image_errors() {
        assert!(DockerDriver.start_task(&task()).is_err());
    }

    #[test]
    fn docker_handler_strings() {
        assert_eq!(DockerDriver.name(), "docker");
        assert!(DockerDriver.capabilities().image_based);
        assert!(DockerDriver.capabilities().isolated);
    }

    #[test]
    fn docker_rejects_non_string_args() {
        let mut config = HashMap::new();
        config.insert("image".to_owned(), serde_json::json!("alpine"));
        config.insert("args".to_owned(), serde_json::json!(["echo", 42]));
        let task = Task { name: "x".to_owned(), driver: "docker".to_owned(), config, resources: Resources::default() };
        assert!(DockerDriver.start_task(&task).is_err());
    }

    /// Real Docker container lifecycle test — requires a running Docker
    /// daemon. Gate behind `docker_test` feature so CI without Docker
    /// stays green.
    #[cfg(feature = "docker_test")]
    #[test]
    fn docker_runs_alpine_echo() {
        let mut config = HashMap::new();
        config.insert("image".to_owned(), serde_json::json!("alpine"));
        config.insert("args".to_owned(), serde_json::json!(["echo", "hello"]));
        let task = Task { name: "echo".to_owned(), driver: "docker".to_owned(), config, resources: Resources::default() };
        let driver = DockerDriver;
        let h = driver.start_task(&task).unwrap();
        assert_eq!(h.state, TaskState::Running);
        assert!(!h.id.is_empty(), "got a container id");
        // Give it a moment to finish
        std::thread::sleep(std::time::Duration::from_secs(2));
        // Should have exited cleanly
        match driver.inspect_task(&h).unwrap() {
            TaskState::Exited => {},
            TaskState::Running => {
                // Still running? stop it
                driver.stop_task(&h).unwrap();
                panic!("alpine echo should exit quickly, not stay running");
            },
            other => panic!("unexpected state: {other:?}"),
        }
        driver.stop_task(&h).unwrap();
    }

    /// Real Docker stop test — requires a running Docker daemon.
    #[cfg(feature = "docker_test")]
    #[test]
    fn docker_stop_kills_container() {
        let mut config = HashMap::new();
        config.insert("image".to_owned(), serde_json::json!("alpine"));
        config.insert("args".to_owned(), serde_json::json!(["sleep", "30"]));
        let task = Task { name: "sleeper".to_owned(), driver: "docker".to_owned(), config, resources: Resources::default() };
        let driver = DockerDriver;
        let h = driver.start_task(&task).unwrap();
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Running);
        driver.stop_task(&h).unwrap();
        assert_eq!(driver.inspect_task(&h).unwrap(), TaskState::Exited);
    }
}
