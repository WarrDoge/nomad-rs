// SPDX-License-Identifier: Apache-2.0

//! Nomad job specification types.
//!
//! Represents the HCL-based job specification used to define workloads.
//! This module provides strongly-typed Rust equivalents of Nomad's job
//! specification structures.

use std::collections::HashMap;

use crate::error::{Error, Result};

/// Lowest job priority Nomad accepts (mirrors upstream `JobMinPriority`).
pub const JOB_MIN_PRIORITY: i32 = 1;
/// Default job priority when unset (mirrors upstream `JobDefaultPriority`).
pub const JOB_DEFAULT_PRIORITY: i32 = 50;
/// Highest job priority Nomad accepts (mirrors upstream `JobMaxPriority`).
pub const JOB_MAX_PRIORITY: i32 = 100;
/// Minimum CPU a task may reserve, in MHz (mirrors upstream resource minimum).
pub const MIN_CPU_MHZ: i32 = 1;
/// Minimum memory a task may reserve, in MB (mirrors upstream resource minimum).
pub const MIN_MEMORY_MB: i32 = 10;

/// A Nomad job represents a set of task groups that run on the cluster.
#[derive(Debug, Clone)]
pub struct Job {
    /// Human-readable job name.
    pub name: String,
    /// Arbitrary metadata key-value pairs.
    pub meta: HashMap<String, String>,
    /// The datacenters where this job can run.
    pub datacenters: Vec<String>,
    /// The task groups that comprise this job.
    pub task_groups: Vec<TaskGroup>,
    /// Job priority (higher = more important).
    pub priority: i32,
}

impl Default for Job {
    fn default() -> Self {
        Self {
            name: String::new(),
            meta: HashMap::new(),
            datacenters: vec!["dc1".to_owned()],
            task_groups: Vec::new(),
            priority: JOB_DEFAULT_PRIORITY,
        }
    }
}

impl Job {
    /// Validate the job specification against Nomad's structural rules.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] describing the first rule the job violates:
    /// missing name, out-of-range priority, missing or empty datacenters,
    /// missing or duplicately-named task groups, or any invalid task group.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Config("missing job name".to_owned()));
        }
        if !(JOB_MIN_PRIORITY..=JOB_MAX_PRIORITY).contains(&self.priority) {
            return Err(Error::Config(format!(
                "job priority must be between {JOB_MIN_PRIORITY} and {JOB_MAX_PRIORITY}, got {}",
                self.priority
            )));
        }
        if self.datacenters.is_empty() {
            return Err(Error::Config("missing job datacenters".to_owned()));
        }
        if self.datacenters.iter().any(String::is_empty) {
            return Err(Error::Config("job datacenter must be non-empty".to_owned()));
        }
        if self.task_groups.is_empty() {
            return Err(Error::Config("missing job task groups".to_owned()));
        }
        let mut seen = std::collections::HashSet::new();
        for group in &self.task_groups {
            if !seen.insert(group.name.as_str()) {
                return Err(Error::Config(format!("job has two task groups named {:?}", group.name)));
            }
            group.validate()?;
        }
        Ok(())
    }
}

/// A task group is a set of tasks that must be co-located on the same node.
#[derive(Debug, Clone)]
pub struct TaskGroup {
    /// Group name.
    pub name: String,
    /// Number of instances of this group to run (count).
    pub count: i32,
    /// Individual tasks within the group.
    pub tasks: Vec<Task>,
}

impl TaskGroup {
    /// Validate the task group against Nomad's structural rules.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] for a missing name, negative count, missing or
    /// duplicately-named tasks, or any invalid task.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Config("missing task group name".to_owned()));
        }
        if self.count < 0 {
            return Err(Error::Config(format!(
                "task group {:?} count can't be negative, got {}",
                self.name, self.count
            )));
        }
        if self.tasks.is_empty() {
            return Err(Error::Config(format!("task group {:?} has no tasks", self.name)));
        }
        let mut seen = std::collections::HashSet::new();
        for task in &self.tasks {
            if !seen.insert(task.name.as_str()) {
                return Err(Error::Config(format!("task group {:?} has two tasks named {:?}", self.name, task.name)));
            }
            task.validate()?;
        }
        Ok(())
    }
}

/// A single unit of work within a task group.
#[derive(Debug, Clone)]
pub struct Task {
    /// Task name.
    pub name: String,
    /// The driver to use (e.g., "docker", "exec", "`raw_exec`").
    pub driver: String,
    /// Driver-specific configuration.
    pub config: HashMap<String, serde_json::Value>,
    /// Resource requirements (CPU, memory, network).
    pub resources: Resources,
}

impl Task {
    /// Validate the task against Nomad's structural rules.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] for a missing name or driver, or invalid
    /// resource requirements.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Config("missing task name".to_owned()));
        }
        if self.driver.is_empty() {
            return Err(Error::Config(format!("task {:?} has no driver", self.name)));
        }
        self.resources.validate()
    }
}

/// Resource requirements for a task.
#[derive(Debug, Clone)]
pub struct Resources {
    /// CPU MHz required.
    pub cpu_mhz: i32,
    /// Memory in MB.
    pub memory_mb: i32,
    /// Reserved network bandwidth in Mbps.
    pub network_mbps: i32,
}

impl Default for Resources {
    fn default() -> Self {
        Self { cpu_mhz: 100, memory_mb: 256, network_mbps: 0 }
    }
}

impl Resources {
    /// Validate that the requested resources meet Nomad's minimums.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Config`] if CPU or memory fall below the minimum, or if
    /// network bandwidth is negative.
    pub fn validate(&self) -> Result<()> {
        if self.cpu_mhz < MIN_CPU_MHZ {
            return Err(Error::Config(format!("minimum cpu value is {MIN_CPU_MHZ}, got {}", self.cpu_mhz)));
        }
        if self.memory_mb < MIN_MEMORY_MB {
            return Err(Error::Config(format!("minimum memory value is {MIN_MEMORY_MB}, got {}", self.memory_mb)));
        }
        if self.network_mbps < 0 {
            return Err(Error::Config(format!("network bandwidth can't be negative, got {}", self.network_mbps)));
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;

    fn valid_task() -> Task {
        Task {
            name: "web".to_owned(),
            driver: "docker".to_owned(),
            config: HashMap::new(),
            resources: Resources::default(),
        }
    }

    fn valid_group() -> TaskGroup {
        TaskGroup { name: "cache".to_owned(), count: 1, tasks: vec![valid_task()] }
    }

    fn valid_job() -> Job {
        Job {
            name: "redis".to_owned(),
            meta: HashMap::new(),
            datacenters: vec!["dc1".to_owned()],
            task_groups: vec![valid_group()],
            priority: JOB_DEFAULT_PRIORITY,
        }
    }

    // ---- defaults (characterization) ----

    #[test]
    fn job_default_has_nomad_priority_and_datacenter() {
        let job = Job::default();
        assert_eq!(job.priority, 50);
        assert_eq!(job.datacenters, vec!["dc1".to_owned()]);
        assert!(job.task_groups.is_empty());
    }

    #[test]
    fn resources_default_meets_minimums() {
        assert!(Resources::default().validate().is_ok());
    }

    // ---- Job::validate ----

    #[test]
    fn valid_job_passes() {
        assert!(valid_job().validate().is_ok());
    }

    #[test]
    fn job_rejects_empty_name() {
        let mut job = valid_job();
        job.name = String::new();
        let err = job.validate().unwrap_err().to_string();
        assert!(err.contains("name"), "{err}");
    }

    #[test]
    fn job_rejects_priority_below_min() {
        let mut job = valid_job();
        job.priority = JOB_MIN_PRIORITY - 1;
        assert!(job.validate().unwrap_err().to_string().contains("priority"));
    }

    #[test]
    fn job_rejects_priority_above_max() {
        let mut job = valid_job();
        job.priority = JOB_MAX_PRIORITY + 1;
        assert!(job.validate().unwrap_err().to_string().contains("priority"));
    }

    #[test]
    fn job_accepts_priority_at_bounds() {
        let mut job = valid_job();
        job.priority = JOB_MIN_PRIORITY;
        assert!(job.validate().is_ok());
        job.priority = JOB_MAX_PRIORITY;
        assert!(job.validate().is_ok());
    }

    #[test]
    fn job_rejects_no_datacenters() {
        let mut job = valid_job();
        job.datacenters.clear();
        assert!(job.validate().unwrap_err().to_string().contains("datacenter"));
    }

    #[test]
    fn job_rejects_empty_datacenter_string() {
        let mut job = valid_job();
        job.datacenters = vec![String::new()];
        assert!(job.validate().unwrap_err().to_string().contains("datacenter"));
    }

    #[test]
    fn job_rejects_no_task_groups() {
        let mut job = valid_job();
        job.task_groups.clear();
        assert!(job.validate().unwrap_err().to_string().contains("task group"));
    }

    #[test]
    fn job_rejects_duplicate_group_names() {
        let mut job = valid_job();
        job.task_groups = vec![valid_group(), valid_group()];
        assert!(job.validate().unwrap_err().to_string().contains("two task groups"));
    }

    #[test]
    fn job_propagates_invalid_group() {
        let mut job = valid_job();
        job.task_groups[0].name = String::new();
        assert!(job.validate().unwrap_err().to_string().contains("task group name"));
    }

    // ---- TaskGroup::validate ----

    #[test]
    fn group_rejects_empty_name() {
        let mut group = valid_group();
        group.name = String::new();
        assert!(group.validate().unwrap_err().to_string().contains("name"));
    }

    #[test]
    fn group_rejects_negative_count() {
        let mut group = valid_group();
        group.count = -1;
        assert!(group.validate().unwrap_err().to_string().contains("negative"));
    }

    #[test]
    fn group_rejects_no_tasks() {
        let mut group = valid_group();
        group.tasks.clear();
        assert!(group.validate().unwrap_err().to_string().contains("no tasks"));
    }

    #[test]
    fn group_rejects_duplicate_task_names() {
        let mut group = valid_group();
        group.tasks = vec![valid_task(), valid_task()];
        assert!(group.validate().unwrap_err().to_string().contains("two tasks"));
    }

    #[test]
    fn group_accepts_zero_count() {
        let mut group = valid_group();
        group.count = 0;
        assert!(group.validate().is_ok());
    }

    #[test]
    fn group_propagates_invalid_task() {
        let mut group = valid_group();
        group.tasks[0].name = String::new();
        assert!(group.validate().unwrap_err().to_string().contains("task name"));
    }

    // ---- Task::validate ----

    #[test]
    fn task_rejects_empty_name() {
        let mut task = valid_task();
        task.name = String::new();
        assert!(task.validate().unwrap_err().to_string().contains("name"));
    }

    #[test]
    fn task_rejects_empty_driver() {
        let mut task = valid_task();
        task.driver = String::new();
        assert!(task.validate().unwrap_err().to_string().contains("driver"));
    }

    // ---- Resources::validate ----

    #[test]
    fn resources_reject_cpu_below_min() {
        let res = Resources { cpu_mhz: MIN_CPU_MHZ - 1, ..Resources::default() };
        assert!(res.validate().unwrap_err().to_string().contains("cpu"));
    }

    #[test]
    fn resources_reject_memory_below_min() {
        let res = Resources { memory_mb: MIN_MEMORY_MB - 1, ..Resources::default() };
        assert!(res.validate().unwrap_err().to_string().contains("memory"));
    }

    #[test]
    fn resources_reject_negative_network() {
        let res = Resources { network_mbps: -1, ..Resources::default() };
        assert!(res.validate().unwrap_err().to_string().contains("network"));
    }
}
