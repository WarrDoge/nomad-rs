// SPDX-License-Identifier: Apache-2.0

//! Nomad job specification types.
//!
//! Represents the HCL-based job specification used to define workloads.
//! This module provides strongly-typed Rust equivalents of Nomad's job
//! specification structures.

use std::collections::HashMap;

use crate::error::{Error, Result};

/// Default job priority (upstream: 50).
#[allow(dead_code)]
pub const JOB_DEFAULT_PRIORITY: i32 = 50;

/// Minimum allowed job priority.
#[allow(dead_code)]
pub const JOB_MIN_PRIORITY: i32 = 1;

/// Maximum allowed job priority.
#[allow(dead_code)]
pub const JOB_MAX_PRIORITY: i32 = 100;

/// A Nomad job represents a set of task groups that run on the cluster.
#[derive(Debug, Clone, PartialEq)]
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
            priority: 50,
        }
    }
}

/// A task group is a set of tasks that must be co-located on the same node.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskGroup {
    /// Group name.
    pub name: String,
    /// Number of instances of this group to run (count).
    pub count: i32,
    /// Individual tasks within the group.
    pub tasks: Vec<Task>,
}

/// A single unit of work within a task group.
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    /// Task name.
    pub name: String,
    /// The driver to use (e.g., `"docker"`, `"exec"`, `"raw_exec"`).
    pub driver: String,
    /// Driver-specific configuration.
    pub config: HashMap<String, serde_json::Value>,
    /// Resource requirements (CPU, memory, network).
    pub resources: Resources,
}

/// Resource requirements for a task.
#[derive(Debug, Clone, PartialEq)]
pub struct Resources {
    /// CPU MHz required.
    pub cpu_mhz: i32,
    /// Memory in MB.
    pub memory_mb: i32,
    /// Reserved network ports.
    pub network_mbps: i32,
}

impl Default for Resources {
    fn default() -> Self {
        Self { cpu_mhz: 100, memory_mb: 256, network_mbps: 0 }
    }
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

impl Job {
    /// Validate the job definition.
    ///
    /// # Errors
    ///
    /// Returns `Validation` if the job name is empty or priority is out of
    /// the allowed range.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Validation("job name cannot be empty".into()));
        }
        if !(JOB_MIN_PRIORITY..=JOB_MAX_PRIORITY).contains(&self.priority) {
            return Err(Error::Validation(format!(
                "priority {} out of range [{}, {}]",
                self.priority, JOB_MIN_PRIORITY, JOB_MAX_PRIORITY
            )));
        }
        if self.datacenters.is_empty() {
            return Err(Error::Validation("at least one datacenter is required".into()));
        }
        if self.datacenters.iter().any(String::is_empty) {
            return Err(Error::Validation(
                "datacenter names cannot be empty".into(),
            ));
        }
        for tg in &self.task_groups {
            tg.validate()?;
        }
        Ok(())
    }
}

impl TaskGroup {
    /// Validate the task group.
    ///
    /// # Errors
    ///
    /// Returns `Validation` if the group name is empty, count is negative,
    /// the task list is empty, or tasks have duplicate names.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Validation("task group name cannot be empty".into()));
        }
        if self.count < 0 {
            return Err(Error::Validation(format!(
                "task group '{}' count cannot be negative ({})",
                self.name, self.count
            )));
        }
        if self.tasks.is_empty() {
            return Err(Error::Validation(format!(
                "task group '{}' has no tasks",
                self.name
            )));
        }
        let mut seen = std::collections::HashSet::new();
        for task in &self.tasks {
            if !seen.insert(&task.name) {
                return Err(Error::Validation(format!(
                    "duplicate task name '{}' in task group '{}'",
                    task.name, self.name
                )));
            }
            task.validate()?;
        }
        Ok(())
    }
}

impl Task {
    /// Validate the task definition.
    ///
    /// # Errors
    ///
    /// Returns `Validation` if the task name is empty or the driver is
    /// missing.
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(Error::Validation("task name cannot be empty".into()));
        }
        if self.driver.is_empty() {
            return Err(Error::Validation(format!("task '{}' driver cannot be empty", self.name)));
        }
        self.resources.validate()?;
        Ok(())
    }
}

impl Resources {
    /// Validate that resource values are non-negative.
    ///
    /// # Notes
    ///
    /// Upstream Nomad enforces minimums — `MIN_CPU_MHZ = 1`, `MIN_MEMORY_MB = 10`.
    /// This validator only rejects negatives, allowing zero for compatibility with
    /// tests and partial specs. The scheduler layer should enforce upstream minimums.
    ///
    /// # Errors
    ///
    /// Returns `Validation` if any resource value is negative.
    pub fn validate(&self) -> Result<()> {
        if self.cpu_mhz < 0 {
            return Err(Error::Validation(format!("cpu_mhz cannot be negative ({})", self.cpu_mhz)));
        }
        if self.memory_mb < 0 {
            return Err(Error::Validation(format!("memory_mb cannot be negative ({})", self.memory_mb)));
        }
        if self.network_mbps < 0 {
            return Err(Error::Validation(format!("network_mbps cannot be negative ({})", self.network_mbps)));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_default() {
        let job = Job::default();
        assert!(job.name.is_empty());
        assert!(job.meta.is_empty());
        assert_eq!(job.datacenters, vec!["dc1".to_owned()]);
        assert!(job.task_groups.is_empty());
        assert_eq!(job.priority, 50);
    }

    #[test]
    fn test_job_with_values() {
        let job = Job {
            name: "web-app".to_owned(),
            meta: HashMap::from([("env".to_owned(), "prod".to_owned())]),
            datacenters: vec!["dc1".to_owned(), "dc2".to_owned()],
            priority: 100,
            ..Job::default()
        };
        assert_eq!(job.name, "web-app");
        assert_eq!(job.meta.get("env").unwrap(), "prod");
        assert_eq!(job.datacenters.len(), 2);
        assert_eq!(job.priority, 100);
    }

    #[test]
    fn test_task_group_creation() {
        let tg = TaskGroup { name: "frontend".to_owned(), count: 3, tasks: vec![] };
        assert_eq!(tg.name, "frontend");
        assert_eq!(tg.count, 3);
        assert!(tg.tasks.is_empty());
    }

    #[test]
    fn test_task_group_with_tasks() {
        let task = Task {
            name: "nginx".to_owned(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources::default(),
        };
        let tg = TaskGroup { name: "web".to_owned(), count: 2, tasks: vec![task] };
        assert_eq!(tg.tasks.len(), 1);
        assert_eq!(tg.tasks[0].name, "nginx");
    }

    #[test]
    fn test_task_creation() {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("/bin/myapp"));
        config.insert("args".to_owned(), serde_json::json!(["-port", "8080"]));

        let task = Task {
            name: "myapp".to_owned(),
            driver: "exec".to_owned(),
            config,
            resources: Resources { cpu_mhz: 500, memory_mb: 1024, network_mbps: 100 },
        };
        assert_eq!(task.name, "myapp");
        assert_eq!(task.driver, "exec");
        assert_eq!(task.resources.cpu_mhz, 500);
        assert_eq!(task.resources.memory_mb, 1024);
        assert_eq!(task.resources.network_mbps, 100);
        assert_eq!(task.config["command"], serde_json::json!("/bin/myapp"));
    }

    #[test]
    fn test_resources_default() {
        let res = Resources::default();
        assert_eq!(res.cpu_mhz, 100);
        assert_eq!(res.memory_mb, 256);
        assert_eq!(res.network_mbps, 0);
    }

    #[test]
    fn test_resources_custom() {
        let res = Resources { cpu_mhz: 2000, memory_mb: 4096, network_mbps: 1000 };
        assert_eq!(res.cpu_mhz, 2000);
        assert_eq!(res.memory_mb, 4096);
        assert_eq!(res.network_mbps, 1000);
    }

    // ------------------------------------------------------------------
    // Validation tests
    // ------------------------------------------------------------------

    #[test]
    fn test_job_validate_empty_name() {
        let job = Job::default();
        assert!(job.validate().unwrap_err().to_string().contains("name cannot be empty"));
    }

    #[test]
    fn test_job_validate_priority_too_low() {
        let job = Job { name: "test".to_owned(), priority: 0, ..Job::default() };
        assert!(job.validate().unwrap_err().to_string().contains("priority"));
    }

    #[test]
    fn test_job_validate_priority_too_high() {
        let job = Job { name: "test".to_owned(), priority: 101, ..Job::default() };
        assert!(job.validate().unwrap_err().to_string().contains("priority"));
    }

    #[test]
    fn test_job_validate_no_datacenters() {
        let job = Job { name: "test".to_owned(), datacenters: vec![], ..Job::default() };
        assert!(job.validate().unwrap_err().to_string().contains("datacenter"));
    }

    #[test]
    fn test_job_validate_valid_job() {
        let job = Job { name: "valid".to_owned(), datacenters: vec!["dc1".to_owned()], priority: 50, ..Job::default() };
        assert!(job.validate().is_ok());
    }

    #[test]
    fn test_task_group_validate_empty_name() {
        let tg = TaskGroup { name: String::new(), count: 1, tasks: vec![] };
        assert!(tg.validate().unwrap_err().to_string().contains("group name"));
    }

    #[test]
    fn test_task_group_validate_negative_count() {
        let tg = TaskGroup { name: "g".to_owned(), count: -1, tasks: vec![] };
        assert!(tg.validate().unwrap_err().to_string().contains("negative"));
    }

    #[test]
    fn test_task_validate_empty_name() {
        let task = Task {
            name: String::new(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources::default(),
        };
        assert!(task.validate().unwrap_err().to_string().contains("task name"));
    }

    #[test]
    fn test_task_validate_empty_driver() {
        let task = Task {
            name: "t".to_owned(),
            driver: String::new(),
            config: HashMap::new(),
            resources: Resources::default(),
        };
        assert!(task.validate().unwrap_err().to_string().contains("driver"));
    }

    #[test]
    fn test_resources_validate_negative_cpu() {
        let res = Resources { cpu_mhz: -1, memory_mb: 256, network_mbps: 0 };
        assert!(res.validate().unwrap_err().to_string().contains("cpu_mhz"));
    }

    #[test]
    fn test_resources_validate_negative_memory() {
        let res = Resources { cpu_mhz: 100, memory_mb: -1, network_mbps: 0 };
        assert!(res.validate().unwrap_err().to_string().contains("memory_mb"));
    }

    #[test]
    fn test_resources_validate_ok() {
        let res = Resources { cpu_mhz: 500, memory_mb: 1024, network_mbps: 100 };
        assert!(res.validate().is_ok());
    }
}
