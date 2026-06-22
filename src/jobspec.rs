// SPDX-License-Identifier: Apache-2.0

//! Nomad job specification types.
//!
//! Represents the HCL-based job specification used to define workloads.
//! This module provides strongly-typed Rust equivalents of Nomad's job
//! specification structures.

use std::collections::HashMap;

/// Default job priority (upstream: 50).
pub const JOB_DEFAULT_PRIORITY: i32 = 50;

/// Minimum allowed job priority.
pub const JOB_MIN_PRIORITY: i32 = 1;

/// Maximum allowed job priority.
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
    /// The driver to use (e.g., "docker", "exec", "raw_exec").
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
        Self {
            cpu_mhz: 100,
            memory_mb: 256,
            network_mbps: 0,
        }
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
        let tg = TaskGroup {
            name: "frontend".to_owned(),
            count: 3,
            tasks: vec![],
        };
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
        let tg = TaskGroup {
            name: "web".to_owned(),
            count: 2,
            tasks: vec![task],
        };
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
            resources: Resources {
                cpu_mhz: 500,
                memory_mb: 1024,
                network_mbps: 100,
            },
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
        let res = Resources {
            cpu_mhz: 2000,
            memory_mb: 4096,
            network_mbps: 1000,
        };
        assert_eq!(res.cpu_mhz, 2000);
        assert_eq!(res.memory_mb, 4096);
        assert_eq!(res.network_mbps, 1000);
    }

    #[test]
    fn test_job_equality() {
        let a = Job {
            name: "test".to_owned(),
            ..Job::default()
        };
        let b = Job {
            name: "test".to_owned(),
            ..Job::default()
        };
        assert_eq!(a, b);

        let c = Job {
            name: "other".to_owned(),
            ..Job::default()
        };
        assert_ne!(a, c);
    }

    #[test]
    fn test_task_group_equality() {
        let a = TaskGroup {
            name: "web".to_owned(),
            count: 2,
            tasks: vec![],
        };
        let b = TaskGroup {
            name: "web".to_owned(),
            count: 2,
            tasks: vec![],
        };
        assert_eq!(a, b);

        let c = TaskGroup {
            name: "web".to_owned(),
            count: 3,
            tasks: vec![],
        };
        assert_ne!(a, c);
    }

    #[test]
    fn test_task_equality() {
        let a = Task {
            name: "app".to_owned(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources::default(),
        };
        let b = Task {
            name: "app".to_owned(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources::default(),
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_job_with_multiple_task_groups() {
        let job = Job {
            name: "full-stack".to_owned(),
            task_groups: vec![
                TaskGroup {
                    name: "backend".to_owned(),
                    count: 2,
                    tasks: vec![Task {
                        name: "api".to_owned(),
                        driver: "docker".to_owned(),
                        config: HashMap::from([("image".to_owned(), serde_json::json!("myapp:latest"))]),
                        resources: Resources {
                            cpu_mhz: 1000,
                            memory_mb: 512,
                            network_mbps: 100,
                        },
                    }],
                },
                TaskGroup {
                    name: "frontend".to_owned(),
                    count: 1,
                    tasks: vec![Task {
                        name: "web".to_owned(),
                        driver: "docker".to_owned(),
                        config: HashMap::from([("image".to_owned(), serde_json::json!("nginx:latest"))]),
                        resources: Resources {
                            cpu_mhz: 500,
                            memory_mb: 256,
                            network_mbps: 50,
                        },
                    }],
                },
            ],
            ..Job::default()
        };
        assert_eq!(job.task_groups.len(), 2);
        assert_eq!(job.task_groups[0].name, "backend");
        assert_eq!(job.task_groups[1].name, "frontend");
    }
}
