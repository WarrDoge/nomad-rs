// SPDX-License-Identifier: Apache-2.0

//! Nomad job specification types.
//!
//! Represents the HCL-based job specification used to define workloads.
//! This module provides strongly-typed Rust equivalents of Nomad's job
//! specification structures.

use std::collections::HashMap;

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
            priority: 50,
        }
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

/// A single unit of work within a task group.
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
