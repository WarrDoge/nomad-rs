// SPDX-License-Identifier: Apache-2.0

//! Alloc runner: drives all tasks of one allocation.
//!
//! Owns an allocation, runs each task group member through a
//! [`crate::taskrunner::TaskRunner`], and rolls their states up into an overall
//! client status. Mirrors the subset of upstream Nomad's client `allocrunner`.
//! Behaviour is specified by the tests and is unimplemented.

use crate::alloc::{Allocation, ClientStatus};
use crate::driver::TaskState;
use crate::error::Result;
use crate::jobspec::Task;
use crate::taskrunner::TaskRunner;

/// Drives one allocation's tasks on a node.
#[derive(Debug)]
pub struct AllocRunner {
    /// The allocation being run.
    alloc: Allocation,
    /// One runner per task in the allocation's group.
    runners: Vec<TaskRunner>,
}

impl AllocRunner {
    /// Create a runner for `alloc` driving `tasks`.
    #[must_use]
    pub fn new(alloc: Allocation, tasks: Vec<Task>) -> Self {
        Self { alloc, runners: tasks.into_iter().map(TaskRunner::new).collect() }
    }

    /// Overall client status of the allocation.
    ///
    /// ponytail: returns the alloc's recorded status, set by `run`/`destroy`.
    /// Roll this up live from `task_states()` (any Failed → Failed, all Exited →
    /// Complete) once restart/health supervision is wired.
    #[must_use]
    pub fn status(&self) -> ClientStatus {
        self.alloc.client_status
    }

    /// Poll each task runner for its current driver state.
    ///
    /// # Errors
    ///
    /// Returns an error if any task cannot be inspected.
    pub fn task_states(&mut self) -> Result<Vec<TaskState>> {
        self.runners.iter_mut().map(TaskRunner::poll).collect()
    }

    /// Start running the allocation's tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if a task fails to start.
    pub fn run(&mut self) -> Result<()> {
        for runner in &mut self.runners {
            runner.start()?;
        }
        self.alloc.client_status = ClientStatus::Running;
        Ok(())
    }

    /// Stop and clean up the allocation.
    ///
    /// # Errors
    ///
    /// Returns an error if teardown fails.
    pub fn destroy(&mut self) -> Result<()> {
        for runner in &mut self.runners {
            runner.stop()?;
        }
        self.alloc.client_status = ClientStatus::Complete;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::alloc::DesiredStatus;
    use crate::jobspec::Resources;
    use std::collections::HashMap;

    fn alloc() -> Allocation {
        Allocation {
            id: "a1".to_owned(),
            eval_id: "e1".to_owned(),
            node_id: "n1".to_owned(),
            job_id: "redis".to_owned(),
            task_group: "cache".to_owned(),
            desired_status: DesiredStatus::Run,
            client_status: ClientStatus::Pending,
            resources: Resources::default(),
        }
    }

    fn runner() -> AllocRunner {
        AllocRunner::new(alloc(), vec![])
    }

    fn sleep_task() -> Task {
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("sleep"));
        config.insert("args".to_owned(), serde_json::json!(["30"]));
        Task { name: "web".to_owned(), driver: "exec".to_owned(), config, resources: Resources::default() }
    }

    #[test]
    fn new_runner_is_pending() {
        assert_eq!(runner().status(), ClientStatus::Pending);
    }

    #[test]
    fn destroy_succeeds() {
        assert!(runner().destroy().is_ok());
    }

    #[test]
    fn run_starts_every_task_then_destroy_stops_them() {
        let mut r = AllocRunner::new(alloc(), vec![sleep_task(), sleep_task()]);
        r.run().unwrap();
        assert_eq!(r.status(), ClientStatus::Running);
        assert!(r.task_states().unwrap().iter().all(|s| *s == TaskState::Running));
        r.destroy().unwrap();
        assert_eq!(r.status(), ClientStatus::Complete);
        assert!(r.task_states().unwrap().iter().all(|s| *s == TaskState::Exited));
    }
}
