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

    /// Poll the tasks and roll their live states up into the alloc's client
    /// status: once every task has exited the alloc is [`ClientStatus::Complete`].
    /// Terminal statuses ([`ClientStatus::Complete`]/[`ClientStatus::Failed`]) are
    /// sticky. A supervisor loop calls this periodically to observe tasks that
    /// exit on their own.
    ///
    /// ponytail: no Failed rollup — the driver's `poll` reports `Exited` for both
    /// clean and crash exits, so a self-exiting task rolls up as Complete. Wire
    /// exit-code/restart supervision in to distinguish Failed.
    ///
    /// # Errors
    ///
    /// Returns an error if any task cannot be polled.
    pub fn refresh_status(&mut self) -> Result<ClientStatus> {
        if matches!(self.alloc.client_status, ClientStatus::Complete | ClientStatus::Failed) {
            return Ok(self.alloc.client_status);
        }
        let states = self.task_states()?;
        if !states.is_empty() && states.iter().all(|s| *s == TaskState::Exited) {
            self.alloc.client_status = ClientStatus::Complete;
        }
        Ok(self.alloc.client_status)
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
        for idx in 0..self.runners.len() {
            if let Err(err) = self.runners[idx].start() {
                // Roll back already-started tasks so a partial failure doesn't
                // leave orphaned processes, and mark the alloc terminal.
                for started in self.runners[..idx].iter_mut().rev() {
                    let _ = started.stop();
                }
                self.alloc.client_status = ClientStatus::Failed;
                return Err(err);
            }
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
        // Try to stop every task even if one fails; return the first error.
        let mut first_err = None;
        for runner in &mut self.runners {
            if let Err(err) = runner.stop() {
                first_err.get_or_insert(err);
            }
        }
        self.alloc.client_status = ClientStatus::Complete;
        first_err.map_or(Ok(()), Err)
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

    fn bad_task() -> Task {
        // exec driver, no command → start_task errors.
        Task {
            name: "bad".to_owned(),
            driver: "exec".to_owned(),
            config: HashMap::new(),
            resources: Resources::default(),
        }
    }

    #[test]
    fn run_rolls_back_started_tasks_on_later_failure() {
        let mut r = AllocRunner::new(alloc(), vec![sleep_task(), bad_task()]);
        assert!(r.run().is_err());
        assert_eq!(r.status(), ClientStatus::Failed);
        // The first task was started then rolled back → not left running.
        assert_eq!(r.task_states().unwrap()[0], TaskState::Exited);
    }

    fn quick_task() -> Task {
        // exits 0 immediately
        let mut config = HashMap::new();
        config.insert("command".to_owned(), serde_json::json!("true"));
        Task { name: "q".to_owned(), driver: "exec".to_owned(), config, resources: Resources::default() }
    }

    #[test]
    fn refresh_status_rolls_up_to_complete_when_all_tasks_exit() {
        let mut r = AllocRunner::new(alloc(), vec![quick_task()]);
        r.run().unwrap();
        assert_eq!(r.status(), ClientStatus::Running);
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert_eq!(r.refresh_status().unwrap(), ClientStatus::Complete);
        assert_eq!(r.status(), ClientStatus::Complete, "rollup persisted on the alloc");
    }

    #[test]
    fn refresh_status_stays_running_while_a_task_lives() {
        let mut r = AllocRunner::new(alloc(), vec![sleep_task()]);
        r.run().unwrap();
        assert_eq!(r.refresh_status().unwrap(), ClientStatus::Running);
        r.destroy().unwrap();
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
