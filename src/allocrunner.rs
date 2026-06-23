// SPDX-License-Identifier: Apache-2.0

//! Alloc runner: drives all tasks of one allocation.
//!
//! Owns an allocation, runs each task group member through a
//! [`crate::taskrunner::TaskRunner`], and rolls their states up into an overall
//! client status. Mirrors the subset of upstream Nomad's client `allocrunner`.
//! Behaviour is specified by the tests and is unimplemented.

use crate::alloc::{Allocation, ClientStatus};
use crate::error::Result;

/// Drives one allocation's tasks on a node.
#[derive(Debug)]
pub struct AllocRunner {
    /// The allocation being run.
    #[allow(dead_code, reason = "read once the runner lifecycle is implemented")]
    alloc: Allocation,
}

impl AllocRunner {
    /// Create a runner for `alloc`.
    #[must_use]
    pub fn new(alloc: Allocation) -> Self {
        Self { alloc }
    }

    /// Overall client status, rolled up from the task runners.
    #[must_use]
    pub fn status(&self) -> ClientStatus {
        todo!("roll task states up into a ClientStatus")
    }

    /// Start running the allocation's tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if a task fails to start.
    pub fn run(&mut self) -> Result<()> {
        todo!("start a TaskRunner per task and supervise them")
    }

    /// Stop and clean up the allocation.
    ///
    /// # Errors
    ///
    /// Returns an error if teardown fails.
    pub fn destroy(&mut self) -> Result<()> {
        todo!("stop all task runners and remove the alloc dir")
    }
}

#[cfg(test)]
#[allow(clippy::missing_docs_in_private_items, clippy::wildcard_imports, reason = "conventional inline test module")]
mod tests {
    use super::*;
    use crate::alloc::DesiredStatus;
    use crate::jobspec::Resources;

    fn runner() -> AllocRunner {
        AllocRunner::new(Allocation {
            id: "a1".to_owned(),
            eval_id: "e1".to_owned(),
            node_id: "n1".to_owned(),
            job_id: "redis".to_owned(),
            task_group: "cache".to_owned(),
            desired_status: DesiredStatus::Run,
            client_status: ClientStatus::Pending,
            resources: Resources::default(),
        })
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn new_runner_is_pending() {
        assert_eq!(runner().status(), ClientStatus::Pending);
    }

    #[test]
    #[ignore = "red spec: implement to unignore"]
    fn destroy_succeeds() {
        assert!(runner().destroy().is_ok());
    }
}
