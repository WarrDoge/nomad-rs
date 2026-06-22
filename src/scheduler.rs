// SPDX-License-Identifier: Apache-2.0

//! Nomad scheduler — evaluation, ranking, and placement logic.
//!
//! The scheduler watches for pending evaluations, calculates placement
//! scores across candidate nodes, and produces allocation plans.

use crate::error::Result;

/// The core scheduler responsible for placing tasks onto nodes.
#[derive(Debug)]
pub struct Scheduler {
    /// Whether the scheduler is currently running.
    running: bool,
}

impl Scheduler {
    /// Create a new scheduler instance.
    #[must_use]
    pub fn new() -> Self {
        Self { running: false }
    }

    /// Run the scheduler evaluation loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the scheduling loop encounters a fatal error.
    pub async fn run(&mut self) -> Result<()> {
        self.running = true;
        tracing::info!("scheduler starting");
        // TODO: implement the bin-packing scheduler loop
        self.running = false;
        Ok(())
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
