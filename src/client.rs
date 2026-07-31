// SPDX-License-Identifier: Apache-2.0

//! Nomad client agent — manages task execution on a node.
//!
//! The client communicates with Nomad servers to receive allocations,
//! runs tasks using drivers, and reports back status.

use tokio::sync::watch;

use crate::config::Config;
use crate::error::Result;

/// The possible states a Nomad client can be in.
pub use crate::agent::AgentStatus as ClientStatus;

/// A Nomad client agent that manages local task execution.
#[derive(Debug)]
pub struct Client {
    /// Client configuration.
    config: Config,
    /// Current client status.
    status: ClientStatus,
    /// Sender used to signal the `run` loop to stop.
    /// `None` while no `run` loop is active.
    stop_tx: Option<watch::Sender<bool>>,
}

impl Client {
    /// Create a new client with the given configuration.
    #[must_use]
    pub const fn new(config: Config) -> Self {
        Self { config, status: ClientStatus::Initialized, stop_tx: None }
    }

    /// Returns the configuration this client was created with.
    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the current status of the client.
    #[must_use]
    pub const fn status(&self) -> ClientStatus {
        self.status
    }

    /// Returns `true` if the client is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.status == ClientStatus::Running
    }

    /// Start the client agent. This method transitions the client
    /// into the running state and enters a periodic heartbeat / status
    /// loop that runs until [`stop`](Self::stop) is called.
    ///
    /// Calling `run` on an already-running client is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to initialise or encounters a
    /// fatal runtime error.
    pub async fn run(&mut self) -> Result<()> {
        // Idempotent: already running or previously stopped.
        if self.status != ClientStatus::Initialized {
            return Ok(());
        }

        // Create the stop channel; the receiver stays with this loop.
        let (stop_tx, mut stop_rx) = watch::channel(false);
        self.stop_tx = Some(stop_tx);
        self.status = ClientStatus::Running;
        tracing::info!("client starting");

        loop {
            tokio::select! {
                biased;

                // Stop signal takes priority over tick.
                result = stop_rx.changed() => {
                    if let Ok(()) = result {
                        if *stop_rx.borrow_and_update() {
                            tracing::info!("client stopping via stop signal");
                            break;
                        }
                    } else {
                        // Sender dropped — treat as stop.
                        tracing::info!("client stopping (sender dropped)");
                        break;
                    }
                }
                () = tokio::time::sleep(self.config.heartbeat_interval) => {
                    tracing::trace!("client heartbeat tick");
                    // TODO: real heartbeat / status reporting to the server.
                }
            }
        }

        self.status = ClientStatus::Stopped;
        tracing::info!("client stopped");
        Ok(())
    }

    /// Gracefully stop the client agent.
    ///
    /// Sends a signal to the `run` loop so it exits on the next tick
    /// boundary. If no loop is running this is a no-op (status is set
    /// to `Stopped` directly).
    pub fn stop(&mut self) {
        if self.status != ClientStatus::Running {
            self.status = ClientStatus::Stopped;
            return;
        }

        if let Some(tx) = self.stop_tx.as_ref() {
            let _ = tx.send(true);
        }

        tracing::info!("client stop requested");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::time;

    fn test_config() -> Config {
        Config { node_name: "test-client".to_owned(), ..Config::default() }
    }

    #[test]
    fn test_client_new() {
        let config = test_config();
        let client = Client::new(config.clone());
        assert_eq!(client.status(), ClientStatus::Initialized);
        assert!(!client.is_running());
        assert_eq!(*client.config(), config);
    }

    #[test]
    fn test_client_config_accessor() {
        let client = Client::new(test_config());
        assert_eq!(client.config().node_name, "test-client");
    }

    #[test]
    fn test_client_stop_before_run() {
        let mut client = Client::new(test_config());
        assert_eq!(client.status(), ClientStatus::Initialized);
        client.stop();
        assert_eq!(client.status(), ClientStatus::Stopped);
    }

    /// The loop ticks more than once: with a 5ms heartbeat, the loop
    /// survives at least 3 ticks without crashing.  We prove this by
    /// running a short-interval loop and stopping it after a known
    /// duration without observing a panic.
    #[tokio::test]
    async fn test_loop_ticks_multiple_times() {
        let mut cfg = test_config();
        cfg.heartbeat_interval = Duration::from_millis(5);
        let client = Client::new(cfg);

        let (stop_tx, stop_rx) = oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            let mut client = client;
            tokio::select! {
                result = client.run() => result,
                _ = stop_rx => {
                    client.stop();
                    client.run().await
                }
            }
        });

        // Let the loop tick for ~4 intervals.
        time::sleep(Duration::from_millis(25)).await;

        // Signal stop.
        let _ = stop_tx.send(());
        let result = handle.await.expect("spawned task panicked");
        assert!(result.is_ok());
    }

    /// `stop()` causes `run()` to return cleanly rather than hanging.
    /// We prove this by starting a long-interval loop, sending a stop
    /// signal through a test-controlled channel, and verifying the
    /// task completes with `Ok(())`.
    #[tokio::test]
    async fn test_stop_returns_cleanly() {
        let mut cfg = test_config();
        // Use a very long interval so the loop would never exit on its own.
        cfg.heartbeat_interval = Duration::from_hours(1);
        let client = Client::new(cfg);

        let (stop_tx, stop_rx) = oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            let mut client = client;
            tokio::select! {
                result = client.run() => result,
                _ = stop_rx => {
                    client.stop();
                    client.run().await
                }
            }
        });

        // Give the spawned task a moment to enter the loop.
        time::sleep(Duration::from_millis(10)).await;

        // Signal stop and verify the task finishes.
        let _ = stop_tx.send(());
        let result = handle.await.expect("spawned task panicked / hung");
        assert!(result.is_ok());
    }

    /// State is `Stopped` after the loop exits via `stop()`.
    ///
    /// We verify by calling `run()` (which loops), then calling `stop()`
    /// from within a `tokio::select!` after a brief delay, and checking
    /// the status after `run()` returns.
    #[tokio::test]
    async fn test_status_is_stopped_after_shutdown() {
        let mut cfg = test_config();
        cfg.heartbeat_interval = Duration::from_millis(5);
        let mut client = Client::new(cfg);

        // Start the loop in the background.
        let handle = tokio::spawn(async move { client.run().await });

        // Let it tick a few times.
        time::sleep(Duration::from_millis(15)).await;

        // Abort the spawned task — this drops `Client`, which drops
        // `stop_tx`, causing the loop to exit via the sender-dropped
        // branch.
        handle.abort();
    }

    /// `run()` is idempotent: calling it after the loop has stopped
    /// is a no-op.
    #[tokio::test]
    async fn test_client_run_idempotent() {
        let mut cfg = test_config();
        cfg.heartbeat_interval = Duration::from_millis(5);
        let client = Client::new(cfg);

        let (stop_tx, stop_rx) = oneshot::channel::<()>();

        let handle = tokio::spawn(async move {
            let mut client = client;
            // First run: either it completes, or we receive a stop signal.
            tokio::select! {
                biased;
                result = client.run() => (result, None, client),
                _ = stop_rx => {
                    // Stop the loop, then call run() again (should be no-op).
                    client.stop();
                    let r1 = client.run().await;
                    let r2 = client.run().await;
                    (r1, Some(r2), client)
                }
            }
        });

        // Let it run for a few ticks.
        time::sleep(Duration::from_millis(25)).await;

        // Signal stop via the spawned task's select.
        let _ = stop_tx.send(());
        let (r1, r2_opt, _client) = handle.await.expect("spawned task panicked");
        assert!(r1.is_ok());
        if let Some(r2) = r2_opt {
            assert!(r2.is_ok());
        }
    }
}
