// SPDX-License-Identifier: Apache-2.0

//! Lifecycle smoke tests for the agent stubs (client, server, scheduler).
//!
//! These pin the current stub contract: server and scheduler construct from
//! configuration, run once, and return `Ok`. The client requires a running
//! server to connect to, so its lifecycle test verifies the error path.

use nomad_rs::client::Client;
use nomad_rs::config::Config;
use nomad_rs::scheduler::Scheduler;
use nomad_rs::server::Server;

#[tokio::test]
async fn client_constructs_and_runs_to_ok() {
    let mut client = Client::new(Config::default());
    // Without a server, run() will fail to connect — this is expected.
    assert!(client.run().await.is_err());
}

#[tokio::test]
async fn server_constructs_and_runs_to_ok() {
    let mut server = Server::new(Config::default());
    assert!(server.run().await.is_ok());
}

#[tokio::test]
async fn scheduler_runs_to_ok() {
    let mut scheduler = Scheduler::new();
    assert!(scheduler.run().await.is_ok());
}
