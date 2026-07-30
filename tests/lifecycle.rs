// SPDX-License-Identifier: Apache-2.0

//! Lifecycle smoke tests for the agent stubs (client, server, scheduler).
//!
//! These pin the current stub contract: each component constructs from
//! configuration, runs once, and returns `Ok`. They exist so the public
//! lifecycle surface stays covered as real behaviour lands behind it.
//!
//! The client's `run()` now enters a blocking event loop, so the client
//! test spawns it in a background task and closes the alloc channel to
//! make it exit cleanly.

use nomad_rs::client::Client;
use nomad_rs::config::Config;
use nomad_rs::scheduler::Scheduler;
use nomad_rs::server::Server;

#[tokio::test]
async fn client_constructs_and_runs_to_ok() {
    let mut client = Client::new(Config::default());
    let allocator = client.allocator();
    let handle = tokio::spawn(async move { client.run().await });
    // Dropping the allocator closes the channel, causing the event loop
    // to shut down and return Ok(()).
    drop(allocator);
    assert!(handle.await.unwrap().is_ok());
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
