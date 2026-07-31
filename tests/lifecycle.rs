// SPDX-License-Identifier: Apache-2.0

//! Lifecycle smoke tests for the agent stubs (client, server, scheduler).
//!
//! These pin the current stub contract: each component constructs from
//! configuration, runs once, and returns `Ok`. They exist so the public
//! lifecycle surface stays covered as real behaviour lands behind it.

use std::time::Duration;

use nomad_rs::client::Client;
use nomad_rs::config::Config;
use nomad_rs::eval_queue::EvalQueue;
use nomad_rs::fsm::Fsm;
use nomad_rs::scheduler::Scheduler;
use nomad_rs::server::Server;

#[tokio::test]
async fn client_constructs_and_runs_to_ok() {
    let mut client = Client::new(Config::default());
    assert!(client.run().await.is_ok());
}

#[tokio::test]
async fn server_constructs_and_runs_to_ok() {
    let mut server = Server::new(Config::default());
    assert!(server.run().await.is_ok());
    server.stop();
}

#[tokio::test]
async fn scheduler_runs_to_ok() {
    let mut scheduler = Scheduler::new();
    let queue = EvalQueue::new();
    let mut fsm = Fsm::new();
    let stop_tx = scheduler.shutdown_handle();

    // run() now loops until stopped; spawn it and stop it remotely.
    let handle = tokio::spawn(async move { scheduler.run(&queue, &mut fsm).await });

    // Let the loop enter its idle wait, then stop.
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(stop_tx.send(true).is_ok());
    let result = handle.await.unwrap();
    assert!(result.is_ok());
}
