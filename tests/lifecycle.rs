// SPDX-License-Identifier: Apache-2.0

//! Lifecycle smoke tests for the agent stubs (client, server, scheduler).
//!
//! These pin the current stub contract: each component constructs from
//! configuration, runs once, and returns `Ok`. They exist so the public
//! lifecycle surface stays covered as real behaviour lands behind it.

use nomad_rs::client::Client;
use nomad_rs::config::Config;
use nomad_rs::scheduler::Scheduler;
use nomad_rs::server::Server;
use nomad_rs::util::block_on;

#[test]
fn client_constructs_and_runs_to_ok() {
    let mut client = Client::new(Config::default());
    assert!(block_on(client.run()).is_ok());
}

#[test]
fn server_constructs_and_runs_to_ok() {
    let mut server = Server::new(Config::default());
    assert!(block_on(server.run()).is_ok());
}

#[test]
fn scheduler_runs_to_ok() {
    let mut scheduler = Scheduler::new();
    assert!(block_on(scheduler.run()).is_ok());
}
