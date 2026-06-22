// SPDX-License-Identifier: Apache-2.0

//! Lifecycle smoke tests for the agent stubs (client, server, scheduler).
//!
//! These pin the current stub contract: each component constructs from
//! configuration, runs once, and returns `Ok`. They exist so the public
//! lifecycle surface stays covered as real behaviour lands behind it.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use nomad_rs::client::Client;
use nomad_rs::config::Config;
use nomad_rs::scheduler::Scheduler;
use nomad_rs::server::Server;

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut pinned = pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        if let Poll::Ready(val) = pinned.as_mut().poll(&mut cx) {
            return val;
        }
    }
}

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
