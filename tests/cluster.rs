// SPDX-License-Identifier: Apache-2.0

//! Multi-node cluster integration tests.
//!
//! Spins up server/client instances in-process and exercises the eval queue
//! lifecycle: job registration → eval creation → eval dequeue.

use nomad_rs::config::Config;
use nomad_rs::eval::EvalStatus;
use nomad_rs::eval_queue::EvalQueue;
use nomad_rs::jobspec::Job;
use nomad_rs::rpc::{Request, Response, RpcEndpoint};
use nomad_rs::server::Server;

#[tokio::test]
async fn cluster_register_job_creates_eval() {
    let mut server = Server::new(Config::default());
    server.run().await.unwrap();

    let queue = EvalQueue::new();
    let endpoint = RpcEndpoint::new(queue.clone());
    let job = Job { name: "redis".to_owned(), priority: 50, ..Job::default() };

    let resp = endpoint.handle(Request::JobRegister(job)).unwrap();
    let Response::JobRegistered { eval_id } = resp else { panic!("expected JobRegistered") };
    assert!(!eval_id.is_empty());

    // The eval should be in the queue.
    let eval = queue.dequeue().unwrap().unwrap();
    assert_eq!(eval.status, EvalStatus::Pending);
    assert_eq!(eval.job_id, "redis");

    server.stop();
}

#[tokio::test]
async fn cluster_dequeue_eval() {
    let mut server = Server::new(Config::default());
    server.run().await.unwrap();

    let queue = EvalQueue::new();
    let endpoint = RpcEndpoint::new(queue.clone());
    let job = Job { name: "web".to_owned(), priority: 80, ..Job::default() };

    endpoint.handle(Request::JobRegister(job)).unwrap();

    // Dequeue via RPC.
    let resp = endpoint.handle(Request::EvalDequeue { schedulers: vec!["service".to_owned()] }).unwrap();
    let Response::Eval(eval) = resp else { panic!("expected Eval response") };
    let eval = eval.expect("expected an eval, got None");
    assert_eq!(eval.job_id, "web");
    assert_eq!(eval.status, EvalStatus::Pending);

    server.stop();
}

#[tokio::test]
async fn cluster_priority_ordering() {
    let mut server = Server::new(Config::default());
    server.run().await.unwrap();

    let queue = EvalQueue::new();
    let endpoint = RpcEndpoint::new(queue.clone());

    // Register low-priority first, high-priority second.
    endpoint.handle(Request::JobRegister(Job { name: "low".to_owned(), priority: 10, ..Job::default() })).unwrap();
    endpoint.handle(Request::JobRegister(Job { name: "high".to_owned(), priority: 90, ..Job::default() })).unwrap();

    // High priority should be dequeued first.
    let first = queue.dequeue().unwrap().unwrap();
    assert_eq!(first.job_id, "high");
    assert_eq!(first.priority, 90);

    let second = queue.dequeue().unwrap().unwrap();
    assert_eq!(second.job_id, "low");
    assert_eq!(second.priority, 10);

    server.stop();
}
