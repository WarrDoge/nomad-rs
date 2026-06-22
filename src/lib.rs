// SPDX-License-Identifier: Apache-2.0

//! `nomad-rs` — Hashicorp Nomad rewrite in Rust under Apache License 2.0.
//!
//! This crate is a from-scratch reimplementation of the Nomad scheduler,
//! client agent, and server components in idiomatic Rust.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

/// Nomad client configuration and lifecycle.
pub mod client;

/// Nomad server configuration and lifecycle.
pub mod server;

/// Shared configuration types.
pub mod config;

/// Nomad job specification types.
pub mod jobspec;

/// Placement constraints, affinities, and spread.
pub mod constraint;

/// Service registration and health checks.
pub mod service;

/// Rolling update / deployment strategy.
pub mod update;

/// Restart and reschedule policies.
pub mod reschedule;

/// Network resources: modes and ports.
pub mod network;

/// Volume requests and mounts.
pub mod volume;

/// Templates rendered into a task.
pub mod template;

/// Cluster node representation and scheduling eligibility.
pub mod node;

/// Allocations: task groups placed on nodes.
pub mod alloc;

/// Evaluations: the scheduler's unit of work.
pub mod eval;

/// In-memory cluster state store.
pub mod state;

/// Replicated finite state machine (Raft command-apply).
pub mod fsm;

/// Consensus contract (Raft).
pub mod raft;

/// RPC request/response contract.
pub mod rpc;

/// Cluster membership / gossip contract.
pub mod membership;

/// Node fingerprinting.
pub mod fingerprint;

/// Task artifacts fetched before a task starts.
pub mod artifact;

/// Task runner: single-task lifecycle.
pub mod taskrunner;

/// Alloc runner: all tasks of one allocation.
pub mod allocrunner;

/// Task drivers: pluggable execution backends.
pub mod driver;

/// Scheduler and evaluation engine.
pub mod scheduler;

/// HTTP API contract.
pub mod api;

/// CLI command parsing.
pub mod cli;

/// Access control: tokens, policies, authorization.
pub mod acl;

/// Telemetry sink contract.
pub mod metrics;

/// Deployments: orchestrated rollouts.
pub mod deployment;

/// Periodic (cron) job configuration.
pub mod periodic;

/// Parameterized jobs and dispatch.
pub mod dispatch;

/// Scaling policies for task groups.
pub mod scaling;

/// Secure variables and the encryption keyring.
pub mod variables;

/// Namespaces: tenancy boundaries.
pub mod namespace;

/// Node drain orchestration.
pub mod drain;

/// Error types used across the project.
pub mod error;

#[doc(hidden)]
pub mod util;
