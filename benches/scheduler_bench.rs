// SPDX-License-Identifier: Apache-2.0

//! Micro-benchmarks for nomad-rs core operations.
//!
//! Run with `cargo bench`.
#![allow(
    clippy::unwrap_used,
    clippy::missing_docs_in_private_items,
    clippy::missing_panics_doc,
    clippy::redundant_closure
)]

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use nomad_rs::eval::EvalStatus;
use nomad_rs::eval::EvalTrigger;
use nomad_rs::eval::Evaluation;
use nomad_rs::eval_queue::EvalQueue;
use nomad_rs::fsm::Command;
use nomad_rs::jobspec::Job;
use nomad_rs::raft_log::RaftLogStore;
use nomad_rs::state::StateStore;

fn make_job(name: &str) -> Job {
    Job { name: name.to_owned(), priority: 50, ..Job::default() }
}

fn make_eval(id: &str) -> Evaluation {
    Evaluation {
        id: id.into(),
        job_id: "bench".into(),
        priority: 50,
        trigger: EvalTrigger::JobRegister,
        status: EvalStatus::Pending,
    }
}

fn bench_state_upsert_job(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/upsert_job");
    group.throughput(criterion::Throughput::Elements(1));

    group.bench_function("single", |b| {
        b.iter_batched_ref(
            || StateStore::new(),
            |state| state.upsert_job(black_box(make_job("bench"))).ok(),
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("batch_1000", |b| {
        b.iter_batched_ref(
            || StateStore::new(),
            |state| {
                for i in 0..1000 {
                    state.upsert_job(make_job(&format!("job-{i}"))).ok();
                }
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_state_list_jobs(c: &mut Criterion) {
    let mut group = c.benchmark_group("state/list_jobs");

    group.bench_function("empty", |b| {
        let state = StateStore::new();
        b.iter(|| state.list_jobs());
    });

    group.bench_function("1000_jobs", |b| {
        let mut state = StateStore::new();
        for i in 0..1000 {
            state.upsert_job(make_job(&format!("job-{i}"))).ok();
        }
        b.iter(|| state.list_jobs());
    });

    group.finish();
}

fn bench_eval_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("eval_queue");

    group.bench_function("enqueue_1000", |b| {
        b.iter_batched_ref(
            || EvalQueue::new(),
            |queue| {
                for i in 0..1000 {
                    queue.enqueue(make_eval(&format!("e-{i}"))).ok();
                }
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.bench_function("dequeue_all", |b| {
        b.iter_batched_ref(
            || {
                let q = EvalQueue::new();
                for i in 0..1000 {
                    q.enqueue(make_eval(&format!("e-{i}"))).ok();
                }
                q
            },
            |queue| {
                while queue.dequeue().unwrap_or_default().is_some() {}
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_raft_log(c: &mut Criterion) {
    let tmp = std::env::temp_dir().join(format!("nomad_bench_raft_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let path = tmp.join("bench");

    let mut group = c.benchmark_group("raft_log");

    group.bench_function("append", |b| {
        b.iter_batched_ref(
            || RaftLogStore::open(&path).unwrap(),
            |store| {
                let cmd = Command::UpsertJob(make_job("bench"));
                store.append(1, cmd).ok();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
    // Cleanup.
    for ext in &["log", "snap", "snap.tmp"] {
        let p = path.with_extension(ext);
        std::fs::remove_file(&p).ok();
    }
    std::fs::remove_dir(&tmp).ok();
}

fn bench_raft_log_read(c: &mut Criterion) {
    let tmp = std::env::temp_dir().join(format!("nomad_bench_raft_read_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    let path = tmp.join("bench");

    // Pre-populate 1000 entries.
    {
        let store = RaftLogStore::open(&path).unwrap();
        for _ in 0..1000 {
            store.append(1, Command::UpsertJob(make_job("bench"))).ok();
        }
    }

    let mut group = c.benchmark_group("raft_log/read");

    group.bench_function("get_last", |b| {
        // Re-open to measure cold start.
        let store = RaftLogStore::open(&path).unwrap();
        b.iter(|| store.get(1000));
    });

    group.bench_function("entries_from_mid", |b| {
        let store = RaftLogStore::open(&path).unwrap();
        b.iter(|| store.entries_from(500));
    });

    group.finish();

    for ext in &["log", "snap", "snap.tmp"] {
        let p = path.with_extension(ext);
        std::fs::remove_file(&p).ok();
    }
    std::fs::remove_dir(&tmp).ok();
}

use nomad_rs::constraint::{Affinity, Spread, SpreadTarget};
use nomad_rs::jobspec::{Resources, Task, TaskGroup};
use nomad_rs::node::{Node, NodeStatus, SchedulingEligibility};
use nomad_rs::scheduler::process_eval;
use std::collections::HashMap;

/// Seed the state with `node_count` nodes and a job that has affinities
/// and spreads, then run `process_eval`. This exercises the scoring path.
fn bench_scoring_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler/scoring_path");

    for node_count in [100, 500, 1000] {
        let group_count = 20;
        let bench_name = format!("{node_count}_nodes_x_{group_count}_instances");
        group.bench_function(&bench_name, |b| {
            b.iter_batched_ref(
                || {
                    let mut state = StateStore::new();

                    // Create nodes with attributes to drive affinities and spreads.
                    for i in 0..node_count {
                        // Distribute across 5 datacenters and 3 os types.
                        let dc_idx = i % 5;
                        let os_idx = i % 3;
                        let mut attrs = HashMap::new();
                        attrs.insert("dc".to_owned(), format!("dc{}", dc_idx + 1));
                        attrs.insert(
                            "os".to_owned(),
                            match os_idx {
                                0 => "linux",
                                1 => "windows",
                                _ => "darwin",
                            }
                            .to_owned(),
                        );
                        let node = Node {
                            id: format!("n{i}").into(),
                            name: format!("node-{i}"),
                            datacenter: format!("dc{}", dc_idx + 1),
                            node_class: String::new(),
                            resources: Resources { cpu_mhz: 8000, memory_mb: 16384, network_mbps: 1000 },
                            status: NodeStatus::Ready,
                            eligibility: SchedulingEligibility::Eligible,
                            draining: false,
                            attributes: attrs,
                            drivers: HashMap::new(),
                        };
                        state.upsert_node(node).ok();
                    }

                    // A job with an affinity, a spread, and many instances.
                    let task = Task {
                        name: "t".to_owned(),
                        driver: "exec".to_owned(),
                        config: HashMap::new(),
                        resources: Resources { cpu_mhz: 500, memory_mb: 1024, network_mbps: 100 },
                    };
                    let job = Job {
                        name: "scored".to_owned(),
                        task_groups: vec![TaskGroup {
                            name: "web".to_owned(),
                            count: group_count,
                            tasks: vec![task],
                            constraints: vec![],
                            affinities: vec![Affinity {
                                left: "dc".to_owned(),
                                right: "dc1".to_owned(),
                                operand: "=".to_owned(),
                                weight: 50,
                            }],
                            spreads: vec![Spread {
                                attribute: "dc".to_owned(),
                                targets: vec![
                                    SpreadTarget { value: "dc1".to_owned(), percent: 40 },
                                    SpreadTarget { value: "dc2".to_owned(), percent: 30 },
                                ],
                            }],
                        }],
                        ..Job::default()
                    };
                    state.upsert_job(job).ok();

                    let eval = Evaluation {
                        id: "bench-scored".into(),
                        job_id: "scored".into(),
                        priority: 50,
                        trigger: EvalTrigger::JobRegister,
                        status: EvalStatus::Pending,
                    };

                    (state, eval)
                },
                |(state, eval)| {
                    black_box(process_eval(eval, state));
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_state_upsert_job,
    bench_state_list_jobs,
    bench_eval_queue,
    bench_raft_log,
    bench_raft_log_read,
    bench_scoring_path,
);
criterion_main!(benches);
