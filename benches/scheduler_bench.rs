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
        id: id.to_owned(),
        job_id: "bench".to_owned(),
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

criterion_group!(
    benches,
    bench_state_upsert_job,
    bench_state_list_jobs,
    bench_eval_queue,
    bench_raft_log,
    bench_raft_log_read,
);
criterion_main!(benches);
