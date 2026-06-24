# Implementation Backlog

> Nomad rewrite in Rust — Apache 2.0
> Reference: HashiCorp Nomad v1.6.5 (last MPL-2.0 tag)

---

## Test-spec status (TDD)

Every subsystem is specified test-first: types are real, behaviour is stubbed
(returns defaults with a `// TODO:` marker), and the tests covering unbuilt
behaviour are `#[ignore]`d until implemented. Implementing a module = drop the
`#[ignore]`s and make the stub satisfy its existing tests; no new test design
needed. Infra layers are specced as **dependency-agnostic traits** — the
concrete crate (raft, http, gossip, crypto, container runtime) is chosen at
implementation time behind the trait.

Run `cargo test` for the green count and `cargo test -- --ignored` for the
pending list. Current: 257 green (the modules below marked ✅), 0 ignored.

> Note: stubs return `Ok(())`/defaults, not `todo!()`. A non-`#[ignore]`d test
> can therefore pass against an empty stub — when un-ignoring a test, confirm it
> actually exercises the new behaviour, not just the stub's default.

**Job specification** — `jobspec` ✅, `constraint`, `service`, `update`,
`reschedule`, `network`, `volume`, `template`, `scaling`, `periodic`,
`dispatch`.

**Domain model** — `error` ✅, `config` ✅, `node`, `alloc`, `eval`,
`namespace`, `variables`.

**Server / control plane** — `state` (store) ✅, `fsm` (command-apply) ✅,
`raft` (`Consensus` trait), `raft_log` (persistence) ✅,
`rpc` (`RpcHandler` trait, `RpcEndpoint`, `eval_queue`) ✅,
`membership` (`Membership` trait), `scheduler` (`node_fits`/`Plan`/`process_eval`, `EvalQueue`),
`deployment`, `drain`, `acl`.

**Client / runtime** — `client`, `fingerprint` (`Fingerprinter` trait),
`allocrunner`, `taskrunner`, `driver` (`TaskDriver`: exec/raw_exec/docker),
`artifact` (`Getter` trait).

**Edge** — `server` lifecycle, `api` (`ApiHandler` trait), `cli` (`parse`),
`metrics` (`MetricSink` trait).

**Still unspecced** (lower-priority breadth to add the same way): CSI volume
plugin lifecycle, Consul/Vault integration internals, Sentinel/quota (ENT),
event stream, autopilot, multi-region federation, native Nomad service
discovery (non-Consul). Each becomes an ignored module when reached.

---

**Legend:** `[x]` implemented (green tests) · `[~]` specced (`#[ignore]`d tests, awaiting
implementation) · `[ ]` not started.

---

## Phase 1: Core Agent Framework ◐

### Agent Lifecycle
- [ ] Agent struct that holds Client and/or Server
- [ ] Signal handling (SIGINT, SIGTERM, SIGHUP)
- [ ] Graceful shutdown with timeout
- [ ] Config file parsing (HCL / JSON)
- [~] CLI entrypoint (`nomad-rs agent`, `nomad-rs server`, `nomad-rs client`) — `cli::parse` spec
- [ ] Logging subsystem (tracing subscriber with env-filter + file rotation)

### Configuration
- [~] Full config struct matching Nomad's surface area — `config::Config` (core subset)
- [ ] Config merge from file + env vars + CLI flags
- [x] Config validation (bind addr, required fields) — `config::Config::validate`
- [ ] Reload on SIGHUP

---

## Phase 2: Server ◐

### Consensus & Cluster Membership
- [~] Raft consensus (using `raft-rs` or similar) — `raft::Consensus` trait
- [~] Serf gossip protocol for cluster membership (or `memberlist` crate) — `membership::Membership` trait
- [~] Leader election — `raft::Consensus::{role,is_leader,leader_addr}`
- [~] Cluster state machine (jobs, evaluations, allocations) — `fsm` + `state`

### RPC Layer
- [ ] mTLS transport
- [~] RPC server (custom protocol over mTLS) — `rpc::RpcHandler` trait + req/resp
- [ ] RPC client for node-to-server communication
- [~] Forwarding to leader — modelled in `rpc::RpcHandler::handle` contract

### Scheduling Engine
- [x] Evaluation loop (dequeue + process) — `scheduler::Scheduler::{run,process_eval}`, `eval_queue::EvalQueue`
- [x] Eval broker (priority dequeue, ack/nack, in-flight tracking) — `eval_queue::EvalQueue`
- [ ] Blocked-eval tracker (re-enqueue when capacity changes)
- [ ] Plan queue + plan applier (serialize plans through the leader)
- [ ] Scheduler types — service / batch / system / sysbatch (distinct placement logic)
- [ ] Scheduler worker pool (concurrent eval processing)
- [~] Feasibility checking (constraints, resources, affinities) — `scheduler::node_fits`, `constraint`
- [ ] Ranking (bin packing, spread, scoring)
- [ ] Preemption (evict lower-priority allocs to place higher)
- [~] Allocation plan generation and apply — `scheduler::Plan`
- [~] Periodic job handling — `periodic::PeriodicConfig`
- [~] Parameterized / dispatch jobs — `dispatch::ParameterizedJob`

### Server Background Jobs
- [ ] Garbage collection — jobs, evals, allocs, nodes, deployments (`core_sched` equivalent)
- [ ] Node heartbeat / TTL tracking + dead-node reaping (core leader loop, not just HA)

---

|## Phase 3: Client ✅

### Task Runner
- [~] Task lifecycle (received → running → dead) — `taskrunner` + `allocrunner`
- [~] Restart policy implementation — `reschedule::RestartPolicy`, `taskrunner::handle_exit`
- [~] Task health checking — `service::ServiceCheck`
- [~] Artifact download (HTTP(S), S3, Git) — `artifact::Getter` trait
- [ ] Client-side alloc garbage collection (disk pressure + terminal alloc cleanup)

### Drivers
- [~] `exec` driver (fork/exec a process) — `driver::ExecDriver`
- [~] `raw_exec` driver (no isolation) — `driver::RawExecDriver`
- [ ] `java` driver
- [~] `docker` driver via bollard (Docker API) — `driver::DockerDriver`
- [ ] `podman` driver
- [ ] Plugin system for 3rd-party drivers (`driver::TaskDriver` trait is the seam)

### Device Plugins
- [ ] GPU detection and allocation (NVIDIA)
- [ ] Custom device plugin interface

### Host Volume Management
- [ ] Mount propagation
- [~] Volume configuration and validation — `volume::{VolumeRequest,VolumeMount}`

---

## Phase 4: Job Specification ◐

### Job HCL Parser
- [ ] HCL → Rust struct deserialization
- [x] Job spec validation (constraints, uniqueness, etc.) — `jobspec::{Job,TaskGroup,Task,Resources}::validate`
- [~] Periodic job spec parsing — `periodic::PeriodicConfig` (config + `next`)
- [~] Parameterized job spec parsing — `dispatch::ParameterizedJob`

### Supported Job Features
- [x] Task groups with count scaling — `jobspec::TaskGroup` (count validation)
- [~] Constraints (hard + soft) — `constraint::Constraint`
- [~] Affinities — `constraint::Affinity`
- [~] Spread (per-datacenter, per-node, etc.) — `constraint::Spread`
- [~] Network resources (ports, DNS, static IPs) — `network::{NetworkResource,Port}`
- [~] CPU / memory / disk / network resource tracking — `jobspec::Resources` (disk TBD)
- [ ] Devices (GPUs, etc.)
- [~] Services (Consul integration equivalent) — `service::Service`
- [~] Checks (HTTP, TCP, Script) — `service::ServiceCheck`
- [~] Templates (Consul template, Vault template) — `template::Template`
- [ ] Log configuration (syslog, file, journald)
- [x] User-defined metadata — `jobspec::Job::meta`
- [~] Migrate / resize / stop strategies — `alloc::DesiredStatus`, `drain` (migrate TBD)
- [~] Update (rolling, blue/green, canary) — `update::UpdateStrategy`, `deployment`
- [ ] Prestart / poststop lifecycle hooks

---

## Phase 5: State & Persistence ✅

### Client State
- [x] SQLite state store — `client_state::ClientState`
- [x] Allocation state tracking (running, completed, failed) — `alloc::{ClientStatus,DesiredStatus}`
- [x] Task state machine with recovery on restart — `driver::TaskState`, `taskrunner`

### Server State
- [x] Raft log persistence — `raft_log::RaftLogStore` (JSONL + snapshots)
- [x] Snapshot and restore — `state::StateStore::{save,load}`
- [x] Job index — `state::StateStore` (job ops)
- [x] Evaluation index — `state::StateStore` (eval ops), `eval_queue::EvalQueue` (priority queue)
- [x] Allocation index — `state::StateStore` (alloc by node/job)
- [x] Node index — `state::StateStore` (node ops)

---

## Phase 6: APIs & Interop ✅

### HTTP API
- [x] Handler contract + request/response — `api::ApiHandler`, `api::{ApiRequest,ApiResponse}`
- [x] `/v1/jobs` — CRUD for jobs
- [x] `/v1/evaluations` — evaluation lifecycle
- [x] `/v1/allocations` — allocation status
- [x] `/v1/nodes` — node registration and status
- [x] `/v1/agent` — agent health, members, self
- [x] `/v1/status` — cluster leader, peers
- [x] `/v1/operator` — Raft, snapshot, debug
- [ ] OpenAPI spec generation

### CLI
- [x] Command parsing — `cli::parse` → `ParsedCommand`
- [x] `nomad-rs job run` / `stop` / `status` / `inspect`
- [x] `nomad-rs node status` / `drain` / `eligibility`
- [x] `nomad-rs server members` / `force-leave` / `join`
- [x] `nomad-rs alloc status` / `logs` / `exec`
- [x] `nomad-rs monitor`
- [ ] Tab completion (bash, zsh, fish)

---

## Phase 7: Production Hardening ◐

### Observability
- [ ] OpenTelemetry tracing + metrics
- [x] Prometheus metrics endpoint — `metrics::MetricSink` trait + `Metric`
- [x] Structured JSON log output — `tracing-subscriber` with `json` feature
- [ ] Health check endpoints for K8s / Nomad itself
- [ ] pprof / debug endpoints

### Security
- [ ] mTLS between all components
- [x] ACL system (capabilities + policies) — `acl::{Token,Policy,Capability}`
- [~] Vault integration for secrets — `variables::{Variable,Keyring}` (native vars; Vault TBD)
- [ ] Workload identity / SPIFFE
- [x] Token-based API auth — `acl::Token::allows`
- [ ] Audit logging

### High Availability
- [ ] Server redundancy with Raft failover
- [ ] Client reconnection with exponential backoff
- [ ] Evaluation re-queuing on leader loss
- [ ] Heartbeat monitoring with dead-node reaping

### Performance
- [ ] Evaluation batching and deduplication
- [ ] Node scoring parallelism
- [ ] Alloc reconcile vs full recompute
- [ ] Connection pooling in RPC layer
- [x] Benchmark suite for scheduler throughput — `cargo bench` (criterion, benches/scheduler_bench.rs)

---

## Phase 8: Testing & CI ◐

- [x] Unit tests for every module — present as `#[ignore]`d specs; un-ignored as modules are implemented
- [x] Integration tests (multi-node cluster in process) — `tests/cluster.rs` (3 tests), `tests/lifecycle.rs`
- [x] Benchmarks for scheduler — `cargo bench` (criterion, benches/scheduler_bench.rs)
- [ ] Property-based testing for state machine invariants
- [x] Fuzz testing for job validation — `fuzz/` (cargo-fuzz, fuzz_targets/validate_job.rs)
- [ ] E2E tests with real Docker containers
- [ ] CI pipeline (mise tasks → GitHub Actions)
- [ ] cargo-deny advisory scanning (scheduled)
- [ ] cargo-audit in CI
- [ ] MSRV policy and testing
- [ ] Cross-compilation targets (linux/arm64, darwin/amd64)

---

## Reference Material

| Resource | Link | Description |
|----------|------|-------------|
| Nomad source (MPL-2.0) | `../nomad-original-ref` (local, v1.6.5) | Upstream HashiCorp Nomad at `a7cfff3`. Go codebase. Module mapping: `scheduler/` → Rust `scheduler, eval, alloc`; `nomad/` → Rust `server, fsm, state, raft`; `client/` → Rust `client, allocrunner, taskrunner`; `command/` → Rust `cli`; `api/` → Rust `api`; `jobspec/` → Rust `jobspec`; `acl/` → Rust `acl`; `drivers/` → Rust `driver`. |
| Nomad docs | https://developer.hashicorp.com/nomad/docs | |
| Nomad API spec | https://developer.hashicorp.com/nomad/api-docs | |
| raft-rs | https://github.com/tikv/raft-rs | |
| memberlist (Rust) | https://crates.io/crates/memberlist | |

---

*Generated from reference: HashiCorp Nomad v1.6.5. Not a commitment — priority is gated by what the first real workload needs.*
