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
pending list. Current: 64 green (the modules below marked ✅), 137 ignored
awaiting implementation.

> Note: stubs return `Ok(())`/defaults, not `todo!()`. A non-`#[ignore]`d test
> can therefore pass against an empty stub — when un-ignoring a test, confirm it
> actually exercises the new behaviour, not just the stub's default.

**Job specification** — `jobspec` ✅, `constraint`, `service`, `update`,
`reschedule`, `network`, `volume`, `template`, `scaling`, `periodic`,
`dispatch`.

**Domain model** — `error` ✅, `config` ✅, `node`, `alloc`, `eval`,
`namespace`, `variables`.

**Server / control plane** — `state` (store), `fsm` (command-apply),
`raft` (`Consensus` trait), `rpc` (`RpcHandler` trait),
`membership` (`Membership` trait), `scheduler` (`node_fits`/`Plan`/`process_eval`),
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
- [~] Evaluation loop (dequeue + process) — `scheduler::Scheduler::{run,process_eval}`
- [ ] Eval broker (priority dequeue, ack/nack, in-flight tracking) — distinct from the loop
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

## Phase 5: State & Persistence ◐

### Client State
- [ ] BoltDB or SQLite state store
- [~] Allocation state tracking (running, completed, failed) — `alloc::{ClientStatus,DesiredStatus}`
- [~] Task state machine with recovery on restart — `driver::TaskState`, `taskrunner`

### Server State
- [ ] Raft log persistence
- [ ] Snapshot and restore
- [~] Job index — `state::StateStore` (job ops)
- [~] Evaluation index — `state::StateStore` (eval ops)
- [~] Allocation index — `state::StateStore` (alloc by node/job)
- [~] Node index — `state::StateStore` (node ops)

---

## Phase 6: APIs & Interop ◐

### HTTP API
- [~] Handler contract + request/response — `api::ApiHandler`, `api::{ApiRequest,ApiResponse}`
- [ ] `/v1/jobs` — CRUD for jobs
- [ ] `/v1/evaluations` — evaluation lifecycle
- [ ] `/v1/allocations` — allocation status
- [ ] `/v1/nodes` — node registration and status
- [ ] `/v1/agent` — agent health, members, self
- [ ] `/v1/status` — cluster leader, peers
- [ ] `/v1/operator` — Raft, snapshot, debug
- [ ] OpenAPI spec generation

### CLI
- [~] Command parsing — `cli::parse` → `ParsedCommand`
- [ ] `nomad-rs job run` / `stop` / `status` / `inspect`
- [ ] `nomad-rs node status` / `drain` / `eligibility`
- [ ] `nomad-rs server members` / `force-leave` / `join`
- [ ] `nomad-rs alloc status` / `logs` / `exec`
- [ ] `nomad-rs monitor`
- [ ] Tab completion (bash, zsh, fish)

---

## Phase 7: Production Hardening ◐

### Observability
- [ ] OpenTelemetry tracing + metrics
- [~] Prometheus metrics endpoint — `metrics::MetricSink` trait + `Metric`
- [ ] Structured JSON log output
- [ ] Health check endpoints for K8s / Nomad itself
- [ ] pprof / debug endpoints

### Security
- [ ] mTLS between all components
- [~] ACL system (capabilities + policies) — `acl::{Token,Policy,Capability}`
- [~] Vault integration for secrets — `variables::{Variable,Keyring}` (native vars; Vault TBD)
- [ ] Workload identity / SPIFFE
- [~] Token-based API auth — `acl::Token::allows`
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
- [ ] Benchmark suite for scheduler throughput

---

## Phase 8: Testing & CI ◐

- [~] Unit tests for every module — present as `#[ignore]`d specs; un-ignored as modules are implemented
- [ ] Integration tests (multi-node cluster in process) — single-process smoke in `tests/lifecycle.rs`
- [ ] Benchmarks for scheduler (O(1000) nodes × O(1000) jobs)
- [ ] Property-based testing for state machine invariants
- [ ] Fuzz testing for HCL parser
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
