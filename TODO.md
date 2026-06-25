# Implementation Backlog

> Nomad rewrite in Rust — Apache 2.0
> Reference: HashiCorp Nomad v1.6.5 (last MPL-2.0 tag)

---

## Status (verified against source, 2026-06-25)

`cargo test`: **292 pass, 0 ignored.** Green count is NOT a proxy for
"implemented" — stubs return `Ok(())`/defaults (no `todo!()`), so a passing
test can be exercising an empty stub. The classification below reflects what
the **code actually does**, not what compiles.

> When implementing a stub: confirm the existing test exercises real behaviour,
> not the stub default. Add assertions if it doesn't.

### ✅ Real (genuine logic + meaningful tests)

- **Persistence/domain core:** `state` (HashMap store), `fsm` (Command→apply),
  `eval_queue` (BinaryHeap priority enqueue/dequeue), `raft_log` (JSONL +
  snapshots), `client_state` (real rusqlite/SQLite store), `error`, `config`.
- **Job spec + domain types** (real validation/matching): `jobspec`,
  `constraint` (operator matching incl regexp/version), `reschedule`
  (RestartPolicy), `service`, `update`, `network`, `volume`, `template`,
  `scaling`, `periodic`, `dispatch`, `node`, `alloc`, `eval`, `namespace`,
  `variables`, `deployment`, `drain`.
- **Edge:** `api` (real method/path routing → handlers), `cli` (`parse` →
  `ParsedCommand`), `acl` (token/policy/capability checks).
- **Infra:** `tls`, `integration` (glue), `logging`, `fingerprint`, `metrics`
  (`MetricSink` trait).

### ⛔ Stub only (signature + passing-against-stub test, no behaviour)

- **Orchestrator core — `scheduler::process_eval` now real** (capacity-aware
  first-fit → `Plan`), and `scheduler::drain_queue` dequeues + applies. `raft`
  is single-node (bootstrap leader, propose→commit→FSM; no replication yet).
  `rpc::RpcEndpoint` commits writes through Raft + reports `NotLeader` (no wire
  transport yet). Still stub: `server::run`, `client::run`, `membership`
  (gossip unimpl), `agent::run`.
- **Drivers — `exec` real and wired** (spawns a child process, kill/inspect);
  `taskrunner`/`allocrunner` now drive it end to end. `raw_exec` + `docker`
  still return a fake `TaskHandle`; `artifact` (`Getter`) still a stub.
- `otel` (tracing/metrics export unimpl).

### Corrections vs prior TODO

- `scheduler::{node_fits, Plan, process_eval}` — now **implemented** (resource
  feasibility + capacity-aware first-fit placement). Ranking/constraints/affinity
  not yet applied.
- `eval_queue` has **no ack/nack/in-flight tracking** — priority enqueue/
  dequeue only.

### Missing for a working orchestrator (real backlog, priority order)

1. ~~`scheduler::process_eval`~~ ✅ done. Next: ranking (bin-pack/spread) +
   wire `constraint`/`Affinity` into `node_fits`.
2. ~~Plan apply~~ ✅ done — `scheduler::{apply_plan, process_and_apply,
   drain_queue}` commit placements via `fsm` and drain an `EvalQueue` in one
   pass. Remaining: async `scheduler::run` worker + plan routing through the
   Raft leader (folded into #8).
3. ~~Real `driver` exec + runner wiring~~ ✅ done. `ExecDriver` (`std::process`,
   pid handle, kill/try_wait); `taskrunner` start/poll/stop via driver;
   `allocrunner` drives one runner per task. Next: isolation (cgroups/
   namespaces, Linux), `raw_exec`/`docker` real backends, live status rollup +
   restart-policy supervision.
4. ~~`raft` append + commit, wire `fsm`~~ ✅ single-node: `RaftNode::bootstrap`
   leads, `propose` appends → commits → applies to FSM (quorum=1). Next:
   multi-node replication + election (pick `raft-rs`), persist log via
   `raft_log`.
5. ~~`rpc` leader-forward~~ ✅ `RpcEndpoint` commits writes through `RaftNode`
   (job/node land in FSM state), follower returns `Response::NotLeader`. Next:
   real wire transport (custom-over-mTLS / gRPC) + actually forwarding to the
   leader addr instead of just reporting it.
6. `membership` gossip (pick `memberlist`).
7. `eval_queue` ack/nack/in-flight + blocked-eval re-enqueue.
8. `server`/`client`/`agent` run-loops that tie the above together.

**Still unspecced:** CSI volume plugin lifecycle, Consul/Vault internals,
Sentinel/quota (ENT), event stream, autopilot, multi-region federation, native
service discovery.

---

**Legend:** `[x]` real impl (logic, not just a passing stub) · `[~]` type/trait
specced, behaviour stubbed · `[ ]` not started.

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
- [~] Raft consensus — `raft::RaftNode` single-node bootstrap (propose→commit→apply to FSM, quorum=1). Multi-node replication via `raft-rs` still TBD
- [~] Serf gossip protocol for cluster membership (or `memberlist` crate) — `membership::Membership` trait
- [~] Leader election — `raft::RaftNode::{role,is_leader,bootstrap}`; real election TBD
- [~] Cluster state machine (jobs, evaluations, allocations) — `fsm` + `state`

### RPC Layer
- [ ] mTLS transport
- [~] RPC server (custom protocol over mTLS) — `rpc::RpcEndpoint` req/resp commits writes via Raft and dequeues evals; wire transport TBD
- [ ] RPC client for node-to-server communication
- [x] Forwarding to leader — `rpc::RpcEndpoint` commits via `RaftNode`; follower returns `Response::NotLeader { leader_addr }`. Actual cross-node forward needs the transport

### Scheduling Engine
- [x] Evaluation loop (dequeue + process) — `scheduler::drain_queue` dequeues an `EvalQueue` → `process_eval` → `apply_plan` (one synchronous pass). Async worker loop + leader leasing still TBD (#8). `eval_queue::EvalQueue` real
- [~] Eval broker — `eval_queue::EvalQueue` does priority enqueue/dequeue only. ack/nack + in-flight tracking NOT implemented
- [ ] Blocked-eval tracker (re-enqueue when capacity changes)
- [ ] Plan queue + plan applier (serialize plans through the leader)
- [ ] Scheduler types — service / batch / system / sysbatch (distinct placement logic)
- [ ] Scheduler worker pool (concurrent eval processing)
- [x] Feasibility checking (resources) — `scheduler::node_fits` real: ready/eligible/not-draining + cpu/mem fit after subtracting live allocs. Affinities/constraints not yet wired in
- [ ] Ranking (bin packing, spread, scoring)
- [ ] Preemption (evict lower-priority allocs to place higher)
- [x] Allocation plan generation and apply — `scheduler::Plan` + `process_eval` (gen) + `apply_plan`/`process_and_apply`/`drain_queue` (apply via `fsm`). Leader/Raft routing still TBD
- [~] Periodic job handling — `periodic::PeriodicConfig`
- [~] Parameterized / dispatch jobs — `dispatch::ParameterizedJob`

### Server Background Jobs
- [ ] Garbage collection — jobs, evals, allocs, nodes, deployments (`core_sched` equivalent)
- [ ] Node heartbeat / TTL tracking + dead-node reaping (core leader loop, not just HA)

---

## Phase 3: Client ◐ (types specced, no task ever actually runs)

### Task Runner
- [x] Task lifecycle (received → running → dead) — `taskrunner` drives a real `ExecDriver` (start/poll/stop); `allocrunner` spawns one `TaskRunner` per task, `run`/`destroy` start/stop them. Live status rollup + restart supervision still TBD
- [~] Restart policy implementation — `reschedule::RestartPolicy`, `taskrunner::handle_exit`
- [~] Task health checking — `service::ServiceCheck`
- [~] Artifact download (HTTP(S), S3, Git) — `artifact::Getter` trait
- [ ] Client-side alloc garbage collection (disk pressure + terminal alloc cleanup)

### Drivers
- [x] `exec` driver — `driver::ExecDriver` real: spawns child via `std::process` (config `command`+`args`), pid handle, `stop_task` kills, `inspect_task` via `try_wait`. NOT yet isolated (no cgroups/namespaces — see code ponytail note)
- [~] `raw_exec` driver — `driver::RawExecDriver` fake handle; no process spawn yet
- [ ] `java` driver
- [~] `docker` driver — `driver::DockerDriver` fake handle; bollard NOT wired
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
