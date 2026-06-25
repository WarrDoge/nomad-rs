# Implementation Backlog

> Nomad rewrite in Rust — Apache 2.0
> Reference: HashiCorp Nomad v1.6.5 (last MPL-2.0 tag)

---

## Status (2026-06-25)

`cargo test`: **305 pass, 0 ignored.** Green count is not a proxy for
"implemented" — stubs return `Ok(())`/defaults, so a passing test can exercise
an empty stub. When implementing a stub, confirm the test asserts real
behaviour, not the stub default.

Orchestrator core is wired end-to-end (single node, in process): job registered
via `rpc` → committed through single-node `raft` into `fsm`/`state` → `eval`
enqueued → drained by the `server` scheduler worker → `process_eval` placement →
committed back through raft → `ack`. `RpcServer`/`RpcClient` move requests over
TCP (length-prefixed JSON); `membership` gossips via SWIM-lite over UDP; the
`exec` driver runs real child processes (`taskrunner`/`allocrunner` drive it).

Still empty/partial: `client::run` (no loop yet), `raw_exec`/`docker` drivers
(fake handle), `artifact::Getter`, `otel` export.

### Real backlog (priority order)

1. **Ranking** — bin-pack/spread scoring + wire `Affinity` into placement
   (first-fit only today). Hard `Constraint`s now filter candidates in
   `process_eval`; scoring/affinity still TBD.
2. **Multi-node raft** — replication + election (pick `raft-rs`); persist log via
   `raft_log`. Unblocks a real cluster.
3. **mTLS + cluster wiring** — wrap the RPC stream in `tls::TlsConfig` →
   `tokio-rustls`; bind a listener in `Server::run`; `client::run` loop that
   dials a server via `RpcClient` and runs allocs; client auto-forward on
   `NotLeader{leader_addr}`.
4. **Driver depth** — isolation (cgroups/namespaces, Linux); real `raw_exec`/
   `docker` backends; live status rollup + restart-policy supervision.
5. **Membership failure detection** — periodic ping/ack (Suspect→Failed),
   indirect probes, self-refutation.
6. **Server housekeeping** — auto `unblock_all` on node change; heartbeat/TTL
   dead-node reaping; GC (jobs/evals/allocs/nodes). (Eval visibility-timeout
   reaping done — `EvalQueue::reap_expired` in `scheduler_worker`.)

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
- [x] Gossip protocol for cluster membership — `membership::GossipMembership` (Apache-clean SWIM-lite over tokio UDP: push-pull join, gossip leave, incarnation merge). Failure detection (ping/ack, Suspect→Failed) TBD
- [~] Leader election — `raft::RaftNode::{role,is_leader,bootstrap}`; real election TBD
- [~] Cluster state machine (jobs, evaluations, allocations) — `fsm` + `state`

### RPC Layer
- [ ] mTLS transport
- [x] RPC server — `rpc::RpcServer` accepts TCP conns, dispatches framed `Request`s through `RpcEndpoint`, writes `Response`s. mTLS wrap (`tls::TlsConfig`) TBD
- [x] RPC client for node-to-server communication — `rpc::RpcClient::{connect,call}` over the same length-prefixed JSON framing
- [x] Forwarding to leader — `rpc::RpcEndpoint` commits via `RaftNode`; follower returns `Response::NotLeader { leader_addr }`. Client auto-redial of the leader addr TBD

### Scheduling Engine
- [x] Evaluation loop (dequeue + process) — `server::scheduler_worker` async background loop: leader-gated dequeue → `process_eval` → commit plan via `raft.propose` → ack/nack (started/stopped by `Server::run`/`stop`). `scheduler::drain_queue` keeps the synchronous one-pass variant. `eval_queue::EvalQueue` real
- [x] Eval broker — `eval_queue::EvalQueue`: priority enqueue/dequeue + ack/nack/in-flight (`MAX_DEQUEUE=3` delivery cap) + `reap_expired` visibility-timeout reaping (wired into `scheduler_worker`, 60s)
- [x] Blocked-eval tracker — `eval_queue::{block,unblock_all,blocked_len}` park evals + bulk re-enqueue. Auto-trigger on node capacity change TBD (needs #8 worker)
- [ ] Plan queue + plan applier (serialize plans through the leader)
- [ ] Scheduler types — service / batch / system / sysbatch (distinct placement logic)
- [ ] Scheduler worker pool (concurrent eval processing)
- [x] Feasibility checking (resources) — `scheduler::node_fits` real: ready/eligible/not-draining + cpu/mem fit after subtracting live allocs + hard `Constraint`s (`TaskGroup.constraints` filtered via `satisfied_by`). Affinities not yet wired in
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
- [x] Constraints (hard) — `constraint::Constraint` wired into `process_eval` via `TaskGroup.constraints`. Soft (affinity) TBD
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
