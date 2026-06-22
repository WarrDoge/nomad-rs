# Implementation Backlog

> Nomad rewrite in Rust — Apache 2.0
> Reference: HashiCorp Nomad v1.6.5 (last MPL-2.0 tag)

---

## Phase 1: Core Agent Framework ☐

### Agent Lifecycle
- [ ] Agent struct that holds Client and/or Server
- [ ] Signal handling (SIGINT, SIGTERM, SIGHUP)
- [ ] Graceful shutdown with timeout
- [ ] Config file parsing (HCL / JSON)
- [ ] CLI entrypoint (`nomad-rs agent`, `nomad-rs server`, `nomad-rs client`)
- [ ] Logging subsystem (tracing subscriber with env-filter + file rotation)

### Configuration
- [ ] Full config struct matching Nomad's surface area
- [ ] Config merge from file + env vars + CLI flags
- [ ] Config validation (port ranges, paths exist, etc.)
- [ ] Reload on SIGHUP

---

## Phase 2: Server ☐

### Consensus & Cluster Membership
- [ ] Raft consensus (using `raft-rs` or similar)
- [ ] Serf gossip protocol for cluster membership (or `memberlist` crate)
- [ ] Leader election
- [ ] Cluster state machine (jobs, evaluations, allocations)

### RPC Layer
- [ ] mTLS transport
- [ ] RPC server (custom protocol over mTLS)
- [ ] RPC client for node-to-server communication
- [ ] Forwarding to leader

### Scheduling Engine
- [ ] Evaluation loop (dequeue + process)
- [ ] Feasibility checking (constraints, resources, affinities)
- [ ] Ranking (bin packing, spread, scoring)
- [ ] Allocation plan generation and apply
- [ ] Periodic job handling
- [ ] Parameterized / dispatch jobs

---

## Phase 3: Client ☐

### Task Runner
- [ ] Task lifecycle (received → running → dead)
- [ ] Restart policy implementation
- [ ] Task health checking
- [ ] Artifact download (HTTP(S), S3, Git)

### Drivers
- [ ] `exec` driver (fork/exec a process)
- [ ] `raw_exec` driver (no isolation)
- [ ] `java` driver
- [ ] `docker` driver via bollard (Docker API)
- [ ] `podman` driver
- [ ] Plugin system for 3rd-party drivers

### Device Plugins
- [ ] GPU detection and allocation (NVIDIA)
- [ ] Custom device plugin interface

### Host Volume Management
- [ ] Mount propagation
- [ ] Volume configuration and validation

---

## Phase 4: Job Specification ☐

### Job HCL Parser
- [ ] HCL → Rust struct deserialization
- [ ] Job spec validation (constraints, uniqueness, etc.)
- [ ] Periodic job spec parsing
- [ ] Parameterized job spec parsing

### Supported Job Features
- [ ] Task groups with count scaling
- [ ] Constraints (hard + soft)
- [ ] Affinities
- [ ] Spread (per-datacenter, per-node, etc.)
- [ ] Network resources (ports, DNS, static IPs)
- [ ] CPU / memory / disk / network resource tracking
- [ ] Devices (GPUs, etc.)
- [ ] Services (Consul integration equivalent)
- [ ] Checks (HTTP, TCP, Script)
- [ ] Templates (Consul template, Vault template)
- [ ] Log configuration (syslog, file, journald)
- [ ] User-defined metadata
- [ ] Migrate / resize / stop strategies
- [ ] Update (rolling, blue/green, canary)
- [ ] Prestart / poststop lifecycle hooks

---

## Phase 5: State & Persistence ☐

### Client State
- [ ] BoltDB or SQLite state store
- [ ] Allocation state tracking (running, completed, failed)
- [ ] Task state machine with recovery on restart

### Server State
- [ ] Raft log persistence
- [ ] Snapshot and restore
- [ ] Job index
- [ ] Evaluation index
- [ ] Allocation index
- [ ] Node index

---

## Phase 6: APIs & Interop ☐

### HTTP API
- [ ] `/v1/jobs` — CRUD for jobs
- [ ] `/v1/evaluations` — evaluation lifecycle
- [ ] `/v1/allocations` — allocation status
- [ ] `/v1/nodes` — node registration and status
- [ ] `/v1/agent` — agent health, members, self
- [ ] `/v1/status` — cluster leader, peers
- [ ] `/v1/operator` — Raft, snapshot, debug
- [ ] OpenAPI spec generation

### CLI
- [ ] `nomad-rs job run` / `stop` / `status` / `inspect`
- [ ] `nomad-rs node status` / `drain` / `eligibility`
- [ ] `nomad-rs server members` / `force-leave` / `join`
- [ ] `nomad-rs alloc status` / `logs` / `exec`
- [ ] `nomad-rs monitor`
- [ ] Tab completion (bash, zsh, fish)

---

## Phase 7: Production Hardening ☐

### Observability
- [ ] OpenTelemetry tracing + metrics
- [ ] Prometheus metrics endpoint
- [ ] Structured JSON log output
- [ ] Health check endpoints for K8s / Nomad itself
- [ ] pprof / debug endpoints

### Security
- [ ] mTLS between all components
- [ ] ACL system (capabilities + policies)
- [ ] Vault integration for secrets
- [ ] Workload identity / SPIFFE
- [ ] Token-based API auth
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

## Phase 8: Testing & CI ☐

- [ ] Unit tests for every module
- [ ] Integration tests (multi-node cluster in process)
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

| Resource | Link |
|----------|------|
| Nomad source (MPL-2.0) | `../nomad-original-ref` (local, v1.6.5) |
| Nomad docs | https://developer.hashicorp.com/nomad/docs |
| Nomad API spec | https://developer.hashicorp.com/nomad/api-docs |
| raft-rs | https://github.com/tikv/raft-rs |
| memberlist (Rust) | https://crates.io/crates/memberlist |

---

*Generated from reference: HashiCorp Nomad v1.6.5. Not a commitment — priority is gated by what the first real workload needs.*
