# CLAUDE.md — nomad-rs

A Rust rewrite of HashiCorp Nomad (scheduler, server, client agent). Edition 2024,
single binary crate. Toolchain is pinned to **1.96.0** by `rust-toolchain.toml` (carries
clippy + rustfmt), and `Cargo.toml`'s `rust-version` is set to match — this is a pinned
binary, not a library chasing a broad MSRV.

**Before you commit, the gate must be green:**

```
mise run check-all   # fmt --check, clippy (all+pedantic+cargo), check, test, doc
```

This file is a guide to the house style: **functional-programming discipline applied to
idiomatic Rust.** It is not "Rust pretending to be Haskell" — it is the small set of FP
habits that make this codebase easy to test and review, grounded in code that already
exists here.

---

## 1. The one rule: pure core, effects at the edge

Separate *deciding what to do* (a pure function of data) from *doing it* (IO, raft,
process spawn, the clock). Keep the decision pure and push the effect outward.

The scheduler is the canonical example (`src/scheduler.rs`):

```rust
// ✗ WRONG: decision and effect tangled. The placement policy is buried in the loop
//          that writes to the FSM, so you can't test "where would this go?" without a
//          live FSM, and there's no overall plan to inspect or reject.
fn schedule(eval: &Evaluation, fsm: &mut Fsm) -> Result<()> {
    for node in fsm.state().list_nodes() {
        if node.is_ready() {
            fsm.apply(Command::UpsertAlloc(make_alloc(eval, &node)))?; // effect in the loop
        }
    }
    Ok(())
}
```

```rust
// ✓ RIGHT: split the decision (pure) from the application (effect), compose at the edge.
// PURE: data in, a Plan (a description of placements) out. No IO, no clock, no raft.
pub fn process_eval(eval: &Evaluation, state: &StateStore) -> Plan { ... }

// EFFECT: a thin shell that performs the Plan against the FSM.
pub fn apply_plan(fsm: &mut Fsm, plan: &Plan) -> Result<()> { ... }

// COMPOSITION: wire the two at the call site.
pub fn process_and_apply(eval: &Evaluation, fsm: &mut Fsm) -> Result<Plan> {
    let plan = process_eval(eval, fsm.state());
    apply_plan(fsm, &plan)?;
    Ok(plan)
}
```

A function is one of three things. Know which you're writing:

| Kind | Does | Lives in |
| --- | --- | --- |
| **Pure transform** | data → data, no effects | `scheduler.rs`, validation, `as_str`/`is_*` predicates |
| **Effect shell** | IO / raft / spawn / clock | `rpc.rs`, `driver.rs`, `membership.rs`, `client_state.rs`, `raft_log.rs` |
| **Composition** | wires the two | `server.rs` loop, `process_and_apply` |

When a function mixes all three, the pure part is the one worth extracting — it returns a
*value describing the effect* (a `Plan`, a `Vec<Command>`), and the shell executes it.

Because the core is pure, it is the cheap place to test — no mocks, no setup. Prefer
*property* tests over examples where a law holds, e.g. **a `Plan` never oversubscribes a
node** or **`process_eval` is idempotent for a fixed state**. (No `proptest` in-tree yet; the
pure scheduler functions — `process_eval`, `free_capacity`, `fits` — are where it would pay
first.)

### Ports and adapters (hexagonal), in moderation

Pure-core / effects-at-the-edge is the load-bearing half of *hexagonal architecture* (ports
& adapters): the domain doesn't know how it's invoked or where its data lands. Take the part
that pays — domain logic (`scheduler`, `fsm`, validation) never imports `tokio`, `rusqlite`,
or a socket — and stop there.

Do **not** invert every dependency behind a trait. The one real *port* here is
`trait TaskDriver`: an open set of execution backends (`exec`, `docker`, `raw_exec`) that are
genuine adapters. Everywhere else the "adapter" is concrete — one in-memory `StateStore`, one
raft — because there is exactly one implementation and the effect is in-process. A trait +
`dyn` at those seams is the ceremony §6 warns against: indirection you pay for and
flexibility you never use. Add a port when a *second* real adapter appears, not before.

And a port need not be a trait. The lighter form is *functional DI* — the core asks for a
capability as a plain function parameter, and the composition root binds the real one:

```rust
// the "port" is a function type; no trait, no dyn.
fn place(find_node: impl Fn(&NodeId) -> Option<Node>, eval: &Evaluation) -> Plan { ... }
// composition root supplies the adapter:
place(|id| state.get_node(id.as_str()), &eval);
```

Reach for a `trait` only when one capability bundles several methods or several live adapters
(as `TaskDriver` does); otherwise a function parameter is the whole pattern.

**Divergence from the textbook:** the FP canon separates rich domain types from flat
serialization DTOs (domain pure, DTO at the gate). We serialize the domain types directly
with serde (`StateStore` save/load, the raft log), and the `#[serde(transparent)]` newtypes
keep the wire stable. Split out DTOs only once the wire format and the domain model start to
drift apart — not pre-emptively.

---

## 2. Make illegal states unrepresentable

Prefer the compiler over runtime checks. Two tools cover most cases here.

**Algebraic data types.** Rust's `enum` and `struct` are ADTs: an `enum` is a *sum* (a value
is exactly one of N variants — `ClientStatus` is one of five), a `struct` is a *product* (all
of its fields at once). The design lever is composing them into a **product of sums** so that
illegal combinations can't be constructed — `Allocation` is a product whose `client_status`
and `desired_status` are sums, so there is no way to build an allocation in an unlisted
status. Corollary: when a *combination* of fields is illegal, collapse it into the type —
two booleans (4 states, some invalid) become one enum that lists only the legal states.

**Sum types for state machines.** Never model a state with a `bool` or a `String`. Every
lifecycle here is an enum with an exhaustive `match` (`src/alloc.rs`, `src/eval.rs`,
`src/node.rs`):

```rust
pub enum ClientStatus { Pending, Running, Complete, Failed, Lost }

impl ClientStatus {
    // Exhaustive match — NO `_ =>` arm. Adding a variant becomes a compile error
    // here, which is the point.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Lost => "lost",
        }
    }
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed | Self::Lost)
    }
}
```

```rust
// ✗ WRONG: stringly-typed status. The typo "runnnig" compiles, and there is no
//          exhaustiveness — add a state later and nothing reminds you to handle it.
struct Alloc { status: String }
if alloc.status == "complete" || alloc.status == "failed" { /* terminal? */ }

// ✓ RIGHT: the enum above. A new variant is a compile error at every `match`, and
//          the predicate is named once.
if alloc.client_status.is_terminal() { /* ... */ }
```

Use `matches!` for predicates. Avoid a wildcard `_` arm on a domain enum — it silently
swallows new variants. (`use_self` is denied, so write `Self::`, not `ClientStatus::`.)

**Newtypes for confusable values.** Raw `String` ids let you pass a node id where a job id
belongs. The four domain ids are newtypes (`src/id.rs`), generated by one macro, each
`#[serde(transparent)]` so the wire format is unchanged:

```rust
pub struct JobId(String);   // also NodeId, AllocId, EvalId

// ✗ WRONG: ids are bare String — swapping the two arguments compiles and ships a bug.
fn allocs_on(node_id: String, job_id: String) -> Vec<Allocation> { /* ... */ }
allocs_on(alloc.job_id, alloc.node_id);    // reversed — silently wrong

// ✓ RIGHT: distinct newtypes — the swap is a compile error.
fn allocs_on(node_id: &NodeId, job_id: &JobId) -> Vec<Allocation> { /* ... */ }
allocs_on(&alloc.job_id, &alloc.node_id);  // error[E0308]: expected `NodeId`, found `JobId`
```

Give a newtype just enough surface to keep call sites clean — here `as_str`, `is_empty`,
`From<&str>`, `Display`, `Borrow<str>` (so `HashMap<NodeId, _>` is queryable by `&str`),
`PartialEq<&str>`. Do **not** add `Deref<Target = str>`; that quietly defeats the type.
Convert at the persistence edge (see `client_state.rs`: `.as_str()` on write,
`row.get::<_, String>(n)?.into()` on read) rather than coupling the id to the storage layer.
A newtype is **zero-cost** — it compiles to the wrapped `String`, so this safety is free at
runtime (the criterion benches confirm no regression).

**Smart constructors, where invariants are real.** The canonical pattern pairs a wrapper with
a private field and `fn new(s) -> Result<Self, _>` that validates on construction, so the value
is *valid-by-type* thereafter. Our id newtypes deliberately skip that — any string is a
syntactically valid id, and validation lives at the aggregate boundary
(`Job::validate`, `Allocation::validate`). Use a smart constructor when a wrapper has a *real*
invariant (a port in `1..=65535`, a bounded quantity); skip the ceremony for opaque ids.

**Polymorphism: prefer enums and static dispatch.** A *closed* set of cases is a sum type
(the status enums above), not a trait hierarchy. When you genuinely need pluggable
*behavior* — the task-execution backends behind `trait TaskDriver` (`ExecDriver`,
`RawExecDriver`, `DockerDriver` in `src/driver.rs`) — use a trait, and prefer **static**
dispatch (`fn run<D: TaskDriver>(..)` or `impl Trait`), which monomorphizes to zero runtime
cost. Reach for `Box<dyn TaskDriver>` only when you must hold a heterogeneous set chosen at
runtime — that buys a vtable indirection and an allocation you usually don't need here.

---

## 3. Errors are values, not panics

`unwrap_used`, `expect_used`, `panic`, and `dbg_macro` are **denied** in non-test code
(`Cargo.toml [lints.clippy]`; `clippy.toml` permits them only under `#[cfg(test)]`). Failure
is a return type.

- One typed error enum via `thiserror`, with a project `Result` alias (`src/error.rs`):
  `pub type Result<T> = std::result::Result<T, Error>;`. Use `thiserror`, not `anyhow` —
  this is library-shaped code and consumers want typed variants.
- `#[from]` wires foreign errors (`io`, `serde_json`, `toml`, `rusqlite`) so `?` just works.
- Propagate with `?`. Reach for combinators before `match`:

```rust
// ✗ WRONG: unwrap panics on a missing job or an IO failure — and the lint denies it.
let job = state.get_job(name).unwrap();
let bytes = std::fs::read(path).unwrap();

// ✓ RIGHT: absence → Option (let-else / combinator); real failure → `?`.
let Some(job) = state.get_job(name) else { return plan };   // early-out on absence
let bytes = std::fs::read(path)?;                           // Error::Io via #[from]

// fold the Option rather than branching on it (this is `desired_count`):
state.get_job(eval.job_id.as_str())
    .map_or(0, |j| j.task_groups.iter().map(|g| g.count.max(0)).sum())
```

- `Option<T>` when absence is normal (`get`/`find`/`lookup`); `Result<T>` when it's a real
  failure. Don't reach for `Result` to mean "not found".

---

## 4. Iterators over loops — but pragmatically

Express transforms as `map`/`filter`/`fold`/`all`/`any` when the body is a transform
(`scheduler.rs` `free_capacity`, `group_demand`, `meets_constraints`):

```rust
// ✗ WRONG: index loop + a mutable flag to express "do all constraints hold?"
let mut ok = true;
for i in 0..group.constraints.len() {
    if !group.constraints[i].satisfied_by(&node.attributes) { ok = false; break; }
}

// ✓ RIGHT: the combinator *is* the sentence (real code in `meets_constraints`).
group.constraints.iter().all(|c| c.satisfied_by(&node.attributes))
```

Keep an imperative `for` loop when the logic is genuinely stateful or index-driven — e.g.
the placement loop in `process_eval` mutates running free-capacity as it reserves nodes.
A `fold` carrying a tuple accumulator there would be *less* readable. Lazy means writing
less code that reads clearly, not forcing a combinator that obscures intent.

---

## 5. Idiomatic-Rust quick rules

| Do | Don't |
| --- | --- |
| `?` to propagate | `match` that just re-wraps and returns |
| exhaustive `match` on domain enums | `_ =>` catch-all that hides new variants |
| `matches!(x, A \| B)` for predicates | a `match` returning `true`/`false` |
| newtype for an id/quantity with meaning | bare `String`/`i32` across boundaries |
| `let Some(x) = .. else { return .. }` | a pyramid of `if let` |
| `#[must_use]` on pure constructors/queries | (lint `must_use_candidate` is denied) |
| return a new value from a transform | mutate a borrowed argument in place |
| `.clone()` to return an owned snapshot (`StateStore::list_*`) | `.clone()` to silence a borrow-checker error |
| doc every item (`missing_docs*` denied) | undocumented `pub` / private items |

`&mut self` is fine — and correct — where you own and supervise live state
(`taskrunner`/`allocrunner` drive real processes). Don't force return-new immutability on
something that legitimately holds a handle.

---

## 6. When NOT to FP-ify (read this before "refactoring for purity")

This codebase's effects are **in-process and cheap**: single-node in-memory raft
(`RaftNode` owns `Fsm` owns `StateStore`), in-memory eval queue, tests that exercise the
real components with no mocks. The usual FP payoff — *extract a pure core so you don't have
to mock an expensive effect* — barely applies when there's nothing expensive to mock.

So **do not**, without a concrete reason:

- inject a `Clock`/`now: Instant` everywhere — the queue's time-dependent tests are already
  deterministic via timeout extremes (`Duration::ZERO` / `from_hours(1)`);
- wrap effects in a trait/effect-system layer, or add `type-state` `PhantomData` ceremony,
  when a plain function and an enum already encode the rule;
- split a 15-line in-process handler into pure-core + shell purely for testability you
  already have.

Force purity only where it removes a real mock, a real bug class, or real duplication. The
FP refactor of this repo was deliberately scoped to the newtype ids in §2 for exactly this
reason. When in doubt, ship the smaller version and say what you skipped.
