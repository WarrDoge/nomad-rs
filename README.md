# nomad-rs

Nomad rewrite in Rust under Apache License 2.0.

A from-scratch reimplementation of the HashiCorp Nomad scheduler, client agent,
and server. Built test-first: every subsystem ships real types with `todo!()`
behaviour and a full test suite that is **red until implemented**. Implementing
a module means making its `todo!()`s satisfy the existing tests.

See [TODO.md](TODO.md) for the implementation backlog and per-module status.

## Toolchain

Pinned in [rust-toolchain.toml](rust-toolchain.toml) (rustup auto-respects it).
[mise](https://mise.jdx.dev) drives the dev tasks.

## Build & check

```sh
mise run check-all   # fmt + clippy + check + test
mise run test        # tests + docs
mise run lint        # fmt + clippy + cargo-deny
mise run fix         # auto-fix fmt + clippy
```

Or with cargo directly:

```sh
cargo test                      # 64 green; the rest are #[ignore]'d red specs
cargo test -- --ignored         # the red list (unimplemented behaviour)
cargo clippy --all-targets
```
