# tachyon-scheduler

Dependency-aware, effect-aware DAG execution (spec §11–§14). Readiness, atomic
conflict and resource grants, critical-path priority, retries, timeouts, and
structured cancellation live here; executors own only single-node work.

No two running nodes may hold conflicting access sets. Unknown or
non-idempotent effects are never blindly replayed after a crash.

## Executed verification

```bash
cargo fmt -p tachyon-scheduler
cargo test -p tachyon-scheduler
cargo clippy -p tachyon-scheduler --all-targets -- -D warnings
```
