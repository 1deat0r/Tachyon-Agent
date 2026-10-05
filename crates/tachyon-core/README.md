# tachyon-core

The Task Supervisor: single logical writer of canonical task state (spec §3,
§15). Every state transition is journalled through `tachyon-store` before the
caller is answered; snapshots let a restarted process rebuild state from
snapshot plus journal tail (spec §18, §41).

Provider-specific types do not leak here. Models propose; Tachyon validates
and executes. Completion is gated by acceptance and verification, never by
model self-report.

## Executed verification

```bash
cargo fmt -p tachyon-core
cargo test -p tachyon-core
cargo clippy -p tachyon-core --all-targets -- -D warnings
```
