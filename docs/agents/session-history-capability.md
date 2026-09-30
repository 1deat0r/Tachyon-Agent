# Capability: gateway session identity and ordered history (`GetSession`, `CreateSession` root)

Capability documentation required by `AGENTS.md` § New capability checklist. Covers the gateway-level surface added for issue #57 slice (a) (durable session lookup and ordered replay, ADR-0005's first release blocker).

## Why deterministic code cannot already solve it

The capability itself is fully deterministic — no model/Jev call is involved. It exists because clients (today's CLI/TUI, tomorrow's ACP adapter) cannot assemble this view themselves: canonical task state and the durable journal are owned by the gateway/Supervisor, and architecture invariants forbid any client from becoming a second owner of that state. The deterministic projection lives gateway-side so clients only ever read.

## Input / output schema

- **`GetSession { session_id }`** → `Ok { session_id, created_at, workspace_root: string | null, turns: [{ turn_seq: int, task_id, status, conversation: [{role, content, …}] }] }` — turns strictly ordered by `turn_seq`. Errors: `unknown_session`, `corrupt_state`.
- **`CreateSession { workspace_root?: string }`** → legacy `{ session_id }`. When `workspace_root` is supplied it must be an absolute path that passes `canonical_workspace_root` (exists / canonicalize / is-dir); otherwise `workspace_not_absolute` / `workspace_not_found` / `workspace_not_canonical` / `workspace_not_a_dir`.

## Access set

Read-only against the store: `sessions`, `tasks`, `task_events` rows for the requested session. `CreateSession` writes one `sessions` row (with optional root). No filesystem writes; canonicalization performs filesystem metadata reads only.

## Effect class

`GetSession`: pure local read (no effect, no idempotency key required). `CreateSession`: local keyed effect (session-id key), idempotent-by-construction only in the sense that a duplicate id fails the primary key — no blind replay path exists.

## Idempotency

`GetSession` is naturally idempotent (reads only). `CreateSession` with an explicit id is at-most-once by primary-key conflict; callers must not retry-and-duplicate (same lost-response discipline as existing commands).

## Resource claim

None. No workspace lease, no concurrency slot; reads do not contend with runs (they never acquire ownership).

## Cancellation behavior

Not cancellable mid-flight — bounded local reads with no long-running work. No interaction with task cancellation or approval state.

## Retry policy

Safe to retry `GetSession` (idempotent read). `CreateSession` retries only when the outcome is known-failed (typed error received), never after an ambiguous/lost response.

## Verification method

Gateway round-trip tests (primary seam): `session_root.rs`, `turn_sequence.rs`, `session_history.rs` — round-trip, restart byte-equality, rejection codes, read-only state fingerprints. Store seam: `tachyon-store` tests for persistence, per-session uniqueness, deterministic legacy backfill. Gate: `cargo verify`.

## Crash-recovery behavior

History is a read-only projection over the append-only journal plus snapshots — recovery of task state follows the existing Supervisor/store reconcile path; `GetSession` never recovers, spawns, or re-enters anything. Restart byte-equality tests pin this.

## Expected latency class

Interactive local IPC: single-digit SQLite reads plus per-turn journal projection; O(turns in session) with no model calls. Not on any critical path of a running task.
