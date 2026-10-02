# Capability: idempotent task creation reconciliation (`CreateTask` idempotency keys)

Capability documentation required by `AGENTS.md` § New capability checklist. Covers the gateway-level surface added for issue #57 ticket 01 (safe prompt creation reconciliation, ADR-0005's second release blocker). Glossary term: **Idempotency key** (CONTEXT.md) — client-supplied request token per Session; distinct from effect-level idempotency (spec §19).

## Why deterministic code cannot already solve it

No deterministic code can decide whether a client whose `CreateTask` response was lost should get the original task or a new one: the ambiguity lives in the client's lost round trip, not in gateway state. The gateway instead persists the decision at creation time — the **Idempotency key** row commits in the same transaction as the task, so every retry resolves deterministically against durable state (replay or typed conflict) instead of guessing. The capability itself runs no model/Jev call.

## Input / output schema

- **`CreateTask { session_id, objective, idempotency_key?: string }`** → `Ok { task_id, status: "Created", turn_seq: int }` — `turn_seq` is the durable per-session stamp `GetSession` reports for that turn (additive; present with or without a key).
- `idempotency_key`, when present: opaque, non-empty, ≤128 bytes (no semantic parsing). Empty/oversized → typed `invalid_idempotency_key`. Absent → legacy semantics (every send creates a new task), otherwise unchanged.
- Retry semantics: same `(session_id, key)` + identical request → the stored success response replays **byte-identically** (same `task_id`, `status`, `turn_seq`; no second task or turn). Same key + different `objective` → typed `idempotency_key_conflict`, no state change.
- Other errors: `unknown_session` (session must exist before any key lookup), plus the pre-existing core/store codes on the create path.
- Persistence: migration `0005_create_task_idempotency.sql`, table `create_task_idempotency (session_id, "key", fingerprint, response_json, created_at)` with `UNIQUE (session_id, "key")`. `fingerprint` is BLAKE3-hex over the canonical string `session_id + "\n" + objective` (deterministic, restart-stable, no wall-clock).

## Access set

Gateway → store only. Writes: the existing atomic create transaction over `sessions` (existence check), `tasks`, `task_events` (seq 0), plus — when a key is supplied — one `create_task_idempotency` row in the SAME transaction. Reads: one indexed `SELECT` on `create_task_idempotency` (pre-create lookup and race recovery), one `SELECT turn_seq` for response assembly, and `ListTasks`/`GetSession`-visible rows afterwards. Replay performs reads only. No filesystem writes beyond the existing `state.db` WAL; no network beyond local IPC.

## Effect class

Creation path: local creation effect — one SQLite transaction committing a task row, its seq-0 journal event, and optionally the key row; never a model/provider call, never an external effect (the IR effect machinery of spec §19 is untouched). Replay path: pure local read — no effect, no driver entry, no supervisor spawn (ADR-0005: reconciliation is a read of a stored response).

## Idempotency

With a key: at-most-once creation keyed by `UNIQUE (session_id, "key")`; losers of a concurrency race roll back their whole create and answer from the winner's record. The stored response is replayed verbatim — this is the **Idempotency key** mechanism, not effect idempotency. Without a key: intentionally non-idempotent (legacy duplicate-per-send semantics, pinned by test). Effect-level idempotency (spec §19 `Idempotency`, `EffectClass`) is unchanged.

## Resource claim

None beyond what create already claims: `TaskOwnership` acquire for the newly minted task id, dropped on refusal. Replay claims nothing — no ownership, no workspace lease, no run-admission slot, no supervisor registry entry. Superseded creates release their ownership guard via the rollback.

## Cancellation behavior

Not cancellable mid-flight — bounded local IPC handlers (key validation, indexed SELECTs, one SQLite transaction). No interaction with task cancellation, pause/resume, run admission, or approval state; a refused/conflicted create leaves every existing resource untouched.

## Retry policy

Keyed `CreateTask`: retry freely after any lost, dropped, or ambiguous response (that is the capability's contract) — same key + same request replays; same key + changed request returns `idempotency_key_conflict` with no state change; after a gateway restart between commit and response, the retry still replays. Keyless `CreateTask`: retry only after a received typed error, never after ambiguity (legacy duplicate-per-send). Validation refusals (`invalid_idempotency_key`) are permanent for that key value.

## Verification method

Gateway seam (`crates/tachyon-gateway/tests/idempotency.rs`): `keyed_retry_replays_byte_identical_response_with_one_task`, `same_key_with_changed_objective_is_a_typed_conflict`, `keyed_retry_after_restart_still_replays_one_task`, `concurrent_same_key_sends_create_exactly_one_task`, `keyless_duplicate_sends_still_create_two_tasks`, `create_task_turn_seq_matches_get_session`, `empty_and_oversized_idempotency_keys_are_refused`. Store seam (`crates/tachyon-store/src/lib.rs` tests): `create_task_with_idempotency_records_response_atomically`, `duplicate_idempotency_key_rolls_back_the_entire_create`, `fingerprint_mismatch_under_same_key_leaves_original_intact`, `idempotency_record_survives_store_reopen_for_replay`. Gate: `cargo verify` (the repo gate — includes fmt, workspace check/tests, and strict `clippy -D warnings`).

## Crash-recovery behavior

The key row commits iff the task + seq-0 journal event commit (one SQLite transaction, write-mutex serialized): a crash between commit and response leaves both durable, so a post-restart retry replays the stored response instead of duplicating; a crash before commit leaves neither. Replay never recovers, spawns, or re-enters anything — the task's own recovery on first post-restart access follows the existing Supervisor/store reconcile path unchanged. Pinned by the restart-retry test and the store reopen test. No expiry/GC of key rows this slice (MVP scale, noted for later evidence-driven revisitation).

## Expected latency class

Interactive local IPC, O(1) in session history: keyed create adds one indexed SELECT plus one INSERT to the existing transaction; replay is a single indexed SELECT (no task creation, no journal write, no projection over turns). No model calls; not on any critical path of a running task.
