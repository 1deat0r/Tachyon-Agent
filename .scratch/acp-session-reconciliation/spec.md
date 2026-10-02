# Spec — Safe task creation/start reconciliation (issue #57, ADR-0005 release blocker 2)

**Tracker status:** ready-for-agent
**Parent:** [#57 ACP v1 distribution through the local gateway](https://github.com/1deat0r/Tachyon-Agent/issues/57) — ADR-0005 second release blocker, "safe prompt creation/start reconciliation"
**Authority:** ADR-0005 (Gateway and ownership boundary), AGENTS.md invariants, CONTEXT.md glossary
**Provenance:** `derived:open-issues` (auto-workflow fresh-goal reopen; prior slice (a) landed as `b7ae280`)

## Problem Statement

A gateway client that sends `CreateTask` and loses the response (connection drop, restart between commit and reply) has no safe way to retry: every retry mints a brand-new task and a new turn, silently duplicating work. The envelope `request_id` is documented only for correlation and is never stored or deduplicated. `StartRun` retries are deduplicated only by an in-memory admission map whose behavior across a gateway restart mid-run is unpinned by tests. ADR-0005 forbids exactly this blind-retry pattern ("If task creation/start or its response is ambiguous, do not retry `StartRun` or create a duplicate") and makes safe reconciliation a release blocker before ACP can be called supported. Without a gateway-level contract, the future ACP adapter has no way to honor it.

## Solution

From the perspective of a gateway client (today's CLI/TUI, tomorrow's ACP adapter):

- `CreateTask` accepts an optional **Idempotency key**. The gateway persists the key, a fingerprint of the request, and the original success response in the same transaction that creates the task. A retry with the same key and the same request replays the stored response verbatim — same `task_id`, no second task, no second turn. The same key with a different request fails with a typed `idempotency_key_conflict`. No key means exactly today's behavior.
- The `CreateTask` success response now includes the task's `turn_seq`, so a client that lost a response can correlate its turn through `GetSession` even without a key.
- `StartRun` retries against a task whose run is active never cause a second driver spawn: while the original gateway era is still up the answer is the typed `run_already_active` refusal; after a restart the answer is the existing recovery/re-entry path or `run_already_active` — both pinned by tests, with the single-driver invariant asserted across the restart.

## User Stories

1. As an ACP adapter developer, I want to retry a `CreateTask` with the same idempotency key after a lost response, so that I never create a duplicate turn for one user prompt (ADR-0005).
2. As an ACP adapter developer, I want a retry with the same key to return the byte-identical original response, so that reconciliation is a pure replay rather than a guess.
3. As an ACP adapter developer, I want reuse of a key with a different objective to fail with a typed conflict error, so that a client bug can never silently overwrite or confuse two prompts.
4. As a CLI/TUI client author, I want the idempotency key to be optional, so that existing callers behave exactly as before without code changes.
5. As an ACP adapter developer, I want `CreateTask` to return the `turn_seq` it minted, so that after any ambiguity I can find my turn in `GetSession` history and reconcile without a key at all.
6. As an operator, I want the key record committed atomically with the task, so that a crash between execution and response still replays correctly after restart.
7. As an operator, I want a retried `StartRun` for an already-active run to return `run_already_active` — before and after a gateway restart — so that a lost response can never double-spawn a driver for one task.
8. As a security reviewer, I want keys scoped per Session and bounded in size (1..=128 bytes, non-empty, opaque), so that cross-session replay and unbounded storage through the key channel are impossible.
9. As a security reviewer, I want a duplicate-key race to resolve to exactly one winner enforced by the store's uniqueness constraint, so that concurrency cannot mint two tasks under one key.
10. As the Task Supervisor, I want idempotent replay to be a read of a stored response — no model call, no effect, no driver entry — so that retry handling never perturbs ownership invariants.
11. As a developer, I want the schema change additive (new table only, no column rewrites), so that existing state databases upgrade cleanly.
12. As a reviewer of issue #57, I want this blocker closed with duplicate-send, conflict, restart-retry, race, and no-double-spawn tests at the gateway seam, so that the ADR contract has executable evidence.

## Implementation Decisions

- **Layers touched:** `tachyon-store` (new idempotency table + record/replay queries), `tachyon-protocol` (additive `CreateTask.idempotency_key` field; additive `turn_seq` in the CreateTask success payload; typed `idempotency_key_conflict` error), `tachyon-gateway` (lookup/replay/fingerprint logic on the CreateTask path; response assembly), `tachyon-core` (one additive `create_task_with_idempotency` seam so the key row commits in the same transaction as the task — legacy `create_task` signature untouched; added after review pass 1, which caught the original "unchanged" claim as factually wrong), no new crate, no ACP wire code.
- **Schema (additive):** one new table keyed `UNIQUE(session_id, key)` holding the request fingerprint and the stored success-response JSON, inserted in the same transaction as the task row + seq-0 journal event (reuse the existing atomic create path; the key row commits iff the task commits).
- **Key contract:** optional field; when present: non-empty, ≤128 bytes, opaque (no semantic parsing). Same `(session_id, key)` + identical request fingerprint → replay stored response verbatim (same `task_id`, `status`, `turn_seq`). Same key + different fingerprint → typed `idempotency_key_conflict`. Absent key → legacy path, zero behavior change.
- **Fingerprint:** deterministic hash over the canonical request fields that matter for identity (`session_id`, `objective`) — no wall-clock, no ordering dependence.
- **Probe surface:** `CreateTask` success response gains `turn_seq`. `ListTasks`/`GetTask` are unchanged; `GetSession` already exposes ordered turns for keyless reconciliation.
- **StartRun contract:** no new field. The typed `run_already_active` refusal remains the single retry answer for an active run; tests pin that a restart mid-run followed by a `StartRun` retry never double-spawns (existing recovery/re-entry policy applies for the driver itself; this slice only pins the admission guarantee). No new "ambiguous" error code this slice — the ADR's ambiguous/in-progress result belongs to the future ACP `session/load` layer.
- **Ownership invariants unchanged:** Supervisor remains sole logical writer; replay performs reads only; remote gateway mode untouched.
- **Retention:** no expiry/GC of key rows this slice (MVP scale); growth noted as fog for later evidence-driven revisitation.

## Testing Decisions

- **What makes a good test:** external behavior only — command in, observable response/state out; assert exactly one task row/turn exists after retries; no assertions on internal call structure.
- **Primary seam (existing):** gateway round-trip tests — prior art: `recovery.rs` (shutdown/restart), `run_path.rs` (`run_already_active`), `cancel_run.rs` (typed duplicate refusal), `session_root.rs`/`turn_sequence.rs` (typed errors, restart equality).
- **Secondary seam (existing):** store tests for same-transaction record+replay, fingerprint mismatch, and unique-index race (prior art: `concurrent_create_task_never_double_assigns_a_turn_seq`).
- **Required cases:** duplicate-send replay (same response bytes); conflict on changed objective; restart then retry → still replays exactly one task (server-side state after commit is identical whether or not the client received the first response — the response row commits with the task — and the store-side `idempotency_record_survives_store_reopen_for_replay` test pins that durability directly); concurrent same-key sends → exactly one task (rollback-under-race genuinely pinned at the store seam); no-key legacy duplicate → two tasks (documents legacy semantics); `CreateTask` response carries correct `turn_seq` matching `GetSession`; `StartRun` retry while active in the same gateway era → `run_already_active`; restart mid-run → retry yields recovery-path acceptance or `run_already_active`, never a second driver; key validation errors (empty/oversized).
- **Verification command:** `cargo verify` (repo gate), executed under this session's established Phase 6 provenance judgment.

## Out of Scope

- ACP wire adapter, `session/load` implementation, and the ADR's ambiguous/in-progress load result (future ACP layer).
- Idempotency keys for `StartRun`, `ResumeTask`, `Approve`, or any other command.
- `ListTasks`/`GetTask` schema additions; key expiry/GC; key row size quotas beyond the per-key 128-byte input bound.
- Effect-level idempotency changes (existing IR/effects machinery untouched); provider/hosted paths; MCP; cancellation bridging; live evaluation.
- Changes to the frozen M14 record, §46 deferrals, remote-gateway defaults, or architecture requirements.

## Further Notes

- ADR-0005 blocker order: blocker 1 (durable session lookup and ordered replay) closed by slice (a) (`b7ae280`); this spec closes blocker 2.
- Glossary: **Idempotency key** added to `CONTEXT.md` during grilling; use it exactly (distinct from effect idempotency).
- AGENTS.md new-capability checklist for this capability ships as `docs/agents/task-creation-reconciliation-capability.md` (acceptance item of ticket 01 — review finding from the prior slice made the checklist a hard requirement).
- No new ADR: ADR-0005 already decides the requirement; this is additive, reversible implementation.
