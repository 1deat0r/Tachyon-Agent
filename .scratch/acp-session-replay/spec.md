# Spec — Durable session identity, turn association, and ordered replay (issue #57 slice a)

**Tracker status:** ready-for-agent
**Parent:** [#57 ACP v1 distribution through the local gateway](https://github.com/1deat0r/Tachyon-Agent/issues/57) — planned follow-on slice (a)
**Authority:** ADR-0005 (ACP v1 through the local gateway), AGENTS.md invariants, CONTEXT.md glossary
**Provenance:** derived from open issues (auto-workflow, `state.md` origin `imported-untrusted`, goal re-used on resume)

## Problem Statement

Tachyon's gateway records sessions as an id and a creation time only. There is no durable workspace identity bound to a session, no monotonic record of which task was which turn, and no API that returns a session's ordered history. The future ACP agent cannot implement `session/load` — which ADR-0005 makes a release requirement with complete, ordered, restart-surviving replay — and every later ACP slice (stdio lifecycle, prompt turns, cancellation bridging) is preconditioned on this missing substrate. Existing gateway clients cannot inspect a session's history at all; per-task state exists, but session-level identity and ordering do not.

## Solution

From the perspective of a gateway client (today's CLI/TUI, tomorrow's ACP adapter):

- A session can be created with an absolute workspace root; the gateway validates it with the existing workspace validation (`canonical_workspace_root`: exists / canonicalize / is-dir — no grant registry exists, so grant enforcement stays at run admission) and persists it as the session's **Session root**. Sessions created without a root keep working exactly as before (the future ACP `session/new` layer will be the one that requires a root, per ADR-0005).
- Every task created inside a session is stamped with a per-session monotonic **turn sequence** at creation time, so the turn→Task association is durable before any run can start and terminal tasks are never reused as new turns.
- A new `GetSession` gateway command returns the session's identity, root, and **Session history**: the turns in stable session order, each with its task's canonical status and conversation. Replay is derived read-only from durable canonical records — it never creates work, re-enters a driver, or repeats effects — and it survives gateway restarts unchanged.

## User Stories

1. As an ACP adapter developer, I want a gateway API that returns a session's durable identity, Session root, and ordered turn history, so that I can implement `session/load` replay without relying on process-local adapter memory.
2. As an ACP adapter developer, I want the turn→Task association persisted durably before any run starts, so that an ambiguous or interrupted prompt can be reconciled on load instead of blindly retried (ADR-0005).
3. As a gateway client author, I want `CreateSession` to accept an absolute workspace root that is workspace-validated at creation (relative input rejected), so that session identity can never silently resolve against gateway-local state.
4. As a gateway client author, I want `CreateSession` without a root to keep its current behavior, so that existing CLI/TUI flows are not broken by this slice.
5. As an operator, I want `GetSession` to fail with a clear, typed error for an unknown session id, so that clients can distinguish "never existed" from transport failure.
6. As an operator, I want session history to be byte-identical across a gateway restart, so that crash recovery never changes what a replay would report.
7. As a TUI user, I want a session's history to appear in stable creation order even when tasks are created in the same instant, so that ordering never depends on wall-clock tie-breaking.
8. As the Task Supervisor, I want the turn sequence assigned atomically at task creation under the store's uniqueness constraint, so that two turns can never claim the same sequence number.
9. As a security reviewer, I want `GetSession` to be strictly read-only, so that no client can mint state, start work, or mutate a session by fetching history.
10. As a security reviewer, I want a relative or non-canonicalizable root rejected at session creation with typed errors, so that a session can never bind a path that fails workspace validation.
11. As a gateway client author, I want `GetSession` history to include each turn's canonical task status, so that a client can tell a completed, failed, cancelled, or in-flight turn apart without fetching every task individually.
12. As a developer, I want the schema change to be additive (nullable column, new index) so that existing state databases upgrade without data loss or downtime hacks.
13. As a developer, I want replay content derived from canonical task state rather than a second message store, so that there is exactly one source of truth for conversation content.
14. As a future ACP client, I want load to replay history only — never restart a task or re-enter a driver — so that recovery remains an explicit Supervisor decision (ADR-0005, CONTEXT.md **Driver re-entry**).
15. As a reviewer of issue #57, I want this slice independently verifiable with gateway round-trip and restart tests, so that the release blocker "durable session lookup and ordered replay" is closed with evidence, not assertion.

## Implementation Decisions

- **Layers touched:** `tachyon-store` (schema migration + persistence queries), `tachyon-protocol` (additive command/response variants), `tachyon-gateway` (command dispatch, read-only handler), `tachyon-core` (read-only `project_task_state` projection reusing the canonical journal-recovery path — added after review pass 1 because the history content must come from canonical state without a second store). No new crate; the ACP adapter crate is deferred to a later slice.
- **Schema (additive):** `sessions` gains a nullable canonical workspace-root column; `tasks` gains a per-session monotonic turn-sequence column with a uniqueness constraint scoped to the session. Existing rows upgrade with well-defined defaults (legacy sessions have no root; existing tasks receive their turn sequence deterministically by creation order during migration).
- **API contract:** new `GetSession { session_id }` command (protocol stays v2, additive variant) returning session identity, optional root, and ordered turns — each turn exposing sequence, task id, canonical `TaskStatus`, and the task's conversation as canonical state. Unknown session → typed error response.
- **`CreateSession`:** gains an optional workspace root; the input must be absolute (relative → typed rejection). When supplied: validate with `canonical_workspace_root` (exists / canonicalize / is-dir — the codebase's existing workspace validation; no grant registry exists anywhere, so session creation performs no grant check — trust enforcement remains at run admission as today, and ADR-0005's authorize-at-`session/new` rule binds when the ACP layer is built), then persist. When absent: legacy behavior, no root bound.
- **Turn association:** assigned atomically at task creation, persisted before `StartRun` can admit work; terminal tasks are never reused; overlapping turns within one session are governed by existing task-status rules (no new concurrency semantics in this slice).
- **Replay derivation:** read-only projection over canonical task snapshots and the append-only task journal in turn order. No new message/event store, no writes on the read path.
- **Ownership invariants unchanged:** the Task Supervisor remains the sole logical writer of task state; `GetSession` observes, never writes. Remote gateway mode remains disabled by default.
- **Concurrency/limits:** no behavior change to `Subscribe`, `SendMessage`, cancellation, approvals, or driver re-entry. Idempotency/lost-response semantics for `CreateTask` are unchanged (ADR-0005's reconciliation story builds on this association later).

## Testing Decisions

- **What makes a good test:** external behavior only — command in, observable response/state out. No assertions on internal call counts, private helpers, or storage layout beyond the public contract. Tests prove the contract a client depends on: ordering, durability, rejection, read-only-ness.
- **Primary seam (existing, preferred):** gateway client round-trip tests driving real commands through the server — prior art: the existing gateway test suite (cancellation, recovery, streaming round-trips).
- **Secondary seam (existing):** store-level tests for migration, per-session uniqueness, and ordering — prior art: existing `tachyon-store` tests.
- **Restart-replay test:** create a session with root and several tasks, tear down and restart the gateway/store, `GetSession` returns the identical ordered history — prior art: the existing recovery/restart test pattern in the gateway suite.
- **Rejection tests:** unknown session id → typed error; non-existent/non-canonicalizable/not-a-dir root at `CreateSession` → rejected (`workspace_not_found`/`workspace_not_canonical`/`workspace_not_a_dir`); relative root → rejected (`workspace_not_absolute`); `GetSession` performs no writes (state unchanged before/after).
- **Backward-compat test:** legacy `CreateSession` without root still succeeds and `GetSession` reports an absent root.
- **Verification command:** `cargo test` for the touched crates plus the repo-standard `cargo verify` (per `docs/DEVELOPMENT_WORKFLOW.md`), executed only after the Phase 6 load-chain inspection.

## Out of Scope

- The ACP wire adapter itself: stdio JSON-RPC lifecycle, `initialize`/`session/new`/`session/prompt`/`session/load` handlers, `tachyon-acp` crate (issue #57 slice c).
- stdio MCP server integration for session-supplied servers (slice b).
- Cancellation acknowledgement / one-shot approval bridging and drain-facing APIs (slice d).
- Secure hosted-provider path (slice e); independent acceptance contracts and pinned live-model evaluation (slice f).
- Advertising any ACP capability (e.g. `loadSession`) — nothing in this slice is client-visible via ACP.
- Mid-turn steering semantics, `CreateTask`/`StartRun` idempotency keys, disconnect/reconnect attach semantics.
- Changes to the frozen M14 record, §46 deferrals, remote-gateway defaults, or any architecture requirement (none are changed; this implements ADR-0005's first release blocker).
- Committing or pushing: this auto-workflow run leaves all work in the working tree (skill rule 5).

## Further Notes

- ADR-0005 lists the release blockers in order; this spec closes the first: "durable session lookup and ordered replay … in separately scoped, independently verified small implementation slices."
- Glossary: **Session root** and **Session history** were added to `CONTEXT.md` during grilling; specs and code should use those terms exactly.
- No new ADR: the architectural decision is already made (ADR-0005 revision 2, expert-reviewed); this slice is its implementation with only additive, reversible schema/API changes.
- Deferred follow-ups stay tracked on issue #57's planned-follow-on list; the remaining slices are not broken into tickets by this spec (dedup on later re-entry).
