# Spec — Supervisor-owned cancellation drain + crash recovery (issue #57, ADR-0005 release blocker 4)

**Tracker status:** ready-for-agent
**Parent:** [#57 ACP v1 distribution through the local gateway](https://github.com/1deat0r/Tachyon-Agent/issues/57) — ADR-0005 fourth release blocker, "Supervisor-owned cancellation drain and crash recovery"
**Authority:** ADR-0005 (Approvals and cancellation), AGENTS.md invariants, CONTEXT.md glossary
**Provenance:** `derived:open-issues` (auto-workflow goal rotation; blockers 1–3 landed: replay `b7ae280`, reconciliation + MCP done-uncommitted in tree)

## Problem Statement

The synchronous drain barrier exists and is pinned: `cancel_run` flips the run token, awaits driver exit, then journals `Cancelled` only after the supervisor acknowledges effect-worker drain — and `cancel_run.rs:214-219` proves the ack is the barrier. But three edges are unwired. First, the MCP slice added two gateway-side park maps (`mcp_approvals`, `mcp_call_approvals`) that the supervisor's `release_parked` never reaches: cancelling a task whose MCP launch or call is parked leaves the park alive, so a later approval could launch client-supplied code for a cancelled task. Second, `execute_granted_mcp_call` holds the global `mcp_live` lock across the whole 60 s call bound, so one hung tool call blocks another session's deny, cancel, or call — a refusal path that cannot refuse promptly. Third, driver cancel does not promptly abort an in-flight `tools/call` round trip, and no test pins what a crash between cancel-intent and drain leaves behind. Until these close, "cancel wins over everything" holds for native effects but not for the MCP surface the prior slice added — and the future ACP adapter cannot report cancelled completion it can trust.

## Solution

From the perspective of a gateway client:

- Cancelling a task (or the session scope that owns it) expires gateway-side MCP launch and call parks exactly like supervisor parks: waiters are released as cancelled, the durable `mcp_approvals` rows record the outcome, and any late approve/deny for the dead id fails with a typed error and consumes nothing.
- Refusal paths never block behind a hung call: the live-server map is isolated per server (shard or remove/reinsert for the call duration), so deny, cancel, and other sessions' calls proceed while one call runs out its bound.
- Driver cancel aborts an in-flight MCP `tools/call` promptly — well under the 60 s bound — and the existing mid-call-death classification (`stopped` + `mcp_call_failed`, never success) applies unchanged. The ack bound is asserted by test, not by prose.
- Crash around cancel is pinned by fault-point tests: cancel-intent-then-crash keeps `Cancelled` terminal for the task while interrupted effects classify via the §19 matrix; restart expires MCP parks (already fail-closed — the tests prove the absence of resurrection, not just the presence of refusal); restart never downgrades a task to `Cancelled` (`kill_restart.rs:198` precedent holds).
- What does not change: the synchronous CancelTask barrier (ack-after-drain stays; `cancel_run.rs:214-219` must keep passing unmodified in intent), session-survives-disconnect, one-shot approval semantics with fresh ids after restart.

## User Stories

1. As an ACP adapter developer, I want cancelling a task to also kill its parked MCP launch/call approvals, so that no approval granted after cancel can ever start client-supplied code.
2. As an ACP adapter developer, I want a late approve for a cancelled park to fail with a typed error, so that a race between my approval and the user's cancel resolves deterministically.
3. As an operator, I want deny/cancel for one session to proceed while another session's MCP call hangs, so that a stuck tool cannot hold refusal paths hostage.
4. As an operator, I want driver cancel to abort a hung MCP call promptly with a test-asserted bound, so that CancelTask latency degrades gracefully instead of wedging for a minute.
5. As an operator, I want a crash between cancel-intent and drain to leave Cancelled terminal plus §19-classified effects — never silent success, never resurrection — so that post-crash state is explainable from the journal alone.
6. As the Task Supervisor, I want one cancel-wins rule across both park maps (supervisor + gateway MCP), so that approval/cancellation serialization is uniform and auditable.
7. As a reviewer of issue #57, I want this blocker closed with cancel-during-park, deny-during-hang, cancel-during-call, and crash-matrix tests at the gateway seam, so that the ADR contract has executable evidence.

## Implementation Decisions

- **Layers touched:** `tachyon-gateway` (cancel path reaches both MCP park maps + durable outcomes; live-map isolation; call abort on driver cancel), `tachyon-store` (park-expiry writes on the cancel path only if the existing `mcp_approvals`/`approvals` rows need new transitions — additive migration if so, none if the current outcome vocabulary covers it), `tachyon-core` (only if driver-cancel needs a new hook into the gateway-owned call future; prefer reusing the existing cancellation token plumbed to the call). No new crate, no new commands, no ACP wire code.
- **Park invalidation:** on task/session cancel, expire matching rows in `mcp_approvals` + `mcp_call_approvals` (durable outcome recorded), release waiters with the cancelled resolution, and make late decide a typed no-op error — mirroring `release_parked` semantics, not duplicating its table.
- **Isolation:** per-server locking for the live map (shard by `(session_id, server_id)` or remove/reinsert around the call); the global lock across the call round trip goes away. The in-code follow-up note from the MCP slice is resolved, not moved.
- **Abort:** the in-flight `tools/call` future becomes cancel-aware (drop the child handle / close the pipe on driver cancel so `recv` returns promptly); the existing stopped + `mcp_call_failed` classification is reused verbatim.
- **Crash matrix:** fault-point tests (`TACHYON_FAULT_POINT` prior art) stop at cancel-intent commit, kill, and assert reconcile: task terminal Cancelled, interrupted effects per §19, MCP parks expired, no post-restart resurrection of any approval id.
- **Ownership invariants unchanged:** Supervisor remains sole logical writer; gateway parks stay second-class mirrors of supervisor parks (same one-shot, same cancel-wins, same audit shape); remote gateway mode untouched.
- **Retention:** nothing new persisted beyond cancel outcomes; no GC questions arise.

## Testing Decisions

- **What makes a good test:** external behavior only — commands in, observable responses/state out; timing bounds generous but finite (assert prompt abort, e.g. well under the 60 s call bound, never exact).
- **Primary seam (existing):** gateway round-trip tests — prior art: `cancel_run.rs` (ack-as-barrier, lease release), `mcp_gated_launch.rs` / `mcp_mediated_call.rs` (fake stdio children), `restart_approval.rs` (expiry + typed refusal after restart).
- **Secondary seams (existing):** core `approval_wait.rs` shapes for cancel-during-wait; store tests for park-expiry transitions if new transitions are added.
- **Required cases:** cancel-during-parked-MCP-launch releases + late approve typed-errors; cancel-during-parked-MCP-call releases + late grant ignored; deny proceeds while another session's call hangs (two-session test); cancel during a hung call aborts promptly with `stopped` + `mcp_call_failed`; fault-point cancel-intent crash keeps Cancelled terminal with §19 classifications; restart expires MCP parks and no pre-restart approval id works after; `cancel_run.rs:214-219` barrier semantics unchanged (regression, unmodified intent).
- **Verification command:** `cargo verify` (repo gate), executed under this session's established Phase 6 provenance judgment.

## Out of Scope

- Async cancel API (would break the pinned sync barrier; belongs to a future adapter-driven design if ever).
- ACP frames, cancelled-completion notifications, `session/request_permission` bridging, fresh-id re-ask over ACP (all need the adapter, which does not exist).
- MCP HTTP/SSE, server-set removal, persistent grants, hosted paths, evaluation.
- Effect-level idempotency changes; provider paths.
- Changes to the frozen M14 record, §46 deferrals, remote-gateway defaults, or architecture requirements.

## Further Notes

- ADR-0005 blocker order: blockers 1–3 closed by slice (a) (`b7ae280`), the reconciliation slice, and the MCP slice; this spec closes blocker 4 (cancellation drain + crash recovery). Remaining after it: the stdio lifecycle / approvals-bridge / evaluation tail of the follow-on list.
- Glossary: **Cancellation drain** added to `CONTEXT.md` during grilling; use it exactly (a `Cancelled` status alone never proves drain).
- No capability checklist doc: this slice adds no capability — it hardens the shipped cancel/MCP surfaces (no new commands, no new effects). Review will hold this line.
- No new ADR: ADR-0005 already decides the requirement; this is additive, reversible implementation.
