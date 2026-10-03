# Phase 7 review — ACP permission bridge (issue #57)

**Rule 17 arc:** Pass 1 (two axes) → Pass 2 (disposition) → fix (Phase 5) → full gate (Phase 6) → Pass 3 (affected surface only).
**Scope:** current-goal commit manifest derived from git: `09cfe18` + `b62dfce..dd5ae93` (TASK 02) + `d22c422..5edd588` (TASK 03) → files: `crates/tachyon-acp/{Cargo.toml, src/codec.rs, src/server.rs, src/turn.rs, tests/common/{mod,scripted}.rs, tests/session_permission_{allow,deny,edges}.rs, tests/session_prompt_stream_edges.rs}` + `docs/agents/acp-adapter-capability.md`. Tracker/chore commits excluded.
**Degradations:** no subagent tool in this harness → orchestrator self-review (same fallback as the cancel-drain goal); TypeSafe JEV probe returned zero configured classifier models (`models.getAvailableOfType('classifier')` → `[]`) → one recorded degradation, deterministic → MiMo routing.

## Pass 1 — Standards axis

- Production diff read end-to-end (`turn.rs` 23 hunks lines 1–2128, `codec.rs` +71, `server.rs` +29). AGENTS.md invariants: adapter holds no grant authority (one `Approve`/`Deny` per validated answer through the Supervisor one-shot registry) ✓; no provider/core/protocol/store changes ✓ (bridge commits touch `tachyon-acp` only); Cargo.toml change is dev-only `tokio test-util` ✓; `allow_always`/`reject_always` appear only as fail-closed test inputs ✓; `loadSession:false` golden intact ✓.
- Finding S1 (judgement call): `PendingDecision::Approve`/`Deny` arms in `stream_turn` step 1 are ~40-line near-duplicates. Repo style keeps explicit match arms; extraction churn in the hot path outweighs benefit → **known issue, no fix**.

## Pass 1 — Spec axis (`.scratch/acp-adapter-capability.md` + `.scratch/acp-permission-bridge/spec.md`)

- All 12 required spec test names present and green (allow, deny/refusal, invalid fail-closed, standalone cancelled, cancel-local-resolution, late-answer drop, timeout suspension, orphan bound, replay ask, golden pins, option-validation table).
- Out-of-scope held: MCP parks untouched, `session/load`/ResourceLink/attach untouched, no advertisement change, no `allow_always`.
- Finding H1 (**hard**): **unguarded gateway read during budget suspension.** `stream_turn` step 4's `read_bound() == None` arm is a plain `conn.read_frame()`. The wrapper's paused branch has NO timer while suspended. Common ordering: settlement `GetTask` armed (`awaiting = Some`) → `approval_request` journal → `ask()` suspends the budget → step 4 waits for the `GetTask` response **with zero bound**. If the gateway stalls (alive, socket open): turn hangs forever (violates capability-doc "-32004 at the 300 s deadline rather than hanging the client forever"); `session/cancel` only resolves the answer receiver (not polled here); client EOF clears reply slots but the serve-loop drain waits on the turn → adapter never exits. Pre-bridge the 300 s wrapper bounded this read; the suspension silently removed it.
- Finding S2 (doc): capability doc claimed "no timer runs" during suspension — imprecise after any fix → fixed with H1.
- Known issue K1: an announced `tool_call` also stays open when the request ends typed (error frame / closed slot / frozen-deadline expiry) — same residue family as the documented cancel-path gap (deliberately no `tool_call_update` on those paths to keep verdict frames byte-pinned).

## Pass 2 — disposition

- H1: **confirmed hard** (deterministic control-flow proof above; recoverability ranks #1 in AGENTS priority order) → smallest safe fix.
- S1: rejected (style judgement) → report known issue. K1: residue, tracked in report.

## Fix (Phase 5) + gate (Phase 6)

- `read_gateway_frame(read, budget)` — plain read while the budget runs (wrapper owns the bound, byte-equivalent behavior); while suspended, `timeout_at(frozen pre-suspension deadline)` ⇒ typed `turn_timed_out` (shared `turn_timed_out()` helper, one message source for wrapper + read). Wired into step 4's `None` arm via `bridge.budget.as_ref()`.
- Tests (+2): `a_gateway_read_while_suspended_keeps_the_frozen_deadline` (paused clock, frozen deadline 1 s, read sleeps 300 s ⇒ typed `turn_timed_out`), `a_gateway_read_while_running_adds_no_local_timer` (negative control: 1 s deadline, read sleeps 5 s ⇒ completes `Ok`; budget-less ⇒ plain).
- **MUTATION RED EVIDENCE:** `frozen` binding forced to `None` ⇒ suspended test FAILED (`a wedged gateway read fails typed at the frozen deadline: "…too late"`, negative control green); reverted byte-identically (`grep -c MUTATION` = 0) ⇒ green.
- **`cargo verify`: exit 0, workspace 843 passed / 0 failed** (841 → 843), fmt + check + clippy `-D warnings` + xtask clippy green. `tachyon-acp` suite: 55 lib + all 14 integration targets green.
- Commits: `d7c7db3` fix, `2683f76` capability-doc precision (2 lines).

## Pass 3 — affected surface (final)

Re-reviewed `read_gateway_frame`, the step-4 arm, both tests, and the 2-line doc edit at their committed SHAs:
- Running path unchanged (plain read, wrapper bound preserved — `a_unsuspended_turn_still_times_out` still green).
- `timeout_at` polls the read first ⇒ a buffered frame wins over an expired deadline (same poll-order property the orphan arm relies on).
- Past frozen deadline ⇒ immediate typed `turn_timed_out` is the honest verdict (the budget was consumed before suspension).
- Precedence correct: phase bounds (orphan 2 s / deny 5 s) are `Some` arms and can only coexist with a running budget; the frozen bound applies only to the `None` arm during suspension, which by construction means `phase == Outstanding && awaiting.is_some()` — a machine-paced gateway round trip, never the human wait (step 2 catches `awaiting.is_none()` before step 4).
- **Verdict: CLEAN.** Rule-17 budget used: pass 1 + pass 2 + pass 3 (3/3; pass 3 ran because pass 2 reported a hard finding).
