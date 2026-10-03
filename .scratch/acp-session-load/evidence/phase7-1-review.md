# Phase 7 review — ACP session/load + loadSession:true (issue #57)

**Rule 17 arc:** Pass 1 (two axes) → Pass 2 (disposition: HARD) → fix (Phase 5) → full gate (Phase 6) → Pass 3 (affected surface only).
**Scope:** goal manifest from commit paths `e412899` (spec/tickets) + `8571481` (load arm) + `9912b5b` (wire tests + fixture seam) + `e73b640` (record+gate) + `3e94c0e` (cancel release) + `2abc099` (advertise flip + doc): `crates/tachyon-acp/{src/turn.rs, src/server.rs, src/codec.rs, src/lib.rs, tests/common/scripted.rs, tests/session_load_replay.rs, tests/session_load_gate.rs, tests/initialize_live_gateway.rs}`, `docs/agents/acp-adapter-capability.md`, `.scratch/acp-session-load/*`.
**Degradations:** no subagent tool in harness → orchestrator self-review (same fallback as prior goals); TypeSafe JEV probe earlier this session returned zero configured classifiers → deterministic → MiMo routing throughout.

## Pass 1 — Standards axis

- AGENTS invariants held: tachyon-acp only (zero gateway/core/protocol/store changes), no grant authority (load is read-only, no turn slot/key), advertise-only-implemented respected (flip landed last), capability-doc checklist sections all updated (Access set/Effect/Idempotency/Cancellation/Retry/Verification/Crash). fmt + clippy `-D warnings` green at each commit.
- Judgement calls logged, not defects: `session_load` as its own server method (mirrors `session_new`); strict `turns` requirement on prompt (real gateway always sends it; fixture default updated to match).

## Pass 1 — Spec axis (`.scratch/acp-session-load/spec.md` + ADR-0005:29/39/40/41)

All spec'd behaviors present with matching tests: replay-before-response FIFO, empty history, typed refusals (`unknown_session`, `workspace_mismatch`, param table), `loadSession:true` flip last, 13 test names in Verification method.

**H1 (HARD finding):** the recorded-turn gate was **load-scoped** — `SessionState.recorded` was only written by `session/load` in this process, so the prompt gate early-returned (no record) for a fresh-key prompt when load never ran. Deterministic proof:
1. The gateway has **no per-session overlap guard**: `create_supervised`/`start_run` scouted — only `UNIQUE(session_id, key)` (idempotency) and `tasks_by_session_turn` (turn_seq uniqueness); `StartRun` guards per-task only. The adapter is the ONLY overlap guard.
2. Reachable paths without load: (a) post-restart re-prompt with an old sessionId (the ACP-typical flow — `session_id_resolves_after_adapter_restart` proves clients re-prompt with old ids); (b) post-`turn_timed_out` re-prompt in the same process (record never existed — no load); (c) same. Both created a second live turn for one session → ADR-0005:39 "do not accept overlapping turns" violated.
3. The record also cost one extra `GetTask` round trip per prompt after a load, and needed clearing/re-deriving logic (three methods + scoped-clear) to stay truthful.

**Known issues (not defects):** (K1) prompt refuses `-32003` on a corrupt last row (`gateway_payload_invalid` — fail-closed by design, documented); (K2) `prompt_turn` now requires `turns` in `GetSession` — true of the real gateway always; any foreign gateway omitting it would fail typed (documented in the spec amendment).

## Pass 2 — disposition

H1 confirmed HARD (correctness/recoverability outrank; ADR invariant). Fix chosen: move the gate into `prompt_turn`'s **existing** `GetSession` (stateless; zero extra gateway calls vs. the old extra `GetTask`), delete the record machinery entirely, derive cancel's fallback target from its own `GetSession`.

## Fix (Phase 5) + gate (Phase 6)

- `prompt_turn`: after `workspace_root`, before `CreateTask` — `recorded_turn(turns)?` non-terminal ⇒ typed `-32003 turn_in_progress` (new message "A previous turn for this session is still in progress…", same code/data marker). Corrupt row ⇒ typed `gateway_payload_invalid`. Terminal/absent ⇒ proceed.
- Deleted: `SessionState.recorded` + 3 methods, `GateVerdict`, `recorded_gate_verdict`, `LoadOutcome`, `recorded_turn_gate` (server), `set_recorded` wiring. `run_load` returns `Vec<Outbound>` again.
- `cancel_pipeline`: no-local-turn path extracted as `cancel_outlived_turn` — derives the target from its own `GetSession` via `recorded_turn`; `ok`/`illegal_transition`/`unknown_task` ⇒ idempotent ok ("not running" = the cancel's goal); terminal/absent ⇒ original byte-pinned no-op; other failures propagate typed.
- Fixture: default `GetSession` now mirrors the real gateway (`turns: []`); new `start_with_get_session_sequence` seam (per-call answers — models gateway truth changing across calls).
- Tests rewritten to stateless semantics + NEW regression pin `prompt_without_any_load_is_refused_while_the_recorded_turn_runs` (THE Phase 7 finding); `gate_gettask_failure_fails_closed` → `prompt_gate_fails_closed_when_reconciliation_fails` (sequence: load ok, prompt's reconciliation fails ⇒ typed, zero creates).
- **`cargo verify` exit 0, workspace 858 passed / 0 failed** (858 → 858: −1 deleted unit, +1 new regression test), fmt + check + clippy `-D warnings` + xtask green.
- **MUTATION RED ×2:** (1) gate never refuses ⇒ `prompt_without_any_load…` + `prompt_after_load_is_refused…` FAILED; (2) cancel never derives target ⇒ `prompt_gate_releases_after_cancel` FAILED; both reverted byte-identically (`grep -c MUTATION` = 0) ⇒ green.
- Commits: `808905e` fix (code+tests), `498656b` docs (capability doc 4 passages + verify names + spec amendment + ticket note).

## Pass 3 — affected surface (final)

Re-reviewed at committed SHAs: gate at `turn.rs:1700` precedes `CreateTask` (`turn.rs:1711`); single prompt path into `prompt_turn` (no bypass); cancel derives at `turn.rs:1916` inside the extracted fn; no stale "stored record" claims in the capability doc; same-call retry contract intact (first turn terminal before retry in `same_call_retry_replays…` — full suite 858 green proves it); sequential guard unaffected (it runs before the gate, unchanged). **Verdict: CLEAN.** Rule-17 budget: 3/3 passes used (pass 3 because pass 2 reported hard).
