# Report — ACP session/request_permission permission bridge (issue #57)

status: running
outcome: success
goal: Advance issue #57 — land the session/request_permission permission bridge (one-shot approvals driven through the adapter) as a small independently verified tracer-bullet slice honoring ADR-0005
provenance: derived:open-issues (goal rotated 2026-10-01T09:06:35Z; residue item from the adapter-lifecycle slice)
spec: .scratch/acp-permission-bridge/spec.md (tracker: ready-for-agent → all tickets done)

## Changed-files manifest (goal commits vs. baseline 8c23b6e)

Code (`tachyon-acp` only; zero gateway/core/protocol/store changes):
- `crates/tachyon-acp/Cargo.toml` (+4, dev-only tokio `test-util`)
- `crates/tachyon-acp/src/codec.rs` (+71/−1, `Outbound::Request` + negative-id golden)
- `crates/tachyon-acp/src/server.rs` (+29/−6, response routing + EOF slot clear + state threading)
- `crates/tachyon-acp/src/turn.rs` (+2128/−71, bridge state machine, validation funnel, cancel local resolution, suspension, orphan/deny bounds, review fix)
- `crates/tachyon-acp/tests/common/mod.rs`, `tests/common/scripted.rs` (fixture: broadcast fan-out, `Deny` reaction)
- `crates/tachyon-acp/tests/session_permission_allow.rs` (+376), `session_permission_deny.rs` (+374), `session_permission_edges.rs` (+364), `session_prompt_stream_edges.rs` (+229/−13, rewritten park test)

Docs/trackers:
- `docs/agents/acp-adapter-capability.md` (two S5 passes + Phase 7 precision edit)
- `.scratch/acp-permission-bridge/{spec.md, issues/01..03, evidence/phase5-2, phase5-3, phase7-1}`
- `docs/agents/auto-workflow/{state.md, decisions.md}` (tracker + decision log)

## Tasks

| ticket | status | commits |
|---|---|---|
| 01 request-and-allow | done (1 commit) | `09cfe18` |
| 02 fail-closed-outcomes | done (5 small tasks) | `b62dfce..dd5ae93` |
| 03 cancel-timeout-orphan | done (5 small tasks) | `d22c422..5edd588` |

## Verification

- Final full gate: `cargo verify` exit 0, **workspace 843 passed / 0 failed** (841 → 843 after the Phase 7 fix), fmt + check + clippy `-D warnings` + xtask clippy green.
- Every small task landed green with per-task mutation-red evidence (decision rows in `decisions.md`, bodies in `evidence/phase5-*.md`).
- Phase 7 fix evidence: `.scratch/acp-permission-bridge/evidence/phase7-1-review.md`.

## Review (rule 17, budget 3/3 used)

- Pass 1 (two axes, orchestrator self-review — no subagent tool in harness): 1 HARD finding (unbounded gateway read while the turn budget is suspended), 2 known issues.
- Pass 2: hard confirmed → fix; judgement-call rejected → known issue.
- Fix: `read_gateway_frame` frozen-deadline bound (`d7c7db3`) + capability-doc precision (`2683f76`); mutation-red recorded.
- Pass 3 (affected surface): CLEAN.
- JEV: zero configured classifier models — one degradation logged; deterministic → MiMo routing kept (no dependency installed, no key requested).

## Known issues / residue (carried, not blocking)

1. Announced `tool_call` stays open when a `session/cancel` resolves its request locally — deliberate (keeps the cancelled segment byte-empty); also applies to typed-failure paths (K2).
2. `PendingDecision::Approve`/`Deny` step-1 arms are near-duplicates — style judgement; explicit arms match repo style.
3. From the capability doc's `Remaining before an 'ACP supported' claim` (unchanged by this goal): `session/load` + `loadSession:true`; MCP servers at session setup; ResourceLink prompt content; attach/reconnect; hosted-provider/live evaluation if still required.
4. Gateway-side non-terminal-status-after-deny gap remains out of scope (bounded by the 5 s deny grace → `refusal`).

## Out-of-repo side effects

None. All commits pushed to `1deat0r/Tachyon-Agent` main per the direct-main workflow; no GitHub issue/comment/label mutations.

## Decision log

`docs/agents/auto-workflow/decisions.md` as of this run (rotates to `decisions-<STATE.updated>.md` at rotation; pairs with this report's suffix).

## Resume instructions

No `report.md` while STATE says `running` ⇒ crashed; next invocation resumes from `docs/agents/auto-workflow/state.md`.
