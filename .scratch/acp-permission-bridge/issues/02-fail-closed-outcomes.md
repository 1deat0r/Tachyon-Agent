# 02: Fail-closed outcomes: deny → refusal, invalid → Deny

**What to build:** Every non-allow outcome of the exchange, with no hangs. `selected`+`reject_once` ⇒ gateway `Deny` (reason names the ACP client) → `tool_call_update` {status: failed} → bounded grace wait for a terminal status → prompt settles `stopReason: refusal` (NEVER `turn_timed_out`, NEVER `end_turn`). Unknown optionId, unknown/malformed outcome, or a response whose shape fails validation ⇒ FAIL CLOSED as `Deny` + log (ADR-0005:47 — the operation must never run). Standalone `outcome: cancelled` with no session/cancel in flight ⇒ no-approval ⇒ `Deny` + log. Allow-path behavior from ticket 01 must not regress.

**Blocked by:** 01 (shares the exchange state, tool_call ids, and request plumbing)

**Status:** pending (blocked by 01)

- [ ] Deny flow end-to-end (scripted): reject_once → `Deny` with correct task_id+approval_id+reason → `tool_call_update failed` → prompt answers `stopReason: refusal` within the grace bound; `turn_timed_out` NEVER appears
- [ ] Unit table: every invalid response shape (wrong optionId, `outcome` other than selected/cancelled, selected without optionId, non-object) ⇒ Deny issued, never Approve, logged
- [ ] Standalone `cancelled` outcome ⇒ Deny + log; if a session/cancel is ALSO in flight, cancel dominates (no decision frames — covered in ticket 03, but the branch must route there, not double-decide)
- [ ] Race pin: `approval {granted:false}` journal observed when no request is outstanding (e.g. another client decided it) ⇒ handled without panic or double-settle
- [ ] Allow path regression: ticket 01's allow-flow test still green
