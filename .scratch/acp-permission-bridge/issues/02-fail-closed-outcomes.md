# TASK 02: Fail-closed outcomes: deny → refusal, invalid → Deny

**Status:** ready
**Blocked by:** None (01 complete)
**What to build:** Every non-allow outcome of the permission exchange, with no hangs. `selected`+`reject_once` ⇒ gateway `Deny` (reason names the ACP client) → `tool_call_update` {status: failed} → bounded grace wait for a terminal status → prompt settles `stopReason: refusal` (never `turn_timed_out`, never `end_turn`). Unknown optionId, unknown or malformed outcome shape ⇒ fail closed as `Deny` + log (ADR-0005:47): the operation must never run. Standalone `outcome: cancelled` with no session/cancel in flight ⇒ no-approval ⇒ `Deny` + log. The gateway journals no terminal status after deny (pre-existing, out of scope): the grace wait must default to `refusal`.
**Verify:** scripted deny e2e test; invalid-shape unit table; allow-path regression; `cargo verify` exit 0 at task end.

## Small tasks (each = one commit, in order)

- [x] S1 Deny settlement path — reject_once drives `Deny`, `tool_call_update` failed, grace-wait, prompt settles `refusal` · Verify: `deny_settles_the_prompt_as_refusal` (scripted)
  - [x] M1a Emit `Command::Deny { task_id, approval_id, reason }` through the exchange's gateway connection, sequenced around the settlement slot like Approve
  - [x] M1b Emit `tool_call_update` {toolCallId, status: failed} after the Deny decision
  - [x] M1c Grace-wait settle: after `approval {granted:false}`, bounded wait for a terminal status; default to `stopReason: refusal`
    - [x] N1a1 Unit: grace default constant → refusal branch, no terminal status needed
    - [x] N1a2 Unit: `refusal` never maps to `turn_timed_out` or `end_turn`
- [x] S2 Fail-closed response validation — every invalid shape issues Deny, never Approve · Verify: `invalid_responses_fail_closed_as_deny` unit table
  - [x] M2a Response validator: optionId must equal `allow_once`/`reject_once`; outcome must be `selected` (with optionId) or `cancelled`; else invalid
    - [x] N2a1 Unit cases: unknown optionId, outcome not selected/cancelled, selected without optionId, non-object response — each ⇒ Deny + log
  - [x] M2b Wire the validator after every client answer, before any Approve path
- [ ] S3 Standalone `cancelled` outcome ⇒ Deny + log · Verify: `standalone_cancelled_outcome_denies` (scripted)
  - [ ] M3a Branch: `outcome: cancelled` with no cancel in flight ⇒ no-approval ⇒ Deny + log
  - [ ] M3b Route guard: when session/cancel IS in flight, do not decide (ticket 03 owns that wiring); no double-decide
- [ ] S4 Race pin: `approval {granted:false}` with no outstanding request ⇒ no panic, no double-settle · Verify: `late_deny_journal_with_no_request_is_handled` (scripted)
- [ ] S5 Capability doc wording + full regression (allow path + prior suites green) · Verify: `cargo verify` exit 0; docs test-name list updated
  - [ ] M5a Update docs/agents/acp-adapter-capability.md deny/refusal/fail-closed sections + test names
  - [ ] M5b Full suite: ticket 01 tests unmodified and green
