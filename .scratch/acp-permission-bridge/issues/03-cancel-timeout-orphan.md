# TASK 03: Cancel interaction, timeout suspension, orphan fallback

**Status:** pending (blocked by 02)
**Blocked by:** 02
**What to build:** Three contracts that keep the bridge honest under stress. (1) `session/cancel` while a permission request is outstanding: resolve the request locally as `cancelled` with zero decision frames (the gateway expires the parked row before its terminal journal; a late Approve would typed-fail), then the drain-ack ordering re-pins: cancel reply → prompt `stopReason: cancelled`. Inline cancel must not deadlock against a pending client response. (2) `TURN_TIMEOUT` suspended while a request is outstanding; cancel and disconnect remain the unblock paths; document it. (3) Orphan fallback: a park observed with no `approval_request` frame after a bound ⇒ typed `approval_required` (never a silent hang, never a false request).
**Verify:** cancel-during-request ordering test; timeout-suspension proof; orphan fallback test; `cargo verify` exit 0 at task end.

## Small tasks (each = one commit, in order)

- [x] S1 Cancel-during-outstanding-request: local `cancelled` resolution, zero decision frames, drain-ack order re-pinned · Verify: `cancel_resolves_the_pending_request_locally_then_reports_cancelled`
  - [x] M1a On session/cancel with an armed request: resolve the local oneshot as `cancelled`, disarm the slot, send NO Approve/Deny
  - [x] M1b Preserve frame order: request emitted → cancel reply (CancelTask drain ack) → prompt `stopReason: cancelled`
    - [x] N1a1 Byte-pin: zero Approve/Deny frames appear between cancel and prompt reply
    - [x] N1a2 Regression: `cancel_mid_turn_awaits_the_drain_ack...` from task 03 of the adapter slice still green
  - [x] M1c No deadlock: inline cancel never awaits the client answer it just invalidated
- [ ] S2 Delayed client answer after cancel ⇒ ignored locally · Verify: `late_permission_answer_after_cancel_is_ignored`
  - [ ] M2a Response arriving for a disarmed id drops with a log line; no panic, no gateway call
- [ ] S3 TURN_TIMEOUT suspended while a request is outstanding · Verify: `turn_timeout_is_suspended_during_an_outstanding_request`
  - [ ] M3a Pause the deadline when a request arms; resume with full remaining budget after resolution
    - [ ] N3a1 Bound-crossing test: held request outlives 300 s window without `turn_timed_out`
  - [ ] M3b Document the suspension in the capability doc
- [ ] S4 Orphan fallback: WaitingApproval with no ask frame within the bound ⇒ typed `approval_required` · Verify: `orphaned_park_falls_back_to_approval_required`
  - [ ] M4a Tighten the interim grace into the orphan bound; keep the request-less stub test green
  - [ ] M4b Emit no false request when no frame ever arrives
- [ ] S5 Capability doc final pass + full regression · Verify: `cargo verify` exit 0; all acp suites green
  - [ ] M5a Cancellation, retry, timeout, fallback sections match shipped behavior with real test names
  - [ ] M5b Full suite: tasks 01+02 tests unmodified and green
