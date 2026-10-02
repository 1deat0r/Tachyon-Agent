# 03: Cancel interaction, timeout suspension, orphan fallback

**What to build:** The three contracts that keep the bridge honest under stress. (1) `session/cancel` while a permission request is outstanding: resolve the request locally as `cancelled` with ZERO decision frames sent (the gateway expires the parked row before its terminal journal; a late Approve would typed-fail anyway), then the existing drain-ack ordering re-pins: cancel reply → prompt `stopReason: cancelled`. The inline cancel must not deadlock against a pending client response. (2) `TURN_TIMEOUT` suspended while a request is outstanding — a human-paced prompt must never be killed by the 300 s turn deadline; document; cancel/disconnect remain the unblock paths. (3) Orphan fallback: a park observed with no `approval_request` frame after a bound ⇒ existing typed `approval_required` (never a silent hang, never a false request).

**Blocked by:** 02 (cancel and timeout interact with the settled exchange semantics)

**Status:** pending (blocked by 02)

- [ ] Cancel-during-outstanding-request (scripted or live): frame order asserted — request emitted → cancel → local `cancelled` resolution with no Approve/Deny frames → CancelTask drain ack → prompt `cancelled`; byte-pin the "zero decision frames" claim
- [ ] Late-gateway race: after cancel, a client's delayed `allow_once` answer arrives ⇒ locally ignored (request already resolved), no Approve issued, no panic
- [ ] Timeout suspension proof: a request held open past the normal turn-timeout window (fake clock or a bound-crossing assertion) does NOT produce `turn_timed_out`; once resolved, the normal deadline applies again
- [ ] Orphan fallback: WaitingApproval with no request frame within the bound ⇒ typed `approval_required` (the ticket-01 rewritten test keeps a variant for the request-less park path)
- [ ] Capability doc: request flow, fail-closed table, refusal-on-deny, timeout suspension, orphan fallback all documented with real test names; `cargo verify` green; all prior acp suites green
