# 01: Cancel invalidates parked MCP approvals

**What to build:** Cancelling a task (or its session scope) reaches the gateway-side MCP park maps, not just the supervisor's. Parked MCP launch approvals and parked MCP call approvals for the cancelled scope expire with their durable outcomes recorded, their waiters are released as cancelled, and any late approve/deny for the dead ids fails with a typed error and consumes nothing — mirroring `release_parked` semantics for supervisor parks.

**Blocked by:** None (can start immediately)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-01)

- [x] Cancel with a parked MCP launch approval expires the park (durable `mcp_approvals` outcome recorded), releases the waiter as cancelled, and the server never launches
- [x] Cancel with a parked MCP call approval expires the park, releases the waiter, and the call never executes
- [x] Late approve/deny for an expired-by-cancel id fails typed (`approval_missing`-family) and changes no state
- [x] Deny-then-cancel and cancel-then-deny orderings both resolve deterministically (cancel wins; no double-outcome rows)
- [x] Tests at the gateway seam (fake stdio children; park → cancel → late-decide orderings for launch and call) + store tests for the expiry transitions if new transitions are added
