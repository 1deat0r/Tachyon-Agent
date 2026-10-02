# 02: Refusals never block behind a hung call + prompt call abort

**What to build:** One hung MCP `tools/call` can no longer hold refusal paths hostage, and driver cancel aborts the in-flight call promptly. The live-server map is isolated per server so deny, cancel, and other sessions' calls proceed while one call runs out its bound; the in-flight call future becomes cancel-aware so driver cancel aborts it well under the 60 s bound with the existing `stopped` + `mcp_call_failed` classification (never success). Resolves the MCP slice's deferred in-code follow-up note instead of moving it.

**Blocked by:** 01 (shares the park/approval plumbing; sequential tree)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-01)

- [x] Deny for session A proceeds while session B's MCP call hangs (two-session test; deny does not wait out the call bound)
- [x] Cancel for a task with a hung MCP call proceeds; the call aborts promptly (test-asserted bound far under 60 s) with `stopped` + `mcp_call_failed`
- [x] Late completion of the aborted call changes no state and reports nothing as success
- [x] Global lock across the call round trip is gone (shard or remove/reinsert); contention-sensitive paths covered by the two-session test
- [x] Tests at the gateway seam with controllable-hang fake children (gated pipe / sleep-then-answer)
