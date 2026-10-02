# 03: Mediated tool calls, secret redaction, and crash semantics

**What to build:** One mediated tool-call path through the existing authorize gate plus the security proofs. `CallMCPTool { session_id, server_id, tool, arguments_json }` compiles to a validated IR node with capability `mcp.tool` and scope `<server-id>/<tool>`; unknown tools fail as `UnknownCapability` with no child I/O; calls park by default and grant via the session-scoped approve commands. Secret env values stay handles end to end (store, journal, receipts, logs). An interrupted call classifies per the §19 matrix (no silent success). Ships the AGENTS.md capability checklist doc for the new capability.

**Blocked by:** 02 (needs a live, handshaked server to call through)

**Status:** pending

- [ ] Scoped carve-out in both `ForbiddenCapability` compilers (`runtime.rs:348`, `:645`) for MCP-mediated nodes only, with contract-version pin per the ADR-0006 §10 pattern; everything else still refused
- [ ] Known-tool call round-trips through `authorize()` and returns a redacted receipt; unknown tool → `UnknownCapability`, provably no child I/O
- [ ] Calls park by default; grant → executes once; late/duplicate grant after cancel is ignored (approval/cancellation serialization)
- [ ] Secret env value appears as handle in store rows, journal, receipts, `ListMCPServers` output, and logs (redaction tests mirroring the provider-key proofs)
- [ ] Kill-mid-call classifies per §19 (interrupted call never reports success; recovery path documented in the test header)
- [ ] Capability checklist doc `docs/agents/mcp-stdio-capability.md` with all 11 AGENTS.md fields, factually matching the shipped code

**Correction (review fix round):** the ticket-03 decision-log row claiming
`no new persistence surface` is OVERRIDDEN by the spec Implementation
Decisions (spec text outranks the row): the spec's Layers bullet promises
`session-scoped approval records reusing the existing approvals
state-machine shape` and its Schema bullet promises `session-scoped
approval records (one-shot, BLAKE3 op hash, journal row) for launches
and calls`. Shipped as additive `mcp_approvals` table (migration
`0008_mcp_approvals.sql`: approval id, session, kind `launch`|`call`,
BLAKE3 op hash, outcome `parked` then one-shot `granted` / `denied` /
`consumed-missing`, park/decide timestamps) written at park and decide
time. The live park stays in-memory (grants never survive a restart —
crash semantics unchanged, fail closed); the rows are the durable audit
trail, not the grant.
