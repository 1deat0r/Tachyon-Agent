# 01: Durable Session root through the gateway

**What to build:** A gateway client can create a session with an absolute workspace root that is workspace-validated (absolute enforced; `canonical_workspace_root`) and persisted durably; it can fetch the session back and see the bound root — including after a gateway restart. Clients that create sessions without a root keep the current behavior, and fetching an unknown session returns a clear typed error.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Session creation accepts an optional **absolute** workspace root; relative input is rejected (`workspace_not_absolute`). When supplied it is validated via `canonical_workspace_root` (exists / canonicalize / is-dir — the codebase's existing workspace validation; no grant registry exists, so no grant check at session creation) and persisted
- [ ] Session creation without a root still succeeds (legacy behavior) and reports an absent root on fetch
- [ ] New `GetSession` gateway command returns session identity and root; unknown session id → typed error
- [ ] Root survives a gateway/store restart
- [ ] `GetSession` is read-only: no state changes on the fetch path
- [ ] Tests at both seams: gateway round-trip (create→fetch→restart→fetch, rejection cases, unknown id) and store-level persistence
