# 02: Approval-gated stdio launch, handshake, and tool inventory

**What to build:** Registered servers launch only through approval. `RegisterMCPServers` under the default Ask policy parks the launch and reports `awaiting_approval` with a session-scoped approval id; `ApproveMCPServers` spawns each pinned server as a supervised stdio child (owned process group, cleared env rebuilt from allowlist + explicit entries with broker-injected secrets, cwd forced to the pinned session root), performs the `initialize` handshake, and records the `tools/list` inventory; `DenyMCPServers` refuses and nothing ever spawns. Session load reconnects only the pinned set under a fresh approval or fails closed; a gateway restart leaves servers stopped until an explicit approved load.

**Blocked by:** 01 (needs the descriptor validation + pinning surface)

**Status:** pending

- [ ] Register-then-approve launches each server; `ListMCPServers` shows `live` plus the real post-handshake tool inventory from a fake MCP child (test script speaking stdio JSON-RPC, never a real server binary)
- [ ] Register without approval leaves servers `awaiting_approval` with no child process spawned (assert no spawn)
- [ ] Deny marks the set `refused`; nothing spawns; later calls for those servers fail closed
- [ ] Handshake/version mismatch reaps the child and marks the server `stopped` with a typed error
- [ ] Restart then load without approval leaves servers stopped; approved reload reconnects only the pinned set; an unpinned/unknown set on load fails closed
- [ ] ACP-disconnect-equivalent (client gone, gateway alive) leaves live servers running; restart never auto-relaunches
- [ ] Tests at the gateway seam (park/deny/approve-launch/handshake-failure/restart-load-fail-closed) with the fake child; store tests for status transitions
