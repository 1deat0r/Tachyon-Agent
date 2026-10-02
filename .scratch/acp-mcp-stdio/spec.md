# Spec — Safe stdio MCP integration path for session-supplied servers (issue #57, ADR-0005 release blocker 3)

**Tracker status:** ready-for-agent
**Parent:** [#57 ACP v1 distribution through the local gateway](https://github.com/1deat0r/Tachyon-Agent/issues/57) — ADR-0005 third release blocker, "MCP subprocess launch/tool mediation, environment and secret handling"
**Authority:** ADR-0005 (Gateway and ownership boundary), AGENTS.md invariants, CONTEXT.md glossary
**Provenance:** `derived:open-issues` (auto-workflow fresh-goal reopen; blockers 1–2 landed: slice (a) `b7ae280`, reconciliation slice done-uncommitted in tree)

## Problem Statement

A client-supplied MCP server is an untrusted subprocess: its command, arguments, environment entries, and identity arrive from outside Tachyon's trust boundary, yet running it executes code with the session's authority. Today there is no MCP path at all — `mcp` matches zero files under `crates/`, there is no gateway command, store state, policy capability, or scheduler path for session-supplied servers. The only production child-spawn is the one-shot tool runner (`tachyon-tools/src/process.rs`), and the production IR compilers reject `process.spawn` outright (`ForbiddenCapability` at `tachyon-core/src/runtime.rs:348` and `:645`). Without a gateway-owned path, the future ACP adapter would have to launch client-supplied servers itself — a parallel, less restrictive execution path that ADR-0005 explicitly forbids. The gateway must therefore own the full lifecycle: validate untrusted descriptors, pin the authorized set per session, launch only as a Supervisor-owned approved operation, mediate every tool call through the existing authorize gate, handle secrets as broker handles, and fail closed on load — each piece independently verified before the ACP layer may use it.

## Solution

From the perspective of a gateway client (today's CLI/TUI, tomorrow's ACP adapter):

- `RegisterMCPServers { session_id, servers: [ServerDescriptor] }` validates each descriptor as untrusted input and durably pins the authorized set for the session. Under the default Ask policy the subsequent launch parks: the response reports `awaiting_approval` with a session-scoped `approval_id`. `ApproveMCPServers { session_id, approval_id }` launches every pinned server (initialize handshake, then live); `DenyMCPServers` refuses (rows stay for audit, nothing ever launches). One approval covers one register call; reconnects after restart need a fresh approval (grants never survive a restart — the pin persists, the grant does not).
- `ServerDescriptor = { server_id (opaque 1..=64), command (absolute path), args (≤32 entries, each ≤4KiB, NUL-free), env ([{name, value, secret}]) }`. Direct spawn only — never a shell. Dangerous variables (`LD_PRELOAD`, `LD_LIBRARY_PATH`, `DYLD_*`) are rejected. The child working directory is always the pinned session root; no other scope this slice.
- `ListMCPServers { session_id }` reports each server's descriptor (secret values shown as broker handles only), liveness, and tool inventory after handshake.
- `CallMCPTool { session_id, server_id, tool, arguments_json }` routes one tool call through `authorize()` as capability `mcp.tool` with scope `<server-id>/<tool>` and the full arguments in the operation JSON. Unknown tools fail as `UnknownCapability`. Calls park by default under Ask policy and are granted through the same session-scoped approve commands with per-call approval ids.
- The transport is a supervised long-lived stdio child actor: newline-delimited JSON-RPC over stdin/stdout, stderr to logs, owned process group, `kill_on_drop`, environment cleared then built from the allowlist plus explicitly supplied entries. Handshake (`initialize` + negotiated version recorded, mismatch fails closed with the child reaped) precedes `tools/list`, which precedes any call.
- Secrets: `secret: true` env values are registered in the `CredentialBroker` at registration; only handles persist in the store, journal, receipts, and `ListMCPServers` output. Redaction is proven the same way as the provider-key proofs.
- Load reconnects only the pinned set or fails closed. ACP disconnect never stops session servers. A gateway restart never auto-relaunches servers; an explicit load re-establishes them under a fresh approval. An interrupted call classifies `UnknownAfterCrash` unless the tool declares otherwise (§19 matrix).

## User Stories

1. As an ACP adapter developer, I want to register client-supplied MCP servers against a session, so that server setup travels with the session rather than living in adapter-local memory.
2. As an ACP adapter developer, I want registration of a malicious descriptor (relative command, shell metacharacters-as-shell, `LD_PRELOAD`, oversized env) to fail with a typed error and pin nothing, so that untrusted setup can never become an authorized server.
3. As an ACP adapter developer, I want launch to park until approved and `ListMCPServers` to show the wait, so that no client-supplied code runs on mere registration.
4. As an ACP adapter developer, I want `tools/list` after handshake to show the server's real inventory, so that capability advertisement always reflects the running child.
5. As an ACP adapter developer, I want one mediated `tools/call` round trip through the authorize gate, so that MCP tools are subject to the same policy as native tools.
6. As a security reviewer, I want every MCP tool proposal authorized per server/tool scope with unknown tools refused, so that a compromised server cannot reach beyond its grant.
7. As a security reviewer, I want raw secret env values to appear nowhere in the store, journal, receipts, or list output — handles only — so that a database or log leak exposes no credentials.
8. As an operator, I want session load to reconnect only the pinned set or fail closed, so that a restarted gateway can never silently widen a session's server set.
9. As an operator, I want a gateway restart to leave servers stopped until an explicit approved load, so that recovery never resumes unknown effects.
10. As the Task Supervisor, I want server launch and tool calls to arrive as validated IR operations through the existing park/grant plumbing, so that MCP adds no parallel execution path.
11. As a developer, I want the schema change additive (new tables only), so that existing state databases upgrade cleanly.
12. As a reviewer of issue #57, I want this blocker closed with register/launch/list/call/approval/load-fail-closed tests at the gateway seam plus pinning and redaction tests below, so that the ADR contract has executable evidence.

## Implementation Decisions

- **Layers touched:** `tachyon-protocol` (new `RegisterMCPServers` / `ListMCPServers` / `ApproveMCPServers` / `DenyMCPServers` / `CallMCPTool` commands + results + typed errors; no existing variant changes), `tachyon-gateway` (validation, stdio transport actor, handshake/list/call, session-scoped approve routing), `tachyon-store` (new `mcp_servers` table + session-scoped approval records reusing the existing approvals state-machine shape — exact columns at implementer discretion, additive migrations only), `tachyon-tools` (broker-backed env construction, redacted receipts), `tachyon-policy` (new `mcp.spawn` + `mcp.tool` capability descriptors), `tachyon-core` (scoped carve-out in the two `ForbiddenCapability` compilers for MCP-mediated nodes with contract-version pin per the ADR-0006 §10 pattern; park/decide plumbing for session-scoped approvals). No new crate, no ACP wire code.
- **Schema (additive):** `mcp_servers` with `UNIQUE(session_id, server_id)` holding command, args JSON, env-with-handles JSON, status (`registered` / `awaiting_approval` / `live` / `refused` / `stopped`), negotiated version, recorded tool inventory; plus session-scoped approval records (one-shot, BLAKE3 op hash, journal row) for launches and calls.
- **Validation (untrusted input):** `server_id` opaque 1..=64 bytes; `command` absolute path (executability checked at launch, shape at registration); direct `spawn` — no shell, so metacharacters are inert data; `args` ≤32 entries, each ≤4KiB, NUL-free; env names `[A-Za-z_][A-Za-z0-9_]*`, values ≤16KiB; `LD_PRELOAD` / `LD_LIBRARY_PATH` / `DYLD_*` rejected; cwd forced to the session's pinned canonical root (no second resolution).
- **Transport:** per-server supervised actor owning one child process group; stdin/stdout newline-delimited JSONRPC 2.0; stderr drained to logs; `kill_on_drop`; env cleared then rebuilt (inherited allowlist per `INHERITED_ENV_KEYS` plus the server's explicit entries, secrets injected from broker handles at spawn); `initialize` handshake records the negotiated version; version mismatch or handshake failure reaps the child and marks the server `stopped` with a typed error; `tools/list` inventory recorded before any call is admitted.
- **Approval:** one approval per register call (covers the whole set) plus one per parked tool call; `allow_once` / `reject_once` only; cancellation-vs-approval serialization per ADR-0005; late responses ignored. Fresh approval required for every (re)launch — reconnects included.
- **Mediation:** tool calls compile to validated IR nodes carrying capability `mcp.tool`, scope `<server-id>/<tool>`, and full arguments; unknown tool names fail as `UnknownCapability` before any child I/O; the call result returns as a redacted receipt.
- **Ownership invariants unchanged:** Supervisor remains sole logical writer; the gateway hosts the transport but dispatches nothing without a validated, authorized, granted operation; remote gateway mode untouched.
- **Retention:** no server-set expiry/GC this slice; growth noted as fog.

## Testing Decisions

- **What makes a good test:** external behavior only — commands in, observable responses/state out; use a fake MCP child (a script speaking the stdio protocol over stdin/stdout) rather than any real server binary; no assertions on internal actor structure.
- **Primary seam (existing shape, new file):** gateway round-trip tests — prior art: `session_root.rs` (typed errors, restart equality), `run_path.rs` (typed refusals), `recovery.rs` (shutdown/restart), approval routing in `server.rs:1612+`.
- **Secondary seams (existing):** store tests for pinning uniqueness, handle-not-raw persistence, and status transitions; tools tests for env allowlist + redaction (prior art: `secret_env_allowlist.rs`, `provider_redaction.rs`).
- **Required cases:** malicious descriptor rejected with typed error and nothing pinned; valid register parks (`awaiting_approval`, nothing spawned); deny leaves rows refused and spawns nothing; approve launches + handshake + `tools/list` inventory visible; unknown tool call is `UnknownCapability` with no child I/O; secret env value appears as handle in store/list/logs; restart then load without approval leaves servers stopped; approved reload reconnects only the pinned set; unknown server set on load fails closed; interrupted call classifies per §19 (no silent success).
- **Verification command:** `cargo verify` (repo gate), executed under this session's established Phase 6 provenance judgment.

## Out of Scope

- ACP wire adapter, `session/load`'s ACP-level result shape, and the ambiguous/in-progress load result (future ACP layer).
- MCP HTTP/SSE transports, embedded-resource content, image/audio prompts, additional workspace roots.
- Persistent `allow_always` / `reject_always` grants for servers or tools (needs the separate durable-grants design).
- Server-set removal commands and expiry/GC; sessions remain the lifecycle scope.
- Effect-level idempotency changes beyond the scoped compiler carve-out; provider/hosted paths; cancellation bridging beyond launch/call approval serialization; live evaluation.
- Changes to the frozen M14 record, §46 deferrals, remote-gateway defaults, or architecture requirements.

## Further Notes

- ADR-0005 blocker order: blockers 1 (durable session lookup and ordered replay) and 2 (safe creation/start reconciliation) closed by slice (a) (`b7ae280`) and the reconciliation slice; this spec closes blocker 3 (MCP launch/mediation + env/secrets). Remaining: cancellation drain + crash recovery, then the stdio lifecycle / approvals bridge / evaluation tail of the follow-on list.
- Glossary: **MCP server** + **MCP tool** added to `CONTEXT.md` during grilling; use them exactly.
- AGENTS.md new-capability checklist for this capability ships as `docs/agents/mcp-stdio-capability.md` (acceptance item of the final ticket — review findings in prior slices made the checklist a hard requirement).
- No new ADR: ADR-0005 already decides the requirement; this is additive, reversible implementation.
