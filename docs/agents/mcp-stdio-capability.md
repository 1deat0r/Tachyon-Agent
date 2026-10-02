# Capability: mediated MCP tool calls (`mcp.tool` via `CallMCPTool`)

Capability documentation required by `AGENTS.md` § New capability checklist. Covers the gateway-level surface added for issue #57 ticket 03 (mediated tool calls, secret redaction, crash semantics — ADR-0005's third release blocker). Glossary terms: **MCP server**, **MCP tool** (CONTEXT.md) — a call is one `tools/call` round trip against a pinned, live server's child, authorized per server/tool scope like any other effect.

## Why deterministic code cannot already solve it

No deterministic code can decide whether an ambiguous client round trip should run a third-party tool: the tool lives in an untrusted child outside Tachyon's trust boundary, its arguments arrive from outside, and its effects are unobservable from the gateway. The gateway instead mediates every call — inventory-check, validated-IR compile, policy authorize with a per-call one-shot grant, supervised child I/O, broker-redacted receipt — so the decision to run is always an explicit, scoped, auditable grant rather than a guess. The capability itself runs no model/Jev call.

## Input / output schema

- **`CallMCPTool { session_id, server_id, tool, arguments_json }`** — `arguments_json` must be a JSON object, else typed `invalid_mcp_call`. Unknown session → `unknown_session`; unpinned server → `unknown_mcp_server`; pinned but non-`live` server → `mcp_not_live`; tool outside the recorded `tools/list` inventory → `unknown_capability` before any child I/O. Under the default Ask policy the call parks: `Ok { status: "awaiting_approval", approval_id, server_id, tool }` — one approval per parked call.
- **`ApproveMCPTool { session_id, approval_id }`** → consumes the park one-shot and executes exactly once: `Ok { status: "ok", server_id, tool, result }` with the broker-redacted child `result`. Unknown/consumed ids → `approval_missing` (a late or duplicate grant after deny/cancel is ignored, never executed); foreign-session ids → `approval_session_mismatch` (not consumed). A server that died while parked → `mcp_not_live`; a child that dies mid-call → typed `mcp_call_failed`, never success. Unknown session → `unknown_session`.
- **`DenyMCPTool { session_id, approval_id, reason }`** → `Ok { status: "denied", server_id, tool }`; nothing ever executes. Same `approval_missing` / `approval_session_mismatch` / `unknown_session` refusals.
- A child that answers `tools/call` with a JSON-RPC error fails as typed `mcp_tool_error` with the redacted, length-capped detail — and the server stays `live` (an answered round trip proves a healthy child; only transport death falls back to `stopped`).
- All three commands are additive inside protocol v2 (no version bump); wire round-trips pinned by the protocol test.

## Access set

Gateway → store + one live child only. Reads: `session_exists`, one `get_mcp_server` row plus its recorded `tools_json` inventory (re-proved under the grant), and a clone of the in-memory `CredentialBroker` vault. Writes: none to the store on success (the receipt is returned, not persisted); on mid-call child death, one `mark_mcp_servers(stopped)` plus dropping the live child (reaped via `kill_on_drop`). Child I/O: exactly one newline-delimited JSON-RPC `tools/call` frame on the already-open pipes of the `live` child — no spawn, no new process, no filesystem writes beyond `state.db` WAL, no network beyond local IPC. The `mcp_live` map lock is held across the single round trip so calls never interleave on shared pipes.

## Effect class

`DestructiveExternalMutation` (compiled node, `tachyon_core::runtime::compile_operation` `mcp.tool` arm): the tool runs inside an untrusted child with ambient authority and declares no recovery path. The node carries capability `mcp.tool`, the full arguments verbatim, and the ADR-0006 §10 contract pin (`MCP_TOOL_CONTRACT_VERSION = 1`); misshaped scope parts fail as `InvalidArgs` before any graph exists. `mcp.tool` smuggled into a model patch batch fails as `UnknownCapability` — mediated calls compile only through `compile_operation` on the gateway path, never through model proposals — and non-MCP `process.spawn` / `credential.use` / `net.fetch` / `shell.exec` stay `ForbiddenCapability` in both compilers (negative tests).

## Idempotency

`Unknown`: every mediated call is at-most-once per grant. The park entry leaves the map before any re-check or child I/O and the policy grant burns on the re-authorization, so one approval executes exactly one round trip; duplicates, late grants after deny, and cross-session grants all fail closed. There is no blind replay: after a mid-call death, retry needs a fresh approved launch (the row fell back to `stopped`) plus a fresh `CallMCPTool` + grant.

## Resource claim

None beyond what the approved launch already holds: the granted call borrows the live child's pipes for one round trip (≤60 s), claims no ownership, workspace lease, run-admission slot, or supervisor registry entry. Denied or refused calls release their park entry and hold nothing.

## Cancellation behavior

Deny is the cancellation: `DenyMCPTool` consumes the park and nothing ever executes; a later grant of the same id is ignored (`approval_missing`). A grant that finds its server no longer `live` fails as `mcp_not_live` instead of executing against a stale child. There is no mid-call cancel bridge this slice — a granted call runs to the 60 s bound, EOF, or a typed answer.

## Retry policy

Retry freely after a typed refusal (`unknown_capability`, `mcp_not_live`, `invalid_mcp_call`, `approval_missing`) — none executed anything. After `mcp_call_failed` or `mcp_tool_error`, retry only as a fresh human-approved call (fresh `CallMCPTool` + fresh grant, and a fresh approved launch first when the row went `stopped`): the interrupted outcome is unknown per §19, so a blind replay could double-apply the tool's external effect. Validation refusals are permanent for that shape.

## Verification method

Gateway seam (`crates/tachyon-gateway/tests/mcp_mediated_call.rs`): `mediated_call_parks_then_grants_once_with_redacted_receipt`, `unknown_tool_fails_before_child_io_with_typed_refusals`, `parked_server_is_not_live_until_its_launch_grants`, `deny_consumes_park_and_late_grant_is_ignored`, `kill_mid_call_never_reports_success_and_marks_stopped`, `secret_env_stays_handles_across_list_receipt_and_durable_files`. Compiler seam (`crates/tachyon-core/tests/mcp_tool_compile.rs`): node shape + contract pin, misshaped-scope refusals, both-compiler forbiddens, patch-path unknown. Protocol seam (round-trip + v2-stays test). Gate: `cargo verify`.

## Crash-recovery behavior

The interrupted call classifies `UnknownAfterCrash` per the §19 matrix: the grant fails as typed `mcp_call_failed`, the row falls back to `stopped` with the inventory cleared (a later `ListMCPServers` never shows stale liveness), and the dead child is reaped — recovery requires reconciliation (fresh approved launch + fresh approved call), never a resume. A crash between park and grant leaves an in-memory park only, so a restarted gateway answers the pre-restart id with `approval_missing` and nothing executes without a fresh call. A crash before the park leaves nothing. The kill-mid-call test documents the in-process fault-kill harness (poison `die` tool) in its header.

## Expected latency class

Interactive local IPC plus one supervised child round trip: park is two indexed store reads + one compile + one authorize (no child I/O); grant adds one `tools/call` frame bounded at 60 s (local children answer in milliseconds). No model calls; not on any running task's critical path.

# Capability: mediated MCP launches (`mcp.spawn` via `ApproveMCPServers`)

Capability documentation required by `AGENTS.md` § New capability checklist. Covers the gateway-level surface added for issue #57 ticket 02 plus the review-fix launch funnel (validated-IR compile + authorize per server, durable one-shot approval records — ADR-0005's third release blocker). Glossary term: **MCP server** (CONTEXT.md) — a launch is one supervised stdio child spawn plus `initialize` handshake plus `tools/list` inventory, granted exactly once per register call's approval id.

## Why deterministic code cannot already solve it

No deterministic code can decide whether an ambiguous client round trip should start a third-party subprocess: the server's command, arguments, and environment arrive from outside Tachyon's trust boundary, and running them executes code with the session's authority. The gateway instead mediates every launch — descriptor validation, durable pin, validated-IR compile, policy authorize with a per-server one-shot grant, supervised spawn with broker-injected secrets — so the decision to run is always an explicit, scoped, auditable grant rather than a guess. The capability itself runs no model call.

## Input / output schema

- **`RegisterMCPServers { session_id, servers: [ServerDescriptor] }`** — each descriptor validated as untrusted input (`server_id` opaque 1..=64 bytes, NUL-free, no `/` so the authorize scope splits; absolute-path command; bounded NUL-free args; well-formed env names and values; dangerous variables rejected). Any rejection fails as typed `invalid_mcp_descriptor` and pins nothing. A valid set pins durably with status `awaiting_approval` and parks: `Ok { servers: [{ server_id, status: "awaiting_approval" }], approval_id }` — one session-scoped approval id per register call, plus one durable `mcp_approvals` row (`kind: "launch"`, BLAKE3 set-document hash).
- **`ApproveMCPServers { session_id, approval_id }`** → consumes the park one-shot and launches each pinned server: `Ok { servers: [{ server_id, status: "live", version, tools }] }`. Unknown/consumed ids → `approval_missing`; foreign-session ids → `approval_session_mismatch` (not consumed). Each stored row is re-validated in full immediately before its spawn (see *environment and secret handling* below). Per-server typed launch failures: `mcp_spawn_failed` (bad command, lost secrets, dead child), `mcp_handshake_failed`, `mcp_version_mismatch`, `mcp_no_session_root`. A failed server is reaped and marked `stopped`; the rest still launch, and the first failure's code fails the command.
- **`DenyMCPServers { session_id, approval_id, reason }`** → `Ok { servers: [{ server_id, status: "refused" }] }`; rows stay for audit and nothing ever spawns. Same `approval_missing` / `approval_session_mismatch` / `unknown_session` refusals.
- All commands are additive inside protocol v2 (no version bump); wire round-trips pinned by the protocol test.

## Access set

Gateway → store + one child spawn per server. Reads: `session_exists`, the pinned `mcp_servers` rows, the session root, and a clone of the in-memory `CredentialBroker` vault. Writes: `pin_mcp_servers` (register), `mark_mcp_live` per launched server, `mark_mcp_servers(stopped|refused)` on failure/refusal, one `mcp_approvals` park row plus its one-shot decide (`granted` / `denied` / `consumed-missing`). Child effects: exactly one supervised spawn per server (owned process group, cleared env rebuilt from the allowlist plus pinned entries with broker-injected secrets, cwd forced to the pinned session root), stderr drained redacted to logs, `initialize` + `tools/list` on the child's pipes. No network beyond local spawn; no filesystem writes beyond `state.db` WAL and the child's own cwd.

## Effect class

`DestructiveExternalMutation` (compiled node, `tachyon_core::runtime::compile_operation` `mcp.spawn` arm): starting an untrusted child hands it ambient authority with no recovery path. The node carries capability `mcp.spawn`, operation `{ server_id, approval_id }`, and the ADR-0006 §10 contract pin (`MCP_SPAWN_CONTRACT_VERSION = 1`); misshaped ids fail as `InvalidArgs` before any graph exists. `mcp.spawn` smuggled into a model patch batch fails as `UnknownCapability` — launches compile only through `compile_operation` on the gateway path, never through model proposals — and non-MCP `process.spawn` / `credential.use` / `net.fetch` / `shell.exec` stay `ForbiddenCapability` in both compilers (negative tests).

## Idempotency

`Keyed` on the launch-approval id: one approval launches its exact pinned set exactly once. The park entry leaves the map before any re-authorization or spawn and each per-server policy grant burns on the re-authorization, so a concurrent second approve of the same id fails closed as `approval_missing`; a re-register supersedes still-parked approvals (stale ids fail as `approval_missing`, durable rows marked `consumed-missing`). There is no blind relaunch: reconnects after restart need a fresh register + approve (grants never survive a restart — the pin persists, the grant does not).

## Resource claim

One supervised child per launched server for as long as it stays `live`: an owned process group (`kill_on_drop` — dropping the live handle always signals the child), held stdio pipes, and the recorded inventory. Denied or refused launches hold nothing; failed launches reap synchronously (no zombie, no orphan). Re-registering a live id reaps the superseded child before the fresh park answers.

## Cancellation behavior

Deny is the cancellation: `DenyMCPServers` consumes the park and nothing ever spawns; a later grant of the same id is ignored (`approval_missing`). An approve that finds no session root marks the set `stopped` instead of launching anywhere. There is no mid-handshake cancel bridge this slice — a granted launch runs to the 10 s handshake bound, EOF, or a typed answer.

## Retry policy

Retry freely after a typed refusal (`invalid_mcp_descriptor`, `approval_missing`, `mcp_not_live`) — none spawned anything. After `mcp_spawn_failed` / `mcp_handshake_failed` / `mcp_version_mismatch`, retry only as a fresh human-approved launch (fresh register + grant): the interrupted outcome is unknown per §19, so a blind replay could double-start the child's external effect. Validation refusals are permanent for that shape.

## Verification method

Gateway seam (`crates/tachyon-gateway/tests/mcp_gated_launch.rs`): `register_parks_launch_with_awaiting_approval_and_spawns_nothing`, `deny_marks_refused_and_never_spawns`, `approve_launches_handshake_and_records_inventory`, `reregister_invalidates_the_superseded_approval`, `reregister_of_a_live_server_reaps_the_old_child`, `version_mismatch_reaps_the_child_and_marks_stopped`, `notifying_child_stays_live_through_handshake_with_inventory_intact`, `restart_leaves_stopped_and_approved_reload_reconnects`, `secret_reregister_rearms_the_vault_after_restart`. Call-side counterpart (`crates/tachyon-gateway/tests/mcp_mediated_call.rs`): `notifying_child_stays_live_through_call_with_result_intact`. Compiler seam (`crates/tachyon-core/tests/mcp_tool_compile.rs`, extended): `mcp.spawn` node shape + contract pin, misshaped-scope refusals, patch-path unknown. Store seam: `mcp_approvals` park/decide/one-shot unit tests. Protocol seam (round-trip + v2-stays test). Gate: `cargo verify`.

## Crash-recovery behavior

The interrupted launch classifies `UnknownAfterCrash` per the §19 matrix: the launch fails typed, the row falls back to `stopped` with version and inventory cleared, and the half-started child is reaped — recovery needs a fresh approved launch, never a resume. A crash between park and grant leaves the in-memory park gone but the durable row `parked`: a restarted gateway answers the pre-restart id with `approval_missing` and nothing executes without a fresh register (the durable row is audit, not a grant — grants never survive a restart). Boot resets every leftover `live` row to `stopped`. A crash before the park leaves nothing but unpinned validation rejects.

## Expected latency class

Interactive local spawn plus two supervised child round trips: register is validation + per-server compile + authorize + one pin transaction (no child I/O); grant adds one spawn plus `initialize` and `tools/list` frames bounded at 10 s each (local children answer in milliseconds). No model calls; not on any running task's critical path.

