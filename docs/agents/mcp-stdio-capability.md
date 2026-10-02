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

# Capability: environment and secret handling at the MCP and provider boundary (env-secrets slice)

Capability documentation required by `AGENTS.md` § New capability checklist. Covers the gateway-level surface added for issue #57 tickets 01–02 plus the provider-key single-source change — ADR-0005's fourth release blocker, "environment and secret handling" (ADR-0005:43: command, arguments, environment entries, and server identity are untrusted; credential handles/redaction rather than persisted raw secrets). Glossary terms: **MCP server**, **Credential handle** (CONTEXT.md) — every secret crosses this boundary as a handle that resolves to raw bytes at exactly one sink (the child's `execve` argv/env, or the provider transport header) and appears as a bare handle everywhere durable or wire-visible.

## Why deterministic code cannot already solve it

Two decisions here are untrusted-input decisions no model should make and no heuristic can prove. First, whether a *stored* descriptor row may spawn is unknowable at register time alone: the row is durable text that anything with write access to `state.db` (or a legacy writer) can mutate, so only deterministic re-validation at the use site can decide "may this row reach `execve`" — and which env names repoint a child's runtime world is a name-class question answered by a fixed rule set, never by a model. Second, whether an argument *value* is a credential is not inferable from the bytes (an opaque token and a path look alike); the client's explicit `secret: true` marking is therefore a hard contract enforced deterministically, not a guess. Redaction coverage is likewise a construction property: only single-sourcing the provider key can make "registered bytes == sent bytes" true by construction rather than by inspection. The capability itself runs no model/Jev call.

## Input / output schema

- **`RegisterMCPServers.servers[].args` entries** — `[{ value, secret }]`, symmetric with `McpEnvEntry`; a legacy plain-string JSON args array dual-parses as all `secret: false` (no migration — `args_json` is free TEXT JSON). `secret: true` values register with the vault in the same pin transaction as env secrets and persist to `args_json` as bare credential handles only (`mcp-secret-1`); list frames, receipts, and durable files carry the handle, never the raw arg bytes. Arg bounds (≤32 args, ≤4 KiB/arg, NUL-free) apply to entries unchanged; `command` stays raw — an absolute path is a location, not a credential. At spawn, secret args resolve from the vault exactly like secret env; a handle this gateway's vault never saw refuses the launch with typed `mcp_spawn_failed` before any process starts.
- **`secret: true` client contract** — the client MUST mark every credential value; Tachyon performs no auto-detection and no entropy heuristics. An unmarked secret persists and travels as a plain value by contract, not by defect.
- **Launch-time descriptor re-validation** — immediately before every spawn, the full `check_mcp_descriptor` core re-runs over each stored row (server-id shape, absolute command, arg bounds/NUL, env name shape, denylist, env value bounds, inherited-allowlist override). Failure refuses with typed `invalid_mcp_descriptor`, name-only (values are never echoed), starts **zero processes**, and the existing launch-failure lifecycle marks the row `stopped`. Register-time validation stays as defense in depth; the launch re-validation is the use-site gate.
- **Expanded denylist (one shared register + launch rule set)** — `is_dangerous_mcp_env` is a single deterministic name/prefix rule set consulted by register-time validation and launch-time re-validation alike, never a copy: exact loader-hijack names (`LD_PRELOAD`, `LD_LIBRARY_PATH`, `LD_AUDIT`, `GCONV_PATH`) and exact interpreter/startup names (`BASH_ENV`, `ENV`, `SHELLOPTS`, `PS4`, `IFS`, `PYTHONPATH`, `PYTHONSTARTUP`, `NODE_OPTIONS`, `PERL5OPT`, `OPENSSL_CONF`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, `GIT_SSH_COMMAND`, `GIT_SSH`, `GIT_EXEC_PATH`, `GIT_TEMPLATE_DIR`, `JAVA_TOOL_OPTIONS`, `_JAVA_OPTIONS`, `RUBYOPT`), plus the `DYLD_*` and `GIT_CONFIG_*` prefixes. The set is a **representative sample of the loader-hijack and interpreter-startup classes**, not an exhaustive enumeration of every vector in either class. Matching is exact-case: the gate is scoped to Linux/POSIX children, where env names are case-sensitive; Windows' case-insensitive environment matching is out of scope for this slice and not covered (a case-variant denylisted name would not match on Windows).
- **Inherited allowlist override rejection** — an explicit descriptor entry may never override an inherited allowlist location key (`PATH`, `HOME`, `TMPDIR`, `LANG` + platform set): rejected by NAME only, the value never echoed.
- **Provider key single-source** — the value resolved and registered with the redactor at config load is the value the provider client sends as the transport header; `invoke` prefers the resolved key and stops re-reading process env (env fallback retained only for directly-constructed clients with no resolved key). Mid-run env rotation changes nothing until restart — that is the contract, not a defect.
- No new gateway commands; wire shapes stay additive inside protocol v2.

## Access set

Pre-spawn re-validation reads the pinned `mcp_servers` row and runs pure CPU checks — no new store writes beyond the existing launch lifecycle (`mark_mcp_servers(stopped)` on refusal). Secret-arg resolution clones the in-memory MCP vault (`mcp_credentials`), exactly like secret env; a refusal names only the argument position. **Broker disjointness:** the provider redactor (`GatewayRuntime.redactor`) and the MCP vault (`mcp_credentials`) are two separate `CredentialBroker` instances, disjoint by construction — MCP children get `env_clear` + allowlist so they never inherit the provider key, MCP secrets never enter the run `ToolsContext` / `process.spawn` path, and the provider key never enters an MCP row — regression-pinned rather than merged. The provider single-source change touches provider construction only (`tachyon-app` → `tachyon-models`), no store and no new process.

## Effect class

`DestructiveExternalMutation` unchanged: re-validation and secret resolution are pre-execution gates around the existing Supervisor-owned `mcp.spawn` launch, never a parallel path — they can only refuse a spawn, never start one. The provider-key change adds no effect class: the same header bytes go on the wire, only sourced from the resolved key.

## Idempotency

Launches stay `Keyed` on the approval id. A validation refusal is permanent for that row shape until a fresh register replaces the row (nothing executed); a lost-vault refusal is permanent for that gateway process until a fresh register re-arms the vault. Provider single-source is construction-time state: per process lifetime `invoke` is deterministic regardless of env rotation.

## Resource claim

None new: one pure-CPU validation pass per stored row immediately pre-spawn, one vault clone per launch, one resolved key per provider client. No ownership, lease, or registry entry beyond what the approved launch already holds.

## Cancellation behavior

Unchanged — no new cancel bridges. A refused launch holds nothing (zero processes), so cancellation has nothing to drain for it; the in-flight-launch and call semantics above are untouched.

## Retry policy

Retry freely after typed `invalid_mcp_descriptor` — nothing spawned, and the refusal is permanent for that row shape until a fresh register writes a valid row. After typed `mcp_spawn_failed` from a missing secret handle (restart with an unarmed vault), retry only as a fresh register + fresh approval: §19 forbids blind replay of an unknown-outcome launch, and the fresh register is the supported re-arm.

## Verification method

Unit seam (`crates/tachyon-gateway/src/server.rs`, `mcp_descriptor_tests`): `denylist_covers_loader_and_interpreter_vectors`, `denylisted_names_are_refused_by_name_without_the_value`, `inherited_allowlist_keys_are_never_overridable`, `check_mcp_descriptor_covers_env_value_bounds`. Spawn-gate units (`crates/tachyon-gateway/src/mcp.rs`): `unarmed_vault_refuses_spawn_before_any_process_starts`, `unarmed_vault_refuses_spawn_for_secret_args_before_any_process`. Gateway seam: `mutated_row_launch_refuses_typed_with_zero_process`, `secret_arg_resolves_into_argv_and_lists_handle_only`, `missing_secret_arg_handle_refuses_spawn_with_zero_process`, `mutated_arg_row_refuses_launch_typed_with_zero_process`, `legacy_plain_string_args_row_still_launches`, `secret_reregister_rearms_the_vault_after_restart`, `secret_arg_reregister_rearms_the_vault_after_restart`, `stderr_echoing_child_keeps_secrets_out_of_durable_files` (`tests/mcp_gated_launch.rs`); `malicious_descriptors_are_rejected_and_pin_nothing`, `pin_then_list_reports_handles_only_and_survives_restart`, `secret_arg_pins_as_handle_and_never_lists_raw_bytes` (`tests/mcp_pinned.rs`); `mcp_child_env_is_allowlist_plus_pins_and_secrets_stay_out_of_process_spawn` (`tests/mcp_env_isolation.rs` — env-isolation pin *and* the broker-disjointness regression: the child env equals the inherited allowlist plus pinned entries with the registered provider key absent, the MCP secret reaches only the child's `execve`, and the ambient provider key appears in no `process.spawn` receipt or spool); `secret_env_stays_handles_across_list_receipt_and_durable_files`, `secret_arg_reaches_argv_and_receipt_redacts_to_handle`, `secret_echoed_as_a_json_key_returns_handle_only` (`tests/mcp_mediated_call.rs`). Store seam: `pinned_handles_survive_reopen_without_raw_secrets`. Protocol seam: `mcp_args_parse_legacy_strings_and_secret_entries`. Provider/app seam: `resolved_key_is_registered_with_the_redaction_registry` and `registered_key_is_the_transport_header_after_process_env_rotation` (`crates/tachyon-app/src/config.rs`), `resolved_key_is_sent_after_env_rotation_and_env_remains_the_fallback` (`crates/tachyon-models/src/openai_compat.rs`), `registered_key_is_scrubbed_before_any_client_can_read_it` (`tests/provider_redaction.rs`). Gate: `cargo verify`.

## Crash-recovery behavior

Classification is unchanged: a refusal is a typed failure with zero processes and the row `stopped`, so crash recovery still needs a fresh approved launch — never a resume. Handles are minted per broker lifetime: a restart empties the in-memory vault while durable rows keep naming their bare handles, so any secret row launched after a restart fails closed (`mcp_spawn_failed`, zero process) until a fresh register re-arms the vault — handles never outlive their broker silently. Provider key rotation across a restart is expected: the new process resolves and registers the new key at load.

## Expected latency class

No added round trips and no added IPC: one pure-CPU validation pass per stored row immediately pre-spawn (bounded rows validate in microseconds) plus one vault clone; the provider key is resolved once at config load instead of per call (strictly fewer env reads). No model/Jev calls; not on any running task's critical path.
