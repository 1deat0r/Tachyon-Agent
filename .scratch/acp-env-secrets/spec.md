# Spec — Environment and secret handling (issue #57, ADR-0005 release blocker 4)

**Tracker status:** ready-for-agent
**Parent:** [#57 ACP v1 distribution through the local gateway](https://github.com/1deat0r/Tachyon-Agent/issues/57) — ADR-0005 fourth release blocker, "environment and secret handling"
**Authority:** ADR-0005 (Gateway and ownership boundary, line 43), AGENTS.md invariants, CONTEXT.md glossary
**Provenance:** `derived:open-issues` (auto-workflow goal rotation; blockers 1–3 + 5 landed: replay `b7ae280`, reconciliation + MCP stdio + cancellation-drain done-uncommitted in tree)

## Problem Statement

The MCP slice built most of the substrate: `secret: true` env values persist as `CredentialBroker` handles only, launch resolves them from the vault into a cleared child environment, and stderr/results/errors are redacted. But ADR-0005:43 requires treating "the command, arguments, environment entries, and server identity as untrusted" and using "credential handles/redaction rather than persisting raw secrets" — three holes make that aspirational. First, descriptor validation runs only at register (`server.rs:1807-1823`); the launch path deserializes `row.env_json` and spawns without re-checking (`server.rs:2192-2201`, `mcp.rs:334-351`), so a mutated or legacy row bypasses the `LD_PRELOAD`/`DYLD_*` denylist — and the denylist itself covers only three names while interpreter-startup vectors (`BASH_ENV`, `PYTHONPATH`, `NODE_OPTIONS`, `GIT_SSH_COMMAND`, …) pass freely, as does an explicit entry that overrides the inherited `PATH`/`HOME` allowlist. Second, `args` have no secret mechanism at all: a token in `--api-key=…` is persisted raw in `args_json` (`server.rs:1917-1930`) and echoed raw by `ListMCPServers` (`server.rs:2007-2018`), directly contradicting "handles rather than raw secrets". Third, the provider key is registered with the redactor as a startup snapshot (`config.rs:255-259`) while `invoke` re-reads the process env on every call (`openai_compat.rs:1043-1047`) — a key rotated after gateway start is used but never registered, so redaction and use are not the same bytes.

## Solution

From the perspective of a gateway client:

- Stored descriptors are re-validated immediately before every spawn: env name shape, denylist, bounds, and allowlist-override rules are enforced at the use site. A row that fails (mutated, legacy, hostile) refuses launch with a typed, name-only error and starts zero processes.
- The denylist is a deterministic, name-based set covering loader hijack (`LD_PRELOAD`, `LD_LIBRARY_PATH`, `LD_AUDIT`, `GCONV_PATH`, `DYLD_*`) and interpreter/startup injection (`BASH_ENV`, `ENV`, `SHELLOPTS`, `PS4`, `IFS`, `PYTHONPATH`, `PYTHONSTARTUP`, `NODE_OPTIONS`, `PERL5OPT`, `OPENSSL_CONF`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, `GIT_SSH_COMMAND`, `GIT_CONFIG_*`, plus same-class siblings `GIT_SSH`, `GIT_EXEC_PATH`, `GIT_TEMPLATE_DIR`, `JAVA_TOOL_OPTIONS`, `_JAVA_OPTIONS`, `RUBYOPT`); explicit descriptor entries may not override inherited allowlist location keys (`PATH`, `HOME`, `TMPDIR`, `LANG` + platform set).
- MCP `args` accept `secret: true` marking symmetric with env: secret values register with the vault at pin, persist as handles in the free-JSON `args_json` (legacy plain-string rows parse as non-secret, no migration), resolve from the vault only at spawn (missing handle ⇒ typed `mcp_spawn_failed`, fail-closed), and appear as handles in list output, receipts, and durable files.
- The provider key has one source of truth: the value resolved and registered at config load is the value `invoke` sends; mid-run env rotation changes nothing until restart, and redaction coverage provably matches the bytes on the wire.
- What does not change: the two brokers stay separate (disjoint by construction — MCP children never inherit the provider key, provider key never enters MCP rows); `secret: false` remains a client opt-in contract with no heuristic guessing; `command` stays raw (an absolute path is a location, not a credential).

## User Stories

1. As an ACP adapter developer, I want args I mark `secret: true` to be persisted, listed, and receipted as handles — never raw — so that a client-supplied token cannot leak into `state.db` or any response frame.
2. As an ACP adapter developer, I want a hostile or mutated descriptor row to refuse launch with a typed error before any process starts, so that register-time validation is not the only gate between untrusted input and `execve`.
3. As an operator, I want interpreter-startup and loader-hijack env names denied by deterministic rule, so that an approved MCP launch cannot repoint the child's runtime world through env alone.
4. As an operator, I want a restart after pinning to still fail closed on secret args whose vault entry is gone, so that handles never outlive their broker silently.
5. As an operator, I want the registered provider key and the key actually sent to be the same bytes by construction, so that redaction evidence covers the wire, not a stale snapshot.
6. As a reviewer of issue #57, I want env-isolation, args-secret lifecycle, and provider-key single-source tests at the gateway seam, so that the ADR "environment and secret handling" blocker has executable evidence.

## Implementation Decisions

- **Layers touched:** `tachyon-protocol` (args entries wire shape, additive with legacy-string parse), `tachyon-gateway` (launch-path re-validation, denylist expansion, args secret registration/resolution/redaction, launch env-isolation test), `tachyon-store` (no migration — `args_json` is free TEXT JSON holding objects with handle values; comment/doc updates only), `tachyon-app` + `tachyon-models` (provider key single-source: resolved value passed into the provider client; `invoke` stops re-reading env when a resolved key is present, env fallback retained for directly-constructed unit tests). No new crate, no new gateway commands, no ACP wire code.
- **Launch re-validation:** run the full `check_mcp_descriptor` (or its shared core) over each stored row immediately before spawn; failure ⇒ typed `invalid_mcp_descriptor` (name-only messages, never echo values), zero process start, existing launch-failure lifecycle semantics reused. Register-time validation stays (defense in depth, not moved).
- **Denylist:** one deterministic name/prefix rule set shared by register and launch paths (single source of truth, no duplicated lists): exact names cover loader hijack (`LD_PRELOAD`, `LD_LIBRARY_PATH`, `LD_AUDIT`, `GCONV_PATH`) and interpreter/startup injection (`BASH_ENV`, `ENV`, `SHELLOPTS`, `PS4`, `IFS`, `PYTHONPATH`, `PYTHONSTARTUP`, `NODE_OPTIONS`, `PERL5OPT`, `OPENSSL_CONF`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, `GIT_SSH_COMMAND`, plus same-class siblings `GIT_SSH`, `GIT_EXEC_PATH`, `GIT_TEMPLATE_DIR`, `JAVA_TOOL_OPTIONS`, `_JAVA_OPTIONS`, `RUBYOPT`), plus the `DYLD_*` and `GIT_CONFIG_*` prefixes. Allowlist-override rejection reports the offending name only.
- **Args secret shape:** entries `{value, secret}` mirroring `McpEnvEntry`; legacy JSON array-of-strings rows parse as all-`secret:false`; secret entries register in the same `encode_mcp_pins` transaction as env secrets; bounds (≤32 args, ≤4 KiB/arg) apply unchanged; spawn resolves handles via `use_handle` exactly like env. `command` has no secret variant.
- **Provider key single-source:** app passes the already-resolved `SecretValue` bytes into provider construction; `invoke` prefers that over `std::env::var`; registration at `config.rs:255-259` and the wire header read the same value. No re-registration machinery, no rotation support (restart is the contract).
- **Broker disjointness:** documented decision + regression test, NOT a merge (no proven leak path; env_clear construction guarantees it).
- **Ownership invariants unchanged:** Supervisor remains sole logical writer; validation/redaction are pre-execution gates around the existing Supervisor-owned launch, not a parallel path; remote gateway mode untouched.

## Testing Decisions

- **What makes a good test:** external behavior only — commands in, observable responses/state/durable bytes out; assertions are absence-of-secret and typed-error, never timing.
- **Primary seam (existing):** gateway round-trip tests — prior art: `mcp_pinned.rs` (descriptor rejection, handles-only list), `mcp_gated_launch.rs` (stderr-shouting child, restart vault), `mcp_mediated_call.rs` (call receipts), `provider_redaction.rs` (journal/frame scrub).
- **Secondary seams (existing):** unit tests on validation/denylist/legacy-args parse; store row shape (handles in `args_json`/`env_json` after pin, raw secret absent from `state.db*`); `tachyon-tools/tests/secret_env_allowlist.rs` patterns for env-isolation.
- **Required cases:** denylisted/mutated-row launch refuses typed with zero process (row written directly to bypass register); explicit `PATH`/`HOME` override rejected by name; provider key absent from MCP child env AND MCP secrets absent from `process.spawn` receipt (broker-disjointness pin); args secret pin → list (handle) → restart → launch (child receives raw, receipt/list/store show handle) → missing-vault restart fails `mcp_spawn_failed`; legacy plain-string args rows still launch; provider key rotated in env after load → invoke still sends registered key and error redaction still scrubs it.
- **Verification command:** `cargo verify` (repo gate), executed under this session's established Phase 6 provenance judgment.

## Out of Scope

- `secret: false` auto-detection / entropy heuristics (client opt-in contract stays authoritative).
- Merging the provider redactor and the MCP vault (disjoint by construction; regression-pinned instead).
- Model-answer, evidence-file, or task-payload redaction (no secret source on those paths today; separate slice if ever needed).
- OpenJEV/judgment key wiring (latent — not wired to the gateway).
- MCP HTTP/SSE, server-set removal, persistent grants, hosted-provider path, ACP frames/adapter, evaluation.
- Changes to the frozen M14 record, §46 deferrals, remote-gateway defaults, or architecture requirements.

## Further Notes

- ADR-0005 blocker order: blockers 1–3 closed by slice (a) (`b7ae280`), the reconciliation slice, and the MCP stdio slice; blocker 5 (cancellation drain + crash recovery) closed by the cancel-drain slice; this spec closes the remaining blocker 4 (environment and secret handling). After it, all five release blockers are closed and the ACP stdio lifecycle adapter becomes the next candidate slice.
- Glossary: **Credential handle** added to `CONTEXT.md` during grilling; use it exactly (raw bytes reach exactly one sink, never a row, prompt, or log).
- Capability doc: UPDATE `docs/agents/mcp-stdio-capability.md` (checklist rows for args-secret marking, launch re-validation, denylist, `secret: true` client contract) — extends the shipped MCP capability, no new file.
- No new ADR: ADR-0005:43/:51 already decides the requirement; this is additive, reversible implementation.
