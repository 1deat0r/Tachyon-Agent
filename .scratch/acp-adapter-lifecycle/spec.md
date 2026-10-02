# Spec — ACP stdio lifecycle and gateway-backed prompt turns (issue #57, ADR-0005 first adapter slice)

**Tracker status:** ready-for-agent
**Parent:** [#57 ACP v1 distribution through the local gateway](https://github.com/1deat0r/Tachyon-Agent/issues/57) — planned follow-on slice "Implement the ACP stdio lifecycle and gateway-backed prompt turns"
**Authority:** ADR-0005 (Decision: protocol pin, Gateway and ownership boundary, Approvals and cancellation), AGENTS.md invariants, CONTEXT.md glossary
**Provenance:** `derived:open-issues` (auto-workflow goal rotation; all five ADR-0005 release blockers now landed: replay `b7ae280` + reconciliation, MCP stdio, cancellation-drain, env/secret done-uncommitted in tree)

## Problem Statement

All five ADR-0005 release blockers are implemented and verified, but there is no ACP surface at all: `crates/` holds 20 members and zero ACP code, so no editor client can speak to Tachyon. The gateway already exposes everything a first adapter needs over its length-prefixed frame protocol (`CreateSession`, `CreateTask` with idempotency keys, `StartRun`, `Subscribe` with cursor replay, `GetTask`, `CancelTask` with synchronous drain, `GetSession`), and the CLI/TUI already prove the client sequence — but none of it is reachable by an ACP client, which speaks newline-delimited JSON-RPC 2.0 on its own stdio with negotiated capabilities, mandatory `session/new|prompt|cancel|update` methods, and a hard rule that unimplemented capabilities stay unadvertised. Until an adapter exists, "ACP v1 distribution" is a decision without a program, and none of the blocker slices have an executable consumer.

## Solution

From the perspective of an ACP client process:

- Spawning `tachyon-acp` starts an agent that negotiates `protocolVersion: 1` over newline-delimited JSON-RPC on stdio (stdout carries only ACP frames, stderr logs), advertises exactly what works — `loadSession: false`, Text-only prompt content, no filesystem/terminal claims — and never starts the gateway itself: if the endpoint file is missing or the socket refuses, every request fails with one clear actionable error.
- `session/new` requires an absolute `cwd` and maps to gateway `CreateSession` with that root; the returned ACP `sessionId` IS the durable Tachyon `SessionId` (identity round-trip), so lookup survives adapter and gateway restarts with no new store surface. A non-empty `mcpServers` parameter is refused with a typed error (MCP-at-setup arrives in a later slice); absent/empty is accepted.
- `session/prompt` is one complete turn: mint a fresh idempotency key, `CreateTask` (durable turn association), `StartRun`, then `Subscribe` and forward `agent_message` journals as `session/update` `agent_message_chunk`s; overlapping prompts in one session are refused, never queued. The prompt response returns the turn's final agent text with a `stopReason` derived from `TaskStatus` (`Completed → end_turn`, `Cancelled → cancelled`; `Failed` has no schema-legal stopReason in the pinned ACP schema, so it answers a typed `-32004` `task_failed` error frame instead of a success frame); an unknown or ambiguous state is likewise a typed JSON-RPC error, never a guessed verdict.
- `session/cancel` maps to `CancelTask`, whose synchronous response is the Supervisor's drain acknowledgement (ADR-0005:49 — a `Cancelled` status alone never proves drain); only after it returns does the adapter answer `cancelled` for the in-flight prompt. The gateway remains the sole owner of task state, approvals, effects, and recovery; the adapter executes nothing itself.
- What does not change: the frame protocol, the Supervisor's sole-writership, gateway lifecycle (user-started, remote mode disabled), one-shot approval semantics, and the advertise-only-implemented rule (later slices add `session/load`, MCP-at-setup, and the permission bridge as their own verified cuts).

## User Stories

1. As an editor-integration developer, I want a standard `tachyon-acp` agent process I can launch as my ACP client's subprocess, so that one prompt round trip runs a real Tachyon turn through the already-running gateway.
2. As an ACP client, I want `initialize` to advertise only `loadSession: false` and Text-only prompts, so that I never attempt a method the agent cannot honor.
3. As an ACP client, I want a missing or dead gateway to surface as one clear typed error on every request, so that Tachyon's user-started-gateway contract is never violated by an implicit launch.
4. As an ACP client, I want `sessionId` to remain valid across adapter and gateway restarts, so that the identity I was handed is durable by construction.
5. As an ACP client, I want live `session/update` chunks while the turn runs and a `stopReason` I can trust after it ends, so that cancellation, success, and failure are distinguishable without polling.
6. As an ACP client, I want a second overlapping `session/prompt` to be refused with a typed error, so that the sequential-turn contract of ACP v1 is enforced deterministically.
7. As the Task Supervisor, I want the adapter to be a pure gateway client — no driver, no tools, no approvals, no effects — so that ADR-0005's "no parallel execution path" invariant is structurally true.
8. As a reviewer of issue #57, I want round-trip tests that drive the adapter with a fake ACP client against a live test gateway, so that the lifecycle contract has executable evidence.

## Implementation Decisions

- **New crate `tachyon-acp`** (binary): workspace member added to root `Cargo.toml` members, `docs/02_IMPLEMENTATION_SPEC.md` §1 crate bullet, and `CONTEXT.md` crate shorthand — all three are pinned by `docs_freshness.rs` tripwires. Deps: `tachyon-protocol`, `tachyon-gateway` (for the exported `transport` module), `tachyon-types`, `tokio`, `serde_json`; the three `tachyon-*` workspace crates are declared `{ workspace = true }` (all are in `[workspace.dependencies]`, like their peers); inline `path = ...` only for non-workspace deps.
- **Two codecs, one process:** ND-JSON-RPC 2.0 on stdio (generic over `AsyncRead`/`AsyncWrite` so tests drive it over tokio duplex; request-id → pending-call map; notifications without id skipped; stderr for logs, stdout frames only) and the gateway's u32-LE length-prefixed frames via an in-crate minimal client mirroring `tachyon-app/src/client.rs` (no dependency on `tachyon-tui`; no shared-client extraction).
- **Method set this slice:** `initialize` (+ `notifications/initialized`), `session/new`, `session/prompt`, `session/cancel`. Everything else ⇒ standard JSON-RPC method-not-found. `session/new` params: absolute `cwd` required (typed error otherwise), `mcpServers` non-empty ⇒ typed error, absent/empty ⇒ ok.
- **Identity session mapping:** ACP `sessionId` = Tachyon `SessionId` string (no alias table, no migration).
- **Turn pipeline:** per prompt — reject if this session already has an active turn (adapter-local sequential guard + gateway idempotency), mint idempotency key, `CreateTask { session_id, objective=prompt text, idempotency_key }`, `StartRun`, `Subscribe { task_id, after_seq: 0 }`; forward journals → `session/update` (`agent_message` → `agent_message_chunk` Text only); on terminal status, build the final response from `GetTask`'s conversation tail; `stopReason` mapping Completed→`end_turn`, Cancelled→`cancelled`, Failed→typed error `-32004` `data: "task_failed"` (no schema-legal stopReason in the pinned ACP schema), else typed error. In-process retry of the SAME call reuses its key; a fresh call always mints a new one.
- **Cancel pipeline:** `session/cancel` → `CancelTask` and await its response (drain ack) before replying `cancelled`; if it resolves the in-flight prompt first, the prompt response carries `stopReason: cancelled`. Approval-parked turns without a permission bridge fail with a typed error surfaced through the prompt (known issue; next slice).
- **Advertisement:** `initialize` response advertises `protocolVersion: 1` and the minimal agent capability set actually implemented (`loadSession: false`, Text-only prompt capabilities, nothing else); no client filesystem/terminal methods are ever invoked.
- **Ownership invariants unchanged:** adapter holds no task state, executes no tools, grants no approvals; gateway remains sole logical writer; remote gateway mode untouched; no ADR change.
- **New capability:** full AGENTS.md checklist documented in `docs/agents/acp-adapter-capability.md` (why/schemas/access set/effect class/idempotency/resource claim/cancellation/retry/verification/crash recovery/latency).

## Testing Decisions

- **What makes a good test:** external behavior only — fake ACP client writes ND-JSON-RPC lines, asserts response lines and exit behavior; no assertions on internal adapter state; timing only as generous bounds (subscribe reconnect windows).
- **Primary seam (new tests, existing gateway fixtures):** `crates/tachyon-acp/tests/` round-trips against a live test gateway (gateway setup prior art: `tachyon-gateway/tests/run_path.rs`, `g5_e2e.rs`, `tests/common/mod.rs`): initialize negotiation + advertisement shape; gateway-down typed error with no process launch; session/new absolute-cwd accept/reject + mcpServers refusal; full prompt turn (update chunks arrive, final response + `end_turn`); overlap refusal; cancel mid-turn → drain-ack order + `stopReason: cancelled`; restart-durable sessionId — adapter-restart durability is proven at the ACP seam (`session_prompt_edges.rs::session_id_resolves_after_adapter_restart`), while gateway-restart persistence is covered by the gateway's own suites (`session_root.rs`, `session_history.rs`, `turn_sequence.rs`): the identity mapping carries zero adapter state, so no new ACP-level gateway-restart test is required.
- **Secondary seams:** codec unit tests (framing, request/response correlation, notification skip, malformed-line error); `stopReason` mapping unit table; neighbors green: `tachyon-app --test docs_freshness` (crate-list tripwires), TUI suites, gateway suites.
- **Required cases:** as listed in Primary; plus "unknown method ⇒ method-not-found", "prompt with empty text ⇒ typed error", "Subscribe overflow (ResyncRequired) re-subscribes or fails the prompt typed — never silently truncates updates".
- **Verification command:** `cargo verify` (repo gate) under this session's established Phase 6 provenance judgment.

## Out of Scope

- `session/load` / `loadSession: true` (needs recorded-turn reconciliation; own slice), reconnect/attach to live tasks, session-scoped subscriptions.
- MCP servers at session setup (non-empty params refused; gateway-side MCP mediation already shipped), `session/request_permission` permission bridge (parked approvals surface as typed prompt errors; own slice), ResourceLink, image/audio/embedded-resource content, persistent permission options.
- Adapter-side durable state (idempotency keys are in-process), shared-client-crate extraction, gateway frame-protocol changes, remote gateway mode, evaluation/live-model runs.
- Changes to the frozen M14 record, §46 deferrals, or architecture requirements; new ADR (ADR-0005 governs).

## Further Notes

- ADR-0005:104: the ADR authorizes no capability before its implementation slice and verification pass — this spec IS that slice for the stdio lifecycle and prompt turns; later slices take load, MCP-at-setup, and the permission bridge.
- Glossary: **ACP adapter** row added to `CONTEXT.md` during grilling; use it exactly (a gateway client, never an execution path).
- Identity sessionId mapping is a deliberate reduction of ADR-0005:37's "durably maps" requirement — durable by construction (sessions table), revisit only if aliasing is ever needed.
- Known issue carried forward: `docs_freshness` tripwires must be updated in the same commit-ready state as the crate (spec §1 bullet + CONTEXT shorthand + members) or `cargo verify` fails.
