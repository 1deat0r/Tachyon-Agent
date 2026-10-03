# Spec — ACP `session/load` + `loadSession: true` (issue #57, ADR-0005 durable session load and ordered replay)

**Tracker status:** ready-for-agent
**Parent:** [#57 ACP v1 distribution through the local gateway](https://github.com/1deat0r/Tachyon-Agent/issues/57) — first residue item in `docs/agents/acp-adapter-capability.md` "Remaining before an 'ACP supported' claim"
**Authority:** ADR-0005:29 (replay-before-response), ADR-0005:39 (recorded-turn reconciliation before a later prompt), ADR-0005:40 (pinned workspace + ordered replay, load never creates/restarts/re-enters), ADR-0005:31 (advertise-only-implemented), AGENTS.md invariants
**Provenance:** `derived:open-issues` (auto-workflow goal rotation 2026-10-03; candidate confirmed from code: `GetSession` already returns ordered turns + conversation, `0004_turn_seq`/`0005_create_task_idempotency` migrations persist the recorded turn, ACP seam is one method arm + the `loadSession:false` golden)

## Problem Statement

The adapter refuses `session/load` (`-32601` method-not-found) and advertises `loadSession: false`, so an ACP client that restarts cannot resume a durable Tachyon session at all — yet the gateway already stores everything needed: `GetSession` returns the session's pinned `workspace_root` and every turn in `turn_seq` order with its `task_id`, `status`, and canonical `conversation` (`{speaker, content}` rows, byte-stable across gateway restarts). Two hazards sit behind the missing arm: (1) the ACP contract requires the FULL conversation to be streamed as `session/update` notifications BEFORE the `session/load` result — a response-first implementation would be a client-visible lie; (2) nothing outside this adapter process knows a session's previous turn is still running — the gateway's `CreateTask` has no per-session overlap guard, and the adapter's sequential-turn guard only sees turns started in the current process, so after an adapter restart a fresh `session/prompt` would happily create a second live turn for the same session (ADR-0005:39 forbids exactly this).

## Solution

From the perspective of an ACP client:

- `session/load` with `{sessionId, cwd, mcpServers}` validates params first (absolute `cwd`; `mcpServers` absent/empty accepted, non-empty refused typed `mcp_servers_unsupported` — same contract as `session/new`), probes gateway liveness, then issues `GetSession`. Unknown session ⇒ typed `-32002 unknown_session`; `cwd` not equal to the session's pinned `workspace_root` ⇒ typed `-32602 workspace_mismatch` (loading never rebinds the pinned root).
- On success the adapter streams every recorded conversation entry in `turn_seq` order as `session/update` notifications — `speaker: "user"` → `user_message_chunk`, `speaker: "agent"` → `agent_message_chunk`, text `content` verbatim — and only THEN answers the request with `{}` (schema `LoadSessionResponse`, no optional fields supported). `messageId` is omitted (optional in schema; no honest ACP message id exists). Empty history ⇒ zero notifications, still `{}`. Load creates no task, restarts nothing, re-enters nothing (ADR-0005:40); it is a read-only replay and freely repeatable.
- **Recorded-turn reconciliation (ADR-0005:39):** after a successful load, the adapter records the session's LAST recorded turn when its status is non-terminal. While that task stays non-terminal, a `session/prompt` on that session is refused typed `-32003 turn_in_progress` (checked against a fresh gateway `GetTask` at prompt entry, so a turn that finished while the adapter was down releases itself without human intervention); `session/cancel` remains the explicit stop path (ADR-0005:41). A terminal (or absent) last turn ⇒ prompts proceed exactly as today.
- `initialize` keeps `loadSession: false` until replay and the prompt gate are both green; the final ticket flips it to `true` and re-pins the byte-exact advertisement golden (ADR-0005:31).
- What does not change: MCP-at-setup/pinned-set reconnect stays its own residue slice; attach/reconnect to a live turn's updates (ADR-0005:41 "may attach") stays its own residue slice; no `session/resume` (that is a different method); no advertisement of modes/configOptions/sessionCapabilities we do not implement; gateway/core/protocol/store untouched — the arm runs on the existing `GetSession`/`GetTask` commands.

## User Stories

1. As an ACP client that restarted, I want to call `session/load` and see the full durable conversation replayed in order before the response, so the UI looks uninterrupted.
2. As an ACP client, I want `loadSession: true` advertised only when load actually works, so I never call a method that will be refused.
3. As an operator, I want a prompt after load to be refused while the recorded turn is still running, so Tachyon never runs two live turns in one session (ADR-0005:39).
4. As an operator, I want the gate to self-release when the recorded turn has since reached a terminal status, so an adapter restart does not wedge a session.
5. As an ACP client, I want a typed `workspace_mismatch` when I load with the wrong cwd, so I cannot accidentally rebind a pinned workspace root.
6. As an ACP client, I want `session/cancel` to remain the way I stop a recorded in-progress turn after loading, so recovery follows the explicit Supervisor policy (ADR-0005:41).
7. As a reviewer of issue #57, I want scripted/live tests pinning replay order, typed refusals, the prompt gate, and the advertisement golden, so ADR-0005:29/39/40 have executable evidence at the ACP seam.

## Implementation Decisions

- **Layers touched:** `tachyon-acp` only — `turn.rs` (`parse_load` param validation, a `load_session` pipeline mirroring `prompt_turn`'s validate→probe→`GetSession` shape but read-only; `SessionState` gains `recorded: Mutex<HashMap<SessionId, TaskId>>`; `try_acquire_turn` gains the recorded-turn gate: fresh `GetTask` on the recorded id, non-terminal ⇒ `-32003 turn_in_progress`, terminal/missing ⇒ clear the record and proceed), `server.rs` (wire `session/load` to the handler; `loadSession` flag flip in the last ticket), `codec.rs` (only if the outbound replay notifications reuse existing `Outbound::notification` — no new frame types). No gateway/core/protocol/store changes.
- **Replay mapping:** `speaker ∈ {user, agent}` → the two chunk types; any other speaker value ⇒ logged-skip (data, never guessed into a role); `content` must be a string (non-string ⇒ logged-skip, replay continues — a corrupt row must not wedge the whole load).
- **Gate semantics:** the gate fires ONLY on a session whose load recorded a non-terminal turn, AND only while that turn is still non-terminal per a fresh `GetTask`. `GetTask` failure at the gate fails typed (fail-closed, `-32001` gateway class). The record is cleared on: gate release (terminal), `session/cancel` completing for that task, or adapter restart (records are in-process state; a fresh load re-establishes them — no durable writer).
- **Idempotency/repeatability:** load is a read → no idempotency key, no `calls` registry entry, repeatable; overlapping a live prompt from the SAME connection is refused by the existing sequential-turn guard? No — load never takes the turn slot: it is a pure read and is allowed alongside anything (it cannot mutate). Order-safety: the replay is a snapshot; a turn streaming concurrently may append rows the snapshot misses — acceptable (client gets `{}` then sees subsequent `session/update` live only if attached; documented as snapshot semantics).
- **Advertisement unchanged until last:** `loadSession:false` golden stays byte-identical through tickets 01-02; ticket 03 flips one byte plus both tests.

## Testing Decisions

- **What makes a good test:** external behavior only — a fake ACP client observes notification order and the final `{}`; the scripted gateway fixture drives `GetSession`/`GetTask` deterministically (live gateway never records a non-terminal turn mid-load without a running driver).
- **Primary seam:** scripted fixture (`tests/common/scripted.rs`) for replay ordering + typed refusals; `session_prompt_edges.rs`-style live test for the prompt gate; `initialize_live_gateway.rs` for the advertisement flip.
- **Required cases:** replay order across 3 turns (user/agent interleaved, byte-identical order to `turn_seq`); replay-before-response ordering (notifications strictly precede the `{}` result frame); empty history ⇒ `{}` with zero notifications; unknown session ⇒ `unknown_session`; non-empty `mcpServers` ⇒ `mcp_servers_unsupported`; non-absolute cwd ⇒ `cwd_not_absolute`; cwd mismatch ⇒ `workspace_mismatch`; gate: recorded non-terminal turn ⇒ later prompt `-32003 turn_in_progress`, recorded terminal turn ⇒ prompt proceeds; gate self-release after recorded turn goes terminal (fresh `GetTask` clears); gate released by `session/cancel`; malformed conversation row (non-string content / unknown speaker) ⇒ logged skip, load still succeeds; golden: `initialize` advertisement `loadSession:true` byte-exact; units: `parse_load` table, speaker mapping table, gate decision table.
- **Verification command:** `cargo verify` under this session's established Phase 6 provenance judgment.

## Out of Scope

- MCP servers at session setup / pinned-set reconnect on load (ADR-0005:43; separate residue slice).
- Attach/reconnect to a live turn's streaming updates (ADR-0005:41 "may attach"; separate residue slice).
- `session/resume`, `session/close`, `additionalDirectories`, modes/configOptions/sessionCapabilities.
- Gateway-side overlap enforcement (the adapter gate is the ACP-boundary enforcement; a gateway invariant would need an ADR).
- Durable adapter records (records are in-process; cross-restart state re-derives from `GetSession` at the next load).

## Further Notes

- Schema pin: `agentclientprotocol/agent-client-protocol` tag `schema-v1.23.0`, `schema/v1/schema.json` (`LoadSessionRequest`/`LoadSessionResponse`, `SessionUpdate` → `user_message_chunk`/`agent_message_chunk`, `ContentChunk.messageId` optional) — local copy in `/tmp/acp-schema-v1.23.0.json` is ephemeral; re-fetch from the tag if gone.
- Protocol doc pin: agentclientprotocol.com/protocol/session-setup §Loading Sessions (replay via `session/update`, then `{}`).
