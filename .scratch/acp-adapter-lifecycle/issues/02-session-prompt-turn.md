# 02: session/new + session/prompt turn pipeline

**What to build:** `session/new` maps to gateway `CreateSession`: absolute `cwd` required (typed JSON-RPC error otherwise), non-empty `mcpServers` refused with a typed error (absent/empty ok), returned ACP `sessionId` IS the Tachyon `SessionId` (identity round-trip, no alias store). `session/prompt` runs one complete turn: adapter-local sequential guard (overlapping prompt in the same session ⇒ typed error, never queued), fresh idempotency key minted per call (in-process retry reuses it), `CreateTask { session_id, objective = prompt text, idempotency_key }`, `StartRun`, `Subscribe { task_id, after_seq: 0 }`, forwarding `agent_message` journals as `session/update` `agent_message_chunk` (Text only — other journal kinds omitted this slice, never mislabeled), then final response from `GetTask`'s conversation tail with `stopReason`: `Completed → end_turn`, `Cancelled → cancelled`; `Failed` has no schema-legal stopReason in the pinned ACP schema (no `"error"` member), so it answers a typed `-32004` `task_failed` error frame; unknown/ambiguous status ⇒ typed error, never a guessed verdict. Empty prompt text ⇒ typed error.

**Blocked by:** 01 (needs the codec, gateway client, and session-less scaffolding from 01)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-02)

- [x] `session/new` with non-absolute `cwd` ⇒ typed error and NO gateway call issued; with absolute cwd ⇒ gateway session created, response `sessionId` equals the gateway `session_id` byte-for-byte
- [x] `session/new` with non-empty `mcpServers` ⇒ typed error, no session created; absent/empty ⇒ ok
- [x] Identity durability: `sessionId` resolves after adapter process restart (fresh adapter instance, same id → GetSession-equivalent succeeds) — no new store surface
- [x] Full prompt round trip against a live test gateway: fake ACP client sends `session/prompt`, receives ≥1 `session/update` `agent_message_chunk`, then a final response whose `stopReason` is `end_turn` and whose content equals the turn's final agent text
- [x] Overlap refusal: second `session/prompt` for a session with an active turn ⇒ typed error, first turn unaffected (asserted by completing it afterwards)
- [x] Idempotency: same-call retry reuses the key (gateway returns the original response — no duplicate task); a NEW prompt call mints a new key (two prompts ⇒ two tasks/turn_seqs)
- [x] StopReason table unit-tested: `Completed → end_turn` and `Cancelled → cancelled` map; `Failed` ⇒ typed `-32004` `task_failed` error branch (never a schema-divergent `"error"` verdict) + unknown-status ⇒ typed error branch
- [x] Empty prompt text ⇒ typed error, no CreateTask issued
- [x] `agent_message` chunk forwarding asserted from the live Subscribe stream (chunks arrive BEFORE the final response); non-message journals produce no fake chunks
