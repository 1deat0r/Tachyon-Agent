# TASK 01: session/load arm — validate, GetSession, replay, respond
**Status:** ready
**Blocked by:** None
**What to build:** `session/load` becomes a real read-only arm in `tachyon-acp`: `parse_load` validates params (`sessionId` parseable, `cwd` absolute, `mcpServers` absent/empty; non-empty ⇒ typed `mcp_servers_unsupported`, bad cwd ⇒ `cwd_not_absolute`, missing ⇒ invalid params), liveness probe, `GetSession` (unknown ⇒ `-32002 unknown_session`; `cwd` ≠ pinned `workspace_root` ⇒ `-32602 workspace_mismatch`), then stream every recorded conversation entry in `turn_seq` order as `session/update` (`speaker: user` → `user_message_chunk`, `agent` → `agent_message_chunk`; non-string content or unknown speaker ⇒ `tracing::warn` + skip, never wedge), and answer `{}` strictly AFTER the last notification. Load takes no turn slot, holds no idempotency key, creates nothing (ADR-0005:40). `loadSession` stays `false` (ticket 03 flips it).
**Verify:** `cargo test -p tachyon-acp` — new scripted test `session_load_replays_history_in_turn_order_then_responds` (3 turns user/agent interleaved; assert every `session/update` frame index < result frame index; byte order == turn order), `session_load_with_empty_history_answers_immediately` (zero notifications, `{}`), units: `load_refuses_bad_params_before_the_gateway` (table: unknown sessionId shape, relative cwd, non-empty mcpServers, missing cwd/sessionId), `load_requires_the_pinned_workspace_root` (unknown session → `unknown_session`; cwd mismatch → `workspace_mismatch`), `replay_maps_speakers_and_skips_corrupt_rows` (user/agent mapping + non-string content + unknown speaker ⇒ skipped, order preserved). Existing method-not-found tests (`codec.rs` `unknown_and_unimplemented_session_methods_yield_standard_method_not_found`, `initialize_live_gateway.rs`) must be updated IN THIS TICKET (load no longer answers `-32601`; the wholly-unknown-method half keeps the pin).

## Small tasks (each = one commit, in order)
- [ ] S1 `parse_load` + typed refusals + units · Verify: `load_refuses_bad_params_before_the_gateway`, `load_requires_the_pinned_workspace_root`
  - [ ] M1a param table extracted to one pure fn (sessionId/cwd/mcpServers)
    - [ ] N1a1 every refusal reuses an existing data marker except `workspace_mismatch` (only new marker, documented)
- [ ] S2 replay pipeline: GetSession → notifications → `{}` + scripted test · Verify: `session_load_replays_history_in_turn_order_then_responds`, `session_load_with_empty_history_answers_immediately`, `replay_maps_speakers_and_skips_corrupt_rows`
  - [ ] M2a `speaker` mapping fn + corrupt-row skip
    - [ ] N2a1 replay never emits before validation/probe errors (ordering assertion in test)
- [ ] S3 method-not-found test updates + full `tachyon-acp` suite green · Verify: `cargo test -p tachyon-acp` exit 0; codec + live tests updated to the new contract
  - [ ] M3a codec test splits load from wholly-unknown method
    - [ ] N3a1 live test still pins `-32601` for `totally/unknown` only
