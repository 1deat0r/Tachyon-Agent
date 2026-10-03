# TASK 01: session/load arm — validate, GetSession, replay, respond
**Status:** done
**Blocked by:** None
**What to build:** `session/load` becomes a real read-only arm in `tachyon-acp`: `parse_load` validates params (`sessionId` parseable, `cwd` absolute, `mcpServers` absent/empty — non-empty ⇒ typed `mcp_servers_unsupported`, relative cwd ⇒ `cwd_not_absolute`, missing ⇒ `invalid_params`) BEFORE the liveness gate; `run_load` then connects, issues `GetSession` (gateway refusal ⇒ typed, e.g. `unknown_session` via the existing `-32002` mapping; missing `workspace_root`/`turns` ⇒ `gateway_payload_invalid`), `check_load_workspace` rejects a `cwd` that differs from the pinned root (new marker `workspace_mismatch`, `-32602`), and the recorded turns replay in `turn_seq` order as `session/update` notifications (`speaker: user` → `user_message_chunk`, `agent` → `agent_message_chunk`; unknown speaker / non-string content ⇒ `tracing::warn` + skip, never wedge) BEFORE the final `{}` result. ALL load frames go through the single writer channel (`tx`) so FIFO order is exactly replay-then-response (ADR-0005:29); the inline `handle_request` return path would race ahead of queued notifications. Load takes no turn slot, holds no idempotency key, creates nothing (ADR-0005:40). `loadSession` stays `false` (ticket 03 flips it).

Decomposition note (learned from the first attempt): `parse_load` cannot land unwired — `clippy -D warnings` dead-code fails the tree, and an arm without replay would violate ADR-0005:29 for the duration of that commit. S1 is therefore the whole arm + pins; S2 adds the fixture seam + wire tests.

**Verify (ticket):** `cargo test -p tachyon-acp` exit 0 + `cargo verify` exit 0 at S2.

## Small tasks (each = one commit, in order)
- [x] S1 arm lands: `parse_load` + `check_load_workspace` + `load_replay_frames` + `run_load` + server arm (tx ordering) + units + the two `-32601` pin updates (codec splits `session/load` from wholly-unknown/`session/resume`; live test moves load to typed `unknown_session`) · Verify: `load_refuses_bad_params_before_the_gateway`, `load_requires_the_pinned_workspace_root`, `replay_maps_speakers_and_skips_corrupt_rows`, `session_load_validates_before_the_gateway`, updated `unknown_and_unimplemented_session_methods_yield_standard_method_not_found`, updated `initialize_succeeds_against_a_live_gateway_with_clean_framing`
  - [x] M1a param table one pure fn; workspace check one pure fn; replay mapping one pure fn
    - [x] N1a1 every refusal reuses an existing data marker except `workspace_mismatch` (only new marker, documented in the unit)
    - [x] N1a2 no notifications queued on any error path (all fallible work precedes the sends)
- [x] S2 fixture seam + scripted wire tests: `ScriptedGateway::start_with_get_session` (override `GetSession` result; default keeps today's echoed shape) + `session_load_replay.rs` · Verify: `session_load_replays_history_in_turn_order_then_responds`, `session_load_with_empty_history_answers_immediately`, `session_load_unknown_session_is_typed`, `session_load_workspace_mismatch_is_typed`; full `tachyon-acp` suite + `cargo verify` exit 0
  - [x] M2a override is `Option<CommandResult>` in `ScriptState`; default path byte-identical to today (existing tests untouched)
    - [x] N2a1 replay notifications strictly precede the `{}` response frame (asserted from the client side)
    - [x] N2a2 zero notifications on both typed refusals
