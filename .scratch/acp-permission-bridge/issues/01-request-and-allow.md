# 01: Outbound permission request + allow path

**What to build:** The agent→client request channel and the happy path of a permission exchange. Add `Outbound::Request` to the codec; the serve loop registers the minted id BEFORE writing (single-writer invariant preserved, distinct id space from client-initiated ids) and routes the client's `Parsed::Response` to the owning turn instead of discarding it. The turn pipeline detects `approval_request` journals in `handle_journal` AND `process_replay` (journal-driven, never snapshot-driven), mints a `toolCallId`, announces `session/update` `tool_call` {toolCallId, title: summary}, then sends `session/request_permission` with exactly `allow_once` + `reject_once` options (schema-verified shapes). On `selected`+`allow_once`: issue gateway `Approve` sequenced around the single settlement slot, emit `tool_call_update` {status: completed}, treat the `WaitingApproval → Executing` bounce as benign, resume streaming, end `end_turn`.

**Blocked by:** None (can start immediately)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-02; proving tests: session_permission_allow.rs (2), session_prompt_stream_edges.rs (allow rewrite + orphan stub), turn.rs/codec.rs units)

- [x] `Outbound::Request` written only by the serve loop; id registered pre-write; client response correlated to the waiting turn (unit: synthetic request→response round trip through `serve()`; unsolicited unrelated responses still ignored safely)
- [x] `approval_request` handled in live frames AND in Subscribe-replay (`process_replay`) — the ask is never read from a `GetTask` snapshot alone (scripted replay test)
- [x] Outgoing frames golden-pinned against the pinned schema artifact: `tool_call` update shape, `session/request_permission` params (sessionId, toolCall required, options EXACTLY the two one-shot entries with `optionId`/`name`/`kind`)
- [x] Allow flow end-to-end (scripted gateway): park → tool_call announced → request emitted → client answers `selected`/`allow_once` → `Approve` reaches the gateway with the right task_id+approval_id → `tool_call_update completed` → Executing bounce settles benign → chunks resume → prompt `end_turn`
- [x] Replay-ack park (approval_request already in the Subscribe ack) produces the same request emission
- [x] Existing suites stay green: acp 55+ tests, docs_freshness 10/10; the old `approval_parked_prompt_fails_typed_within_a_bounded_window` test is REWRITTEN to expect a request (bridge replaces immediate refusal on the request-bearing path)
