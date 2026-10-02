# Evidence — Phase 5-2, TASK 02 (fail-closed outcomes), acp-permission-bridge slice

**Implementer:** auto-workflow sub-agent (rules 1–19). **Date:** 2026-10-02.
**Ticket:** `.scratch/acp-permission-bridge/issues/02-fail-closed-outcomes.md` — all boxes checked, `Status: done` lands in the S5 commit.
**Scope guard honored:** only adapter files (`crates/tachyon-acp` src + tests) + capability doc + workflow logs + tracker. `session_permission_allow.rs` and `session_prompt_stream_edges.rs` (ticket 01) are UNMODIFIED (`git diff --name-only` never lists them; their suites pass: 2/2 and 5/5). Ticket 03 territory untouched except the S3 route guard, which sends nothing while a cancel owns the exchange (no double-decide). `state.md` untouched. No writes outside the repo.

## Commit cadence (rule 19 — one small task = one commit, each pushed by the post-commit hook)

| Small task | Commit | Verify |
|---|---|---|
| S1 Deny settlement path | `b62dfce` `feat(acp): bridge S1 deny settlement path (#57)` | `deny_settles_the_prompt_as_refusal` green (5.01 s) |
| S2 Fail-closed response validation | `91a32f0` `feat(acp): bridge S2 fail-closed response validation (#57)` | `invalid_responses_fail_closed_as_deny` green (0.00 s) |
| S3 Standalone `cancelled` ⇒ Deny | `4508ce9` `feat(acp): bridge S3 standalone cancelled denies (#57)` | `standalone_cancelled_outcome_denies` green (5.01 s) |
| S4 Late deny-journal race pin | `42506d4` `feat(acp): bridge S4 late deny journal race pin (#57)` | `late_deny_journal_with_no_request_is_handled` green |
| S5 Doc wording + full regression | this commit | `cargo verify` exit 0 |

## File-by-file (whole ticket)

- `crates/tachyon-acp/src/turn.rs` — `DENY_SETTLE_GRACE` (5 s), `DENY_REASON_REJECTED`, `DENY_REASON_CANCELLED`; `refusal_prompt_result()` = `{stopReason:"refusal",content:[]}`; `PermissionPhase` + `Denying{approval_id,tool_call_id,reason}` and `Denied{deadline}`; `PendingDecision::{Approve,Deny}` (one write site, sequenced around the single settlement slot at loop step 1, `awaiting.is_none()` gate unchanged); `tool_call_update` `completed` after Approve / `failed` after Deny (byte-pinned goldens); loop step 4 bound generalized to `read_bound` → `ReadExpiry::{Orphan,DenySettle}` — Orphan ⇒ typed `approval_parked`, DenySettle ⇒ `Ok(refusal_prompt_result())`; `PermissionAnswer` + `classify_permission_answer` (M2a: `allow_once`/`reject_once` only under `selected`; `cancelled` valid; else `Invalid{reason}`); `answer_action(response, cancel_owns)` — the single funnel before any Approve path (M2b): Allow→Grant, Reject→Deny, Invalid→Deny+`warn` (reason: "the ACP client sent an invalid permission response (…); failing closed"), Cancelled+cancel mark→`NoDecision`, Cancelled standalone→Deny+`warn`, error frame/EOF→typed `approval_parked` (unchanged from ticket 01); `ClientAnswer::NoDecision`; `SessionState.cancels_in_flight` per-session consume-on-once mark (`arm_cancel_resolution` called in `cancel_pipeline` BEFORE `CancelTask`, `take_cancel_resolution` consumed by the turn on a `cancelled` payload only, cleared at `try_acquire_turn`); `observe_approval_journal` wired for `kind == "approval"` — observation + log ONLY, arms nothing.
- `crates/tachyon-acp/tests/common/scripted.rs` — `denies_seen: Vec<(task_id, approval_id, reason)>` + accessor; `Command::Deny` arm delegates to `deny_answer` (extracted because `script_answer` hit clippy `too_many_lines` 101/100): fixed reaction mirroring real `decide` — response `{"task": Executing}`, journals `approval {granted:false}` then `status Executing`, NEVER a terminal status (the known gap). No `Script` field added ⇒ ticket 01's `Script` literals untouched.
- `crates/tachyon-acp/tests/session_permission_deny.rs` — NEW file, three scripted tests + shared helpers (`deny_script`, `park_until_request`, byte-pinned goldens for announce/request/failed-update).
- `docs/agents/acp-adapter-capability.md` — 8 line edits (M5a): intro residue list; `stopReason` set now `end_turn|cancelled|refusal` with the 5 s deny-grace default documented; `approval_required` narrowed (no longer claims "client did not grant"); turn-pipeline contract = full allow/deny/invalid/standalone-cancelled/cancel-owned/journal-observation sequence; cancellation section names the deny path + zero-decision cancel mark; retry section adds the `refusal` verdict; Verification-method test list adds 5 units + 3 round-trip tests; residue bullet now ships allow+deny and defers cancel/timeout/orphan to the next slice.
- `docs/agents/auto-workflow/decisions.md` — decision rows for every small task (S1×6, S2×4, S3×4, S4×3, S5×2), all `source: agent-default` except the commit-cadence rows (`user-invocation`).
- `.scratch/acp-permission-bridge/issues/02-fail-closed-outcomes.md` — checkboxes flipped per small task; `Status: done` in S5.

## Red / mutation evidence (every small task)

Each mutation was applied to the working tree, the Verify test was run RED, then reverted byte-identically (`grep -c MUTATION` = 0 after each revert).

- **S1:** `refusal_prompt_result()` mutated to `"end_turn"` ⇒ `deny_settles_the_prompt_as_refusal` FAILED: `assertion left == right failed: a deny settles refusal: {...,"stopReason":"end_turn"}` (5.01 s). Revert ⇒ green. (Also: the S1 test asserts refusal at `elapsed >= 4 s` — proves the bounded grace WAIT ran, not an instant guess.)
- **S2:** `classify_permission_answer` mutated so `outcome:"selected"` + ANY optionId ⇒ `Allow` ⇒ `invalid_responses_fail_closed_as_deny` FAILED at the classify assertion (line 2630): `{ "outcome": "selected", "optionId": "allow_always" } must classify invalid`. Revert ⇒ green.
- **S3:** guard arm mutated (`if cancel_owns || !cancel_owns` — standalone also skips the decision) ⇒ `standalone_cancelled_outcome_denies` FAILED: `timed out waiting for the response for 1` (30.26 s) — zero `Deny` issued, prompt never settled. Revert ⇒ green (deny file 3/3).
- **S4:** `handle_journal` deny-journal arm mutated to `return Err(approval_parked())` (deciding from the journal) ⇒ `late_deny_journal_with_no_request_is_handled` FAILED: reply was `-32004 approval_required` instead of `end_turn`. Revert ⇒ green.
- **S5 first `cargo verify` run was RED at `cargo fmt --all -- --check`** (formatting drift in files committed during S1–S4). Fix: `cargo fmt --all` (format-only diff, rides this commit). Second run: exit 0.

## Checkbox proofs

- **S1/M1a:** scripted test pins `fixture.denies_seen() == [(SCRIPT_TASK_ID, APPROVAL_ID, reason containing "ACP client")]`, length 1, `approvals_seen` empty, written only when the settlement slot is free (loop step 1, unchanged gate).
- **S1/M1b:** `updates[0]` byte-equals the golden `tool_call_update {"status":"failed"}` frame; `updates.len() == 1`.
- **S1/M1c + N1a1:** units `deny_grace_expiry_defaults_to_refusal_without_a_terminal_status` (bound maps `Denied`→`DenySettle`, ≤ 5 s; `refusal_prompt_result` needs no terminal status; `AwaitingRequest`→`Orphan`; Idle/Sent unbounded) and scripted `elapsed >= 4 s && < 30 s`.
- **S1/N1a2:** unit `refusal_never_maps_to_turn_timed_out_or_end_turn` — stopReason is exactly `refusal`, never `end_turn`/`turn_timed_out`; NO terminal status maps to `refusal` (single source).
- **S2/M2a + N2a1:** unit `invalid_responses_fail_closed_as_deny` — 13-case table: unknown optionId (incl. `allow_always`/`reject_always`), unknown outcome, `selected` without optionId, non-object (`string`/`null`/`42`/`[]`), `{}` ⇒ each `Invalid` and each funnels to `ClientAnswer::Deny` whose reason contains `ACP client` + `invalid`; `allow_once` ⇒ Grant; `reject_once` ⇒ Reject; `cancelled` ⇒ Cancelled (valid shape); only `allow_once` grants.
- **S2/M2b:** `answer_action` is the only call site of `receiver.await` in the loop; it runs before any phase can become `Approving`.
- **S3/M3a:** scripted `standalone_cancelled_outcome_denies` — one `Deny`, reason contains `ACP client` + `cancelled`; unit asserts the same deny shape.
- **S3/M3b:** unit `standalone_cancelled_denies_but_a_cancel_mark_blocks_a_decision` — `answer_action(cancelled, cancel_owns=true)` ⇒ `NoDecision` (zero decision frames); mark is per-session and consumed on first read (stale mark can never suppress a later standalone deny). `cancel_pipeline` arms the mark before `CancelTask`; `try_acquire_turn` clears stale marks.
- **S4:** scripted `late_deny_journal_with_no_request_is_handled` — journal with NO request ever: `denies_seen` empty, `approvals_seen` empty, `get_task_calls == 2` (journal arms no read), `end_turn` + tail, stderr contains `journalled deny observed`, exit 0.
- **S5/M5a:** doc diff = 8 lines, all deny/refusal/fail-closed + test-name updates (above).
- **S5/M5b:** `cargo verify` exit 0; ticket 01's two test files untouched; allow suites 2/2 and 5/5.

## Gate outputs

- Per-task gates: `cargo test -p tachyon-acp --locked` green after every small task (lib 44 → 45 → 46 → 46 units; deny file 1 → 2 → 3 tests); `cargo clippy -p tachyon-acp --all-targets --locked -- -D warnings` clean after every small task.
- **Final full gate: `cargo verify` → exit 0** (log: `/tmp/opencode/verify-s5.log`). Workspace tests: **831 passed, 0 failed** (984 `test result` assertions incl. zero-test targets); fmt check clean; `cargo check --workspace --locked` clean; workspace clippy `-D warnings` clean; xtask clippy clean.
- Named Verifies in the final run: `deny_settles_the_prompt_as_refusal ... ok`, `standalone_cancelled_outcome_denies ... ok`, `late_deny_journal_with_no_request_is_handled ... ok`, `invalid_responses_fail_closed_as_deny ... ok`.

## Known issues / handover

- Gateway still journals no terminal status after a deny (pre-existing gap, out of scope): the adapter's 5 s grace is the load-bearing `refusal` default; logged as a known issue feeding a future core slice.
- Ticket 03 owns: resolving an outstanding request locally on cancel (the S3 mark + `NoDecision` branch is its insertion point), closing/observing the announced tool call on the cancel path, `TURN_TIMEOUT` suspension, orphan-grace tightening. The S3 guard must keep sending zero decision frames there.
- Error-frame / dropped-slot answers still fail typed `approval_required` (ticket 01 behavior; not listed in this ticket's validator table).
