# Spec — ACP session/request_permission permission bridge (issue #57, ADR-0005 approvals through the adapter)

**Tracker status:** ready-for-agent
**Parent:** [#57 ACP v1 distribution through the local gateway](https://github.com/1deat0r/Tachyon-Agent/issues/57) — residue item "permission bridge" from the adapter lifecycle slice
**Authority:** ADR-0005 (Approvals and cancellation, lines 47–49), AGENTS.md invariants, CONTEXT.md glossary
**Provenance:** `derived:open-issues` (auto-workflow goal rotation; adapter lifecycle slice landed uncommitted; residue list in `docs/agents/acp-adapter-capability.md`)

## Problem Statement

The adapter runs real turns end-to-end, but when a task parks for approval the prompt fails typed (`-32004 approval_required`) — approvals cannot be answered by an ACP client at all. ADR-0005:47-49 requires exactly that path: bind every approval to the pending operation, offer only one-shot `allow_once`/`reject_once`, validate the returned optionId against what was offered (unknown/unoffered ⇒ fail closed), let cancellation win, and have the client answer `cancelled` to outstanding permission requests before the drain ack. The gateway side is fully built (supervisor parks, one-shot decisions, cancel-expiry, late-approve typed refusals) and the codec's outbound agent→client request path (`Peer::send_request`) shipped with no production caller — but the serve loop has no request variant, discards unsolicited responses, and the turn pipeline has no notion of a permission exchange. Three concrete hazards sit behind the missing wiring: `status → WaitingApproval` journals *before* the `approval_request` (so snapshot-driven detection races), a denied run journals no terminal status (a naive bridge would hang to the 300 s turn timeout), and permission prompts are human-paced while the turn deadline is not.

## Solution

From the perspective of an ACP client:

- When the turn's task parks, the adapter announces a `tool_call` (`toolCallId` minted, `title` from the approval summary) via `session/update`, then sends `session/request_permission` with exactly two options — `allow_once` and `reject_once`. Detection is journal-driven (`approval_request` frames, including Subscribe-replay), never derived from a `GetTask` snapshot alone.
- The client's response is validated strictly: `selected` + `allow_once` ⇒ gateway `Approve`; `selected` + `reject_once` ⇒ gateway `Deny`; any other optionId or outcome shape ⇒ fail closed as a `Deny` (the operation must never run). After the decision the adapter emits `tool_call_update` (`completed`/`failed`) and the turn resumes on the allow path (the benign `WaitingApproval → Executing` bounce settles as non-terminal and streaming continues).
- A deny settles the prompt honestly: after a bounded grace wait for a terminal status, the prompt answers `stopReason: refusal` (schema-legal) — it never hangs to `turn_timed_out` and never guesses `end_turn`. The gateway's pre-existing non-terminal-after-deny state is out of scope and logged as a known issue.
- `session/cancel` with an outstanding request resolves it locally as `cancelled` without sending any decision (the gateway expires the parked row itself before the terminal journal; a late approve is typed-refused). The drain-ack ordering from the lifecycle slice is unchanged: cancel reply → prompt `stopReason: cancelled`.
- The 300 s turn deadline is suspended while a permission request is outstanding (human-paced); cancel and client disconnect remain the unblock paths. A park observed with no request frame after a bound falls back to the existing typed `approval_required` (orphan safety, never a silent hang).
- What does not change: one-shot-only options, no `allow_always`, gateway parks stay the durable truth, the adapter executes nothing, MCP parks stay out of scope (they never reach the subscription), and `initialize` needs no new advertisement.

## User Stories

1. As an editor user, I want an ACP permission prompt to appear when my task needs approval, so that allowing or rejecting it actually drives the run instead of failing the turn.
2. As an ACP client, I want exactly `allow_once`/`reject_once` options with strict response validation, so that a malformed answer can never grant work.
3. As an ACP client, I want a `tool_call` announcement tied to each permission request, so that the request references something I can render.
4. As an operator, I want a rejected approval to settle the prompt as `refusal` promptly, so that a deny never wedges a turn for five minutes.
5. As an ACP client, I want cancelling during a permission prompt to resolve it locally as `cancelled` and then report `stopReason: cancelled` after the drain ack, so that ADR-0005:49's ordering is what I observe.
6. As an operator, I want the turn deadline suspended while a human is being asked, so that slow answers are not mistaken for dead runs.
7. As the Task Supervisor, I want the adapter to keep issuing only `Approve`/`Deny` against the existing one-shot registry, so that approvals remain Supervisor-owned and auditable.
8. As a reviewer of issue #57, I want scripted-fixture tests covering allow, deny, invalid-response, cancel-during-request, timeout suspension, and orphan fallback, so that ADR-0005:47-49 has executable evidence at the ACP seam.

## Implementation Decisions

- **Layers touched:** `tachyon-acp` only — `codec.rs` (new `Outbound::Request` variant; write-time id registration by the serve loop; `Parsed::Response` routed to the owning turn instead of discarded; distinct id space from client-initiated ids), `server.rs` (queue/register/write path preserving the single-writer invariant; cancel interaction with outstanding requests), `turn.rs` (`handle_journal` + `process_replay` recognize `approval_request`/`approval`; permission-exchange state on `SessionState` or the turn future; option validation; Approve/Deny sequenced through the existing single settlement slot via `GatewayConn::send`; deny→refusal settlement; timeout suspension; orphan fallback). No gateway/core/protocol/store changes; no new crate; no new commands.
- **Tool-call announcement:** `tool_call` update {toolCallId, title: summary} before the request; `tool_call_update` {toolCallId, status: completed|failed} after the decision — shapes from the pinned schema-v1.23.0 artifact (re-fetch from `agentclientprotocol/agent-client-protocol` tag `schema-v1.23.0` / commit `6d08f412…` if the local copy is gone).
- **Response contract:** strictly `allow_once`/`reject_once`; unknown optionId, unknown outcome, or malformed shape ⇒ fail closed as `Deny` + log (ADR:47). Standalone `outcome: cancelled` (no session/cancel in flight) ⇒ no-approval ⇒ `Deny` + log. Response never arriving is bounded only by disconnect/cancel.
- **Deny settlement:** journal `approval {granted:false}` → bounded grace wait for terminal status → else `stopReason: refusal`. The existing `Failed → -32004 task_failed` mapping stays untouched (unreachable branch).
- **Cancel:** outstanding request resolved locally (`cancelled`), zero decision frames sent; `release_parked` gateway-side already expires the row; drain-ack frame ordering from the lifecycle slice unchanged and re-pinned.
- **Timeout:** `TURN_TIMEOUT` paused while a request is outstanding; document; orphan park (WaitingApproval, no request frame, bound elapses) ⇒ existing typed `approval_required`.
- **Advertisement unchanged:** `session/request_permission` is not an `initialize` capability (schema: no AgentCapabilities flag) — no advertisement change is legal or needed.
- **Ownership invariants unchanged:** decisions still flow through `Command::Approve/Deny` → Supervisor one-shot registry; adapter holds no grant authority; remote mode untouched.

## Testing Decisions

- **What makes a good test:** external behavior only — fake ACP client observes outgoing `tool_call`/`request_permission` frames and answers them; scripted gateway drives parks deterministically (live `run_policy` never Asks — established fact).
- **Primary seam (existing fixtures):** `crates/tachyon-acp/tests/common/scripted.rs` parks + journal injection; `session_prompt_stream_edges.rs:192`'s test is REWRITTEN (bridge replaces immediate `approval_required` with a request emission); pre-StartRun supervisor park (restart_approval.rs pattern) for the pre-stream branch; `session_cancel_live.rs` GatedProvider fixture for cancel-during-request ordering.
- **Required cases:** allow path (request emitted with exactly the two options → client `allow_once` → Approve issued → Executing bounce → chunks resume → `end_turn`); deny path (reject_once → Deny → `tool_call_update failed` → prompt settles `refusal` within the grace bound, NOT `turn_timed_out`); invalid optionId/outcome ⇒ Deny issued (fail closed), operation never runs; standalone `cancelled` outcome ⇒ Deny; cancel during outstanding request (local resolution, no decision frames, drain-ack order re-pinned, prompt `cancelled`); timeout suspended while outstanding (fake clock or long-bound proof); orphan fallback ⇒ `approval_required`; replayed `approval_request` in Subscribe ack handled; tool_call/tool_call_update shapes golden-pinned against the artifact; units for option-validation table + timeout-suspension logic.
- **Verification command:** `cargo verify` under this session's established Phase 6 provenance judgment.

## Out of Scope

- MCP parks (never journal `approval_request` — decided by different command families; future MCP-at-setup slice must keep namespaces separate).
- `session/load`, attach/reconnect, ResourceLink, non-text prompt content.
- Gateway/core changes: task-approval expiry, deny-leaves-non-terminal-status cleanup, new test APIs for live mid-turn parks, exposing `task_failure` on the wire.
- `allow_always`/`reject_always`, persistent grants, advertisement changes, new ADR.

## Further Notes

- ADR-0005:47-49 already decides this contract; this spec makes it executable at the ACP seam (ADR:104 implementation-slice rule).
- The lifecycle slice's `approval_required` typed error becomes an orphan fallback, not the primary behavior — its test is rewritten, not deleted.
- Biggest adjudicated risk (grill Q6): deny → `refusal` avoids the 300 s hang; the underlying gateway status gap is logged as a known issue feeding a future core slice.
- Schema artifact lives at `/tmp/opencode/acp-schema-v1.json` (ephemeral) — re-fetch pinned tag if gone; ADR:47-49 is the binding contract regardless.
