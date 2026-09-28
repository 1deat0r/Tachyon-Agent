# 0005 — ACP v1 through the local gateway

**Status:** accepted · 2026-09-28

**Decision revision:** 2

## Context

The frozen M14 MVP is TUI-first. `MVP_REPORT.md` and `PROGRESS.md` record ACP as post-MVP work, while the 24 September consideration checklist says the ACP-versus-TUI choice was “Not chosen in grill.” That wording records the checklist’s historical state; it is not evidence of an earlier formal ACP decision.

After the M14 freeze, issue [#57](https://github.com/1deat0r/Tachyon-Agent/issues/57) selects stable ACP v1 as Tachyon’s first post-MVP editor-client compatibility target. This ADR records that decision on 2026-09-28 without changing the MVP record or lifting any deferred-work boundary.

The architecture already makes CLI and TUI clients of one persistent gateway. Adding ACP as another client preserves one Task Supervisor and one execution path. A separate ACP runtime would duplicate ownership of task state, effects, approvals, recovery, and verification.

## Decision

### Protocol and artifact pin

- Implement ACP wire protocol **v1**, negotiated as integer `protocolVersion: 1` during `initialize`.
- Pin schema-generation and compatibility review to ACP JSON Schema release **`schema-v1.23.0`**, tag commit `6d08f412a7a1370d3cc9a124e3be3d6acf92641e`, released 2026-09-18. The protocol version and schema artifact version are separate: the artifact version does not determine wire compatibility.
- Do not target the v2 draft. Upgrade the schema snapshot only through a reviewed issue/PR that checks the wire protocol, generated artifacts, and interoperability contract.

### ACP requirements and Tachyon target

The ACP v1 contract requires an Agent to implement `initialize` and return a negotiated protocol version and its capabilities. Agents MUST support the baseline session methods `session/new`, `session/prompt`, `session/cancel`, and `session/update`, plus Text and ResourceLink prompt content. Agents MUST support the MCP stdio transport; clients MAY provide server configurations, and agents SHOULD connect to the configured servers. The client launches the ACP Agent subprocess using UTF-8 newline-delimited JSON-RPC; stdout carries only valid ACP messages, while stderr may carry logs.

ACP `session/load` is optional. An Agent that advertises `loadSession: true` MUST replay the complete conversation before responding. Tachyon chooses complete durable session loading and ordered replay as a release requirement, and must not advertise `loadSession` before that contract is implemented. Tachyon also chooses to support client-supplied MCP servers over stdio for its ACP release target.

Advertise any optional Agent capability only after its full behavior is implemented. Capabilities outside the initial target include image/audio/embedded-resource prompts, additional workspace roots, MCP HTTP/SSE, and session resume/close. Do not offer persistent permission options. Do not invoke Client filesystem or terminal methods unless that Client advertises them and Tachyon implements the corresponding policy-checked behavior.

### Gateway and ownership boundary

- The ACP Agent is a client of the already-running local gateway. It does not start or restart the gateway implicitly. If the gateway is unavailable, the Agent returns a clear actionable error. Remote gateway mode remains disabled by default.
- The gateway and Task Supervisor remain the only path for starting and coordinating work. The Task Supervisor is the sole logical writer for task state and owns capability checks, validated Execution IR, effects, approvals, cancellation, recovery, and verification-gated completion. ACP metadata and model proposals never grant authority.
- The Gateway/store durably maps each ACP `sessionId` to one Tachyon `SessionId`, its authorized canonical workspace, and a monotonically ordered session turn/message history. Session lookup and replay work across ACP process restarts; process-local adapter memory is not authoritative.
- On `session/new`, require an absolute ACP `cwd` and authorize the requested root through existing workspace trust/capability policy before binding its canonical identity. On load, require the same workspace identity. Reject ungranted roots, implicit workspace switches, and unsupported additional roots.
- Each sequential `session/prompt` is one complete ACP turn and creates a fresh durable Task in that Session. Persist the accepted turn-to-Task association before starting work; do not reuse a terminal Task or accept overlapping turns. ACP JSON-RPC request IDs correlate transport messages and are not cross-connection idempotency keys. If task creation/start or its response is ambiguous, do not retry `StartRun` or create a duplicate. Require `session/load` to reconcile the recorded turn before accepting a later prompt; return a clear ambiguous/in-progress result if it cannot be reconciled. Recovery and driver re-entry remain explicit Supervisor decisions. ACP v1 has no standard mid-turn steering method; the supported control path is to cancel the current turn, wait for Supervisor drain, then submit a new prompt.
- `session/load` validates the pinned workspace and replays the full durable conversation in stable session order, including the existing turn’s current state. Loading does not create, restart, or re-enter a Task and does not repeat effects. Any recovery or driver re-entry follows the existing Supervisor recovery policy and explicit user action.
- Losing the ACP stdio connection does not implicitly cancel a Gateway Task. The existing Supervisor continues the task or recovers it under Tachyon’s normal policy. Reconnect/load exposes that existing durable state and may attach to its live updates; it does not create a Task, restart a driver, or replay work. Explicit `session/cancel` is required to stop an active task.
- Treat prompt Text and ResourceLinks as user-provided data/evidence, not policy. Resolve ResourceLinks only through bounded evidence access inside the session’s declared roots and existing policy. Reject unsupported, unresolved, oversized, or out-of-root resources safely. Do not advertise embedded `Resource` content unless separately implemented.
- MCP server subprocesses supplied by the client use the MCP stdio transport, which is separate from the ACP Agent’s own stdio connection. Treat the command, arguments, environment entries, and server identity as untrusted. Tachyon’s selected process working directory is the pinned session root unless another scope is separately policy-authorized. The act of launching a server must itself be a Supervisor-owned validated Execution IR/process operation with explicit access, effect/idempotency, resource, policy, approval, cancellation, and recovery semantics. Pin the authorized server set for a Session; on load, reconnect only that set or fail closed. Use credential handles/redaction rather than persisting raw secrets. Route every MCP tool proposal through the same Task Supervisor, validated Execution IR, capability/effect policy, and verification path. Never let an MCP subprocess call around those boundaries.

### Approvals and cancellation

- Bind every approval to the exact validated operation and its scope. Validate the returned option ID against the options offered for that still-pending operation. Unknown, unoffered, stale, or mismatched responses fail closed and cannot approve or dispatch work. The Task Supervisor serializes approval decisions and cancellation: if cancellation is accepted first, it invalidates the pending approval and blocks later dispatch; late approval responses are ignored. If approval is accepted first, any resulting work remains subject to cancellation and effect recovery.
- Offer one-shot `allow_once` and `reject_once` outcomes only. Do not offer `allow_always` or `reject_always` until persistent grants have a separate durable policy design.
- On `session/cancel`, the ACP Client MUST answer each outstanding `session/request_permission` request with outcome `cancelled`. The Agent treats that result as no approval, stops model and tool work, waits for a Supervisor-owned drain acknowledgement, sends final updates, and only then returns `stopReason: cancelled` for the original `session/prompt` request. A `Cancelled` task status alone does not prove drain. Cancellation acknowledgement does not claim that an external effect was undone; effect idempotency and recovery rules still apply.

Before ACP can be called supported, Tachyon must implement durable session lookup and ordered replay, safe prompt creation/start reconciliation, MCP subprocess launch/tool mediation, environment and secret handling, and Supervisor-owned cancellation drain and crash recovery in separate issue-sized slices. These are release blockers; the ACP layer must not add a parallel or less restrictive execution path.

## Alternatives considered

1. **Keep TUI as the only client after MVP** — rejected as the first post-MVP distribution target. It would preserve current behavior but would not provide the selected editor-protocol integration point.
2. **Give the ACP process its own driver, store, or tools** — rejected because it would create multiple owners of canonical task state and consequential effects, contrary to the architecture freeze.
3. **Start the gateway automatically from the ACP process** — rejected because it changes gateway lifecycle and recovery semantics without a separate architecture decision.
4. **Implement editor-specific integrations first** — deferred in favor of one standard client boundary. Any later surface must remain a gateway client.
5. **Target ACP v2 draft or HTTP/SSE transport now** — rejected because v2 is draft and HTTP/SSE is not part of the pinned v1 stdio target.

## Evidence

### Official ACP sources

- [ACP project versioning](https://github.com/agentclientprotocol/agent-client-protocol#versioning) distinguishes negotiated protocol versions from Rust crate and JSON Schema artifact versions.
- [Schema v1.23.0 release](https://github.com/agentclientprotocol/agent-client-protocol/releases/tag/schema-v1.23.0) and [immutable v1 schema snapshot](https://github.com/agentclientprotocol/agent-client-protocol/tree/6d08f412a7a1370d3cc9a124e3be3d6acf92641e/schema/v1) identify the pinned artifact.
- ACP v1 contracts: [initialization](https://agentclientprotocol.com/protocol/v1/initialization), [transports](https://agentclientprotocol.com/protocol/v1/transports), [session setup and MCP](https://agentclientprotocol.com/protocol/v1/session-setup), [prompt turns and cancellation](https://agentclientprotocol.com/protocol/v1/prompt-turn), and [tool calls and permissions](https://agentclientprotocol.com/protocol/v1/tool-calls).

### Tachyon sources

- `docs/01_ARCHITECTURE_FREEZE.md` — Task Supervisor, gateway/client separation, durable state, effect policy, verification gate, and remote-mode default.
- `docs/02_IMPLEMENTATION_SPEC.md` §§3, 15, 17–19, 27–29, 32–37, 41, 45–46 — session/task split, sole writer, journal and recovery, trust, capabilities, gateway, and MVP deferrals.
- `docs/04_IMPLEMENTATION_PLAN.md` — MVP milestone order and the post-MVP boundary.
- `MVP_REPORT.md` and `PROGRESS.md` — frozen TUI-first M14 status and recorded ACP post-MVP direction.
- `docs/11_CONSIDERATION_CHECKLIST.md` — historical distribution question, resolved by this ADR.
- [Issue #57](https://github.com/1deat0r/Tachyon-Agent/issues/57) — post-MVP ACP map, sequencing, and completion gate.

## Consequences

- ACP becomes the first planned editor-client surface after the frozen MVP; TUI remains the MVP surface and remains a gateway client.
- ACP support is not complete until the required v1 methods/content, durable session identity and replay, policy-mediated stdio MCP, approvals, cancellation drain, disconnect/reconnect, and crash recovery work together.
- A session load is history replay, not task execution. Driver re-entry remains explicit and Supervisor-owned.
- New provider or protocol features remain optional. Unimplemented capabilities stay unadvertised; unmediated tools, incomplete approvals, and ambiguous effects block release.
- No change is made to the frozen M14 report, MVP exit dispositions, remote-gateway default, or §46 deferred capabilities.

## Migration/rollback plan

The ACP adapter is a separable gateway client. Disable or remove the adapter without changing the gateway’s task/session schema or the existing CLI/TUI clients. Persisted Sessions and Tasks remain accessible through existing clients. Do not delete or rewrite journal history as part of adapter rollback.

If an ACP behavior cannot be mapped safely to existing Supervisor contracts, keep the corresponding capability unadvertised and block the ACP release. A protocol or schema pin change requires a new compatibility review and regression coverage; it must not silently change the frozen MVP contract.

## Expert review and adjudication

**Decision revision:** 2. Three independent reviewers checked protocol conformance, Tachyon architecture/specification, and adversarial security boundaries.

### Round 1 — 2026-09-28

- Protocol review: `BUILD`, no blockers.
- Architecture/specification review: `CONDITIONAL`. Findings A1–A3 were accepted and resolved: persist the prompt-turn/Task association and never blindly retry an ambiguous start (Decision, gateway boundary); await Supervisor drain acknowledgement rather than trusting `Cancelled` alone (Approvals and cancellation); make session mapping and message order durable in Gateway/store (Gateway and ownership boundary).
- Security-boundary review: `CONDITIONAL`. Findings S1–S4 were accepted and resolved: authorize `cwd` before binding the canonical root; mediate MCP server launch as a validated process operation; serialize cancellation against approval and invalidate late grants; keep existing Gateway Tasks alive on ACP disconnect and replay their state on reconnect.

### Round 2 — 2026-09-28

All three reviewers returned `BUILD` after the revisions. The protocol reviewer separately verified that line 28 distinguishes Agent-advertised capabilities from Client-advertised filesystem/terminal capabilities. There are no open or deferred review findings. The implementation prerequisites listed above remain release blockers; this ADR does not authorize any capability before its implementation slice and verification pass.
