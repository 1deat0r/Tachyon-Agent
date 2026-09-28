# Tachyon — Shared Language

Domain glossary for issues, PRs, tests, and code names. Use these terms exactly; do not drift to synonyms.

Authoritative deeper contracts: `docs/01_ARCHITECTURE_FREEZE.md`, `docs/02_IMPLEMENTATION_SPEC.md`, `AGENTS.md`.

## System shape

| Term | Meaning |
|------|---------|
| **Gateway** | Local IPC (and optional remote) boundary. CLI/TUI are MVP clients; the ACP v1 Agent is a post-MVP client target (ADR-0005). Clients talk only to the gateway; no client owns agent decisions. |
| **Task Supervisor** | Single logical writer of canonical task state for one task. Owns routing, planning, steering, recovery, completion coordination. |
| **Session** | Persistent user interaction context. Owns zero or more tasks. |
| **Task** | Executable unit of work with a durable status machine. |
| **CLI** | `tachyon` binary (`tachyon-app`). Argument parsing, config, output. No decision logic. |
| **TUI** | Ratatui client (`tachyon-tui`). Pure gateway client (AD-014): display + input only. |
| **Shared driver** | The ONE run path spawned by the gateway (`StartRun` → `drive`). CLI/TUI never spawn their own. Runs are never respawned at gateway boot without **driver re-entry**. |

## Execution and state

| Term | Meaning |
|------|---------|
| **Execution IR / ExecutionGraph / ExecutionNode** | Validated machine-readable plan before anything runs. Model tool calls are proposals until validated into IR. |
| **Access set** | Declared read/write (and related) footprint of a node. No two running nodes may hold conflicting access sets. |
| **Effect class** | Declared consequence class of an operation (e.g. pure read, local process, external keyed effect). Paired with **idempotency**. |
| **Commit barrier** | Point where irreversible/ambiguous effects become durable under policy. |
| **Durable journal** | Append-only event log; source of truth for recovery. Snapshots are materializations, not the sole truth. |
| **TaskStatus** | Canonical enum: `Created`, `Routing`, `Planning`, `Executing`, `Verifying`, `WaitingApproval`, `Paused`, `Recovering`, `Completed`, `Failed`, `Cancelled`. |
| **Recovering** | Status while a gateway/supervisor rebuilds state after restart; not a silent resume of unknown effects. Awaiting user go-ahead. |
| **Driver re-entry** | User-triggered (`Resume` on `Recovering`) respawn of the in-flight run's driver; never automatic at gateway boot. A `Recovering` task with no run to re-enter transitions to `Paused` instead. |
| **Fresh-id re-ask** | After restart, a continuation approval always issues a new approval id; the pre-restart id stays dead (expired or consumed), never reused or silently granted. |
| **Fault point** | Named hold built into production code, armed only by env (`TACHYON_FAULT_POINT`); lets a test stop a process at an exact commit seam, kill it, and assert reconcile after restart. No-op unless armed. |
| **Effect fixture** | Minimal keyed/queryable external-effect executor whose only job is proving spec §19 crash reconcile: `effects.state` `prepared` → `committed` around the remote call; recovery classifies interrupted rows. Not a general effect protocol. |
| **Workspace pin / canonical root** | One durable canonical filesystem root for a run; policy, evidence, and mutation all read that same value (no second resolution). |
| **Workspace lease** | Exclusive claim on a canonical workspace root for the life of a run (`workspace_busy` when contended). |

## Security and judgment

| Term | Meaning |
|------|---------|
| **Capability** | Explicit policy-controlled permission (path globs, process, network, credentials). Model text cannot grant capabilities. |
| **Access set** | See above; also the thing policy checks against capabilities. |
| **JudgmentProvider** | Abstraction over OpenJEV (and fakes). OpenJEV is replaceable; core works without it. |
| **Acceptance contract / verification gate** | Machine-checkable definition of done. Completion never comes from model self-report. |
| **Approval** | Human (or policy) grant for a parked operation; one-shot, durable, never silently replayed when unknown. |

## Intent and learning (ADR 0004, Phase A)

| Term | Meaning |
|------|---------|
| **IntentSpec** | Belief record of what the human wants (goal, outcome, requirements, compatibility requirements, constraints, preferences, non-goals, criteria, ambiguities, assumptions, evidence, confidence). Inferred items carry provenance and never outrank hard constraints. |
| **Intent conformance** | Advisory post-verification check (`IntentConformanceReport`) of whether the human objective was met. Only `Violated` fails conformance; never a completion gate. |
| **Correction class** | One of six: task-specific, project convention, persistent preference, model misunderstanding, missing project context, bad evidence. Only the middle four may become durable knowledge. |
| **Durable knowledge item** | A gated correction with evidence refs, confidence, contradicting observations, and revalidation/expiry policy. Task-specific notes never persist; bad evidence lowers confidence instead of creating prohibitions. |
| **Clarification ask/skip** | Policy weighing information value against interruption cost. High-confidence low-risk reads below cost skip; everything else asks a closed question through the judgment pattern. |

## Routing and cost

| Term | Meaning |
|------|---------|
| **Fast router** | Predictive cheapest-sufficient route (not serial trial of models). |
| **Jev** | Cheap judgment/scoring path behind `JudgmentProvider` (and related tools). |
| **Evidence** | Deterministic facts collected for routing/planning (repo intelligence, hashes, search), before or alongside model calls. |

## Repo / crates (shorthand)

Use crate names when the boundary matters: `tachyon-core` (supervisor/state), `tachyon-gateway` (IPC), `tachyon-ir`, `tachyon-intent`, `tachyon-store`, `tachyon-policy`, `tachyon-tools`, `tachyon-scheduler`, `tachyon-verify`, `tachyon-models`, `tachyon-judgment`, `tachyon-router`, `tachyon-tui`, `tachyon-app`.

**Dependency rule:** lower-level crates never import `tachyon-core`, gateway, or UI. Provider types do not leak into core/IR.

## Words to avoid (use the glossary term instead)

| Don't say | Say |
|-----------|-----|
| "agent loop" for the supervisor | **Task Supervisor** |
| "plan" for the validated DAG | **Execution IR** / **ExecutionGraph** |
| "permission" | **capability** |
| "done" without evidence | **verification gate passed** / **Completed** |
| "restart resume" for unknown effects | **Recovering** + reconcile (never blind replay) |
| "auto-resume on restart" | **driver re-entry** (user-triggered only) |
| "hold point" / "breakpoint" | **fault point** |
| "re-ask the approval" after restart | **fresh-id re-ask** |
| "effect protocol" for the M12 fixture | **effect fixture** (full protocol deferred) |
| "MCP call" for in-process tools | **native tool** (MCP is external boundary only) |
