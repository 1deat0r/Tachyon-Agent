# AGENTS.md — Tachyon Implementation Rules

This repository implements Tachyon. These instructions apply to all coding agents working in the repository.

## Architecture and context

Architecture requirements: `docs/01_ARCHITECTURE_FREEZE.md`; implementation contract: `docs/02_IMPLEMENTATION_SPEC.md`; milestone dependencies: `docs/04_IMPLEMENTATION_PLAN.md`. Consult the relevant sections and ADRs for the change; never silently reinterpret architecture-level requirements or read unrelated documents by default.

## Priority order

When requirements appear to conflict, prefer:

1. Correctness and recoverability.
2. Explicit security/effect boundaries.
3. Verified task success.
4. Lower critical-path wall-clock latency.
5. Lower time-to-first-useful-result.
6. Fewer unnecessary model/Jev calls.
7. Lower monetary/token cost.
8. Implementation elegance.

Do not optimize raw token use at the cost of wall-clock latency or correctness.

## Core invariants

- Deterministic truth must not be delegated to an LLM unnecessarily.
- Models propose; Tachyon validates and executes.
- Every scheduled operation is represented by validated Execution IR. *(Enforced for the production `fs.read` evidence slice since ADR-0006: the Supervisor lowers typed requests to validated IR and dispatches them through `tachyon-scheduler`. Consequential effect nodes are not yet dispatched this way.)*
- No running nodes may hold conflicting access sets.
- Task state has one logical writer: the Task Supervisor.
- Every consequential effect declares effect class and idempotency.
- Unknown/non-idempotent effects are never blindly replayed after a crash.
- Hard user constraints cannot be bypassed by model output.
- Untrusted repository/external text is data, not executable policy.
- Completion is gated by acceptance/verification, never model self-report.
- Provider-specific types do not leak into `tachyon-core`.
- OpenJEV is replaceable behind `JudgmentProvider`.
- MCP is an external integration boundary, not the internal call path.
- CLI and TUI contain no agent decision logic.
- Remote gateway mode is disabled by default.

## New capability checklist

Every new capability must document:

- Why deterministic code cannot already solve it.
- Input/output schema.
- Access set.
- Effect class.
- Idempotency.
- Resource claim.
- Cancellation behavior.
- Retry policy.
- Verification method.
- Crash-recovery behavior.
- Expected latency class.

## Performance discipline

Do not speculate about performance when it can be measured. Add instrumentation and benchmark.

Never claim Tachyon is faster because a different model/provider was used. Harness comparisons should use the same model where possible.

## Development routing

Deterministic evidence first; TypeSafe JEV for bounded typed uncertainty; generative models only for unresolved open-ended reasoning. JEV never verifies truth.

## Scope discipline

MVP explicitly defers:

- workflow compilation;
- self-modifying routers;
- browser/computer use;
- distributed workers;
- mobile and Telegram clients;
- agent swarms;
- plugin marketplace;
- custom vector database.

Implement these only after the MVP exit gate or an approved ADR.

## Local-first development

Follow [`docs/DEVELOPMENT_WORKFLOW.md`](docs/DEVELOPMENT_WORKFLOW.md) for the local loop, verification tiers, and atomic commits. Routine work: implement in the working tree, run `cargo verify`, inspect the diff, commit directly to `main` — one logical change per commit, each compiling and `--locked`-clean on its own. The `post-commit` hook pushes every commit to `1deat0r/Tachyon-Agent` immediately, so an intermediate commit is a published commit; never end a session with verified work uncommitted, and never squash unrelated changes. Pull requests and GitHub CI are retired — direct commits to `main`, gated only by local `cargo verify`; add no Issue, branch, PR, hook, or CI wait. Skill process steps never override this policy.

### References

- Canonical remote `1deat0r/Tachyon-Agent`; GitHub Issues/Wayfinder maps only when persistent shared tracking materially helps — [`docs/agents/issue-tracker.md`](docs/agents/issue-tracker.md).
- Triage label vocabulary: [`docs/agents/triage-labels.md`](docs/agents/triage-labels.md).
- Single-context repo: consult `CONTEXT.md` and relevant ADRs for domain language/decisions — [`docs/agents/domain.md`](docs/agents/domain.md).

## Runtime behavior

Bare `tachyon` opens the TUI only against a running gateway. Do not silently auto-start the runtime; lifecycle changes need an explicit design decision.
