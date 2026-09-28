# AGENTS.md — Tachyon Implementation Rules

This repository implements Tachyon. These instructions apply to all coding agents working in the repository.

## Architecture and context

Architecture-level requirements are defined by `docs/01_ARCHITECTURE_FREEZE.md`, the implementation contract by `docs/02_IMPLEMENTATION_SPEC.md`, and milestone dependencies by `docs/04_IMPLEMENTATION_PLAN.md`. Consult the relevant sections and ADRs for the change; do not silently reinterpret architecture-level requirements or read unrelated documents by default.

Do not silently reinterpret architecture-level requirements.

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
- Every scheduled operation is represented by validated Execution IR.
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

Follow [`docs/DEVELOPMENT_WORKFLOW.md`](docs/DEVELOPMENT_WORKFLOW.md) for the default local loop, verification tiers, commits, and when GitHub tracking, PRs, and CI add value. Run `cargo verify` before committing. Issues, branches, and PRs are optional for routine work. Skills may provide planning, implementation, TDD, review, and debugging techniques, but their default Issue, branch, PR, hook, or CI steps do not override this repository policy.

### Optional GitHub tracking

The canonical remote is `1deat0r/Tachyon-Agent`. Use GitHub Issues and Wayfinder maps only when persistent shared tracking materially helps; see [`docs/agents/issue-tracker.md`](docs/agents/issue-tracker.md).

### Triage labels

Canonical roles: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`; plus `type/*`, `comp/*`, `P0`–`P3`, `needs-repro`, and `wayfinder:*`. See `docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repo: consult root `CONTEXT.md` and relevant ADRs when domain language or decisions are involved. Architecture references remain authoritative where applicable. See [`docs/agents/domain.md`](docs/agents/domain.md).

## Runtime behavior

Bare `tachyon` opens the TUI only against a running gateway. Do not silently auto-start the runtime; lifecycle changes need an explicit design decision.
