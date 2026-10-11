# 0017 — Self-modifying routers: out of v1

## Status

decided · 2026-10-11 · declined for v1 (decline-by-default)

## Context

Charter non-goal: no self-modifying runtime in MVP. The router is on the
critical path of every task; letting it rewrite itself risks the
propose/validate separation. No v1 exit needs it.

## Decision

Self-modifying routers stay out of v1. Tasks 44-46 dormant.

## Alternatives considered

1. Offline router tuning from journals — rejected for v1: still a feedback
   loop into the critical path during a freeze; re-propose with separation
   proof after v1.

## Evidence

Charter non-goal (no self-modifying runtime). Router is critical path; decline-by-default.

## Consequences

Router changes remain explicit code changes with tests and gates.

## Invariant impact

Models-propose-Tachyon-validates preserved by exclusion.

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
