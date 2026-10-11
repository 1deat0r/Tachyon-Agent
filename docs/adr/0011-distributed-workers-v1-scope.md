# 0011 — Distributed workers: out of v1

## Status

decided · 2026-10-11 · declined for v1

## Context

Charter non-goal: no distributed execution cluster in MVP. Single-writer
and conflict-free access sets are proven locally; distribution would reopen
both. No v1 exit needs remote workers.

## Decision

Distributed workers stay out of v1. Tasks 26-28 dormant.

## Alternatives considered

1. Read-only remote evidence — rejected: still a new trust/topology boundary
   during a freeze.
2. Admit after v1 — open with single-writer proof required.

## Evidence

Charter non-goal (no distributed cluster); M12 recovery proven locally. No v1 exit needs remote workers.

## Consequences

One local gateway remains the only writer.

## Invariant impact

Single-writer and no-conflicting-access-sets preserved by exclusion.

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
