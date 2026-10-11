# 0016 — Agent swarms: out of v1

## Status

decided · 2026-10-11 · declined for v1 (decline-by-default)

## Context

AD-011: no multi-agent default; native concurrency covers parallel work.
Charter non-goal (no swarm framework). One logical writer per task would
need proof across every agent. No v1 exit needs swarms.

## Decision

Agent swarms stay out of v1. Tasks 41-43 dormant.

## Alternatives considered

1. Two-agent pilot — rejected: single-writer proof + failure isolation
   design during a freeze, for no exit.

## Evidence

AD-011 (no multi-agent default); charter non-goal. No exit needs swarms; decline-by-default.

## Consequences

One Task Supervisor per task remains the only writer.

## Invariant impact

Single-writer and models-propose-validates preserved by exclusion.

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
