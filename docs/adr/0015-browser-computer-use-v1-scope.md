# 0015 — Browser/computer use: out of v1

## Status

decided · 2026-10-11 · declined for v1 (decline-by-default)

## Context

Charter non-goal: no browser automation in MVP. Unbounded outside-world
effects with no taxonomy, allow/deny list, or approval binding designed.
Highest effect risk of all deferred items.

## Decision

Browser/computer use stays out of v1. Tasks 38-40 dormant.

## Alternatives considered

1. Read-only page fetch — rejected for v1: still untrusted-input isolation
   plus network effect work during a freeze.

## Evidence

Charter non-goal; unbounded effects with no taxonomy. Highest-risk deferred item; decline-by-default.

## Consequences

No browser surface in v1.

## Invariant impact

Effect-class/idempotency and approval-binding requirements stand; a future
proposal needs the full taxonomy first.

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
