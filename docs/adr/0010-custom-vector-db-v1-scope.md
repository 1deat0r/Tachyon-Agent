# 0010 — Custom vector database: out of v1

## Status

decided · 2026-10-11 · declined for v1

## Context

Retrieval already works through the existing portfolio (ADR-0008). No v1
exit needs a custom vector store; M4 lexical/symbol index meets the slices.

## Decision

Custom vector DB stays out of v1. Tasks 23-25 dormant.

## Alternatives considered

1. Contained retrieval swap — rejected: unneeded for any exit; new storage
   engine risk during a freeze.
2. Admit after v1 — open: re-propose with measurements.

## Evidence

M4 index meets Slice A targets (<250 ms p50); no v1 exit cites vector search. No workload shows a custom store improving latency/cost/success.

## Consequences

No new storage backend in v1. Retrieval improvements go through the
existing surface.

## Invariant impact

None (no change).

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
