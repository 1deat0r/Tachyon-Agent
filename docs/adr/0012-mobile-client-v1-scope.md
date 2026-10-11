# 0012 — Mobile client: out of v1

## Status

decided · 2026-10-11 · declined for v1

## Context

Distribution wedge for v1 is ACP compat (task 50), already a gateway client
per AD-014. A mobile client is a second new surface with no exit needing it.

## Decision

Mobile client stays out of v1. Tasks 29-31 dormant.

## Alternatives considered

1. Thin mobile shell on gateway API — rejected for v1: review/support cost
   during freeze; re-propose after v1 with the ACP precedent.

## Evidence

AD-014 makes clients cheap, but v1 distribution wedge is ACP (task 50). No exit needs a mobile surface.

## Consequences

v1 clients: CLI, TUI, ACP. No agent decision logic leaves the gateway.

## Invariant impact

None (no change); any future client must carry no decision logic (AD-014).

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
