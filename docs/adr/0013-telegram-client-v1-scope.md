# 0013 — Telegram client: out of v1

## Status

decided · 2026-10-11 · declined for v1

## Context

Same as mobile: ACP is the v1 distribution wedge. A Telegram client adds a
third-party network dependency and credential surface with no exit needing it.

## Decision

Telegram client stays out of v1. Tasks 32-34 dormant.

## Alternatives considered

1. Bot as gateway client — rejected for v1: network/secret handling scope
   during freeze; re-propose after v1.

## Evidence

Same as mobile, plus third-party network + secret scope. No exit needs it.

## Consequences

No Telegram surface in v1.

## Invariant impact

None (no change); future client must be a gateway client with no decision logic.

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
