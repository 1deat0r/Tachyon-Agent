# 0009 — Workflow compilation: out of v1

## Status

decided · 2026-10-11 · declined for v1

## Context

V1 closes M22 and the MVP exits. AD-010 defers the workflow compiler until
runtime/verification semantics are proven. No v1 exit needs compiled workflows.

## Decision

Workflow compilation stays out of v1. No plan/manifest/implement slices
activate (tasks 20-22 dormant).

## Alternatives considered

1. Admit a minimal compiler — rejected: no exit criterion needs it; adds
   unproven semantics to the freeze.
2. Admit later in v1 — rejected: same reason; re-propose after v1.

## Evidence

AD-010 defers the workflow compiler; v1 exits (E1-E12) need none of it. No measurement exists where compilation improves verified success.

## Consequences

Tasks 20-22 activate nothing and block nothing. Traces and stable IR keep
accumulating as the future input.

## Invariant impact

None (no change). A future proposal must preserve models-propose-validates
and verification-gated completion.

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
