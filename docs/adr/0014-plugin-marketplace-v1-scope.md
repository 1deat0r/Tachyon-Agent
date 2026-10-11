# 0014 — Plugin marketplace: out of v1

## Status

decided · 2026-10-11 · declined for v1

## Context

Charter non-goal: no marketplace in MVP. Third-party code is untrusted by
default; sandboxing, signing, and grant-scope design do not exist yet. No
v1 exit needs plugins.

## Decision

Plugin marketplace stays out of v1. Tasks 35-37 dormant.

## Alternatives considered

1. Curated allowlist — rejected: still untrusted-code containment work with
   the security seat decisive, during a freeze.

## Evidence

Charter non-goal (no marketplace); no sandboxing/signing design exists. Security seat decisive: no exit justifies the surface.

## Consequences

No third-party code execution surface in v1.

## Invariant impact

Untrusted-text-is-data and effect boundaries preserved by exclusion.

## Migration/rollback plan

Decline needs no migration. Re-proposal after v1 re-enters through a new ADR with measurements and invariant proofs.

## Verdict

DECLINE for v1 with rationale above.
