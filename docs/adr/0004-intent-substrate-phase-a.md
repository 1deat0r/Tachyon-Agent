# 0004 — Intent substrate as an extension of the verification gate

**Status:** proposed · 2026-09-27

## Context

An external design pack (`tachyon-self-improving-pack.zip`, proposal dated
2026-09-27, code seeds are stubs — design only) proposes Intent IR,
intent-conformance verification, and correction learning (its Phase A), plus
policy evolution through promotion gates (its Phases B–F). MVP is frozen at
M14 and `AGENTS.md` defers self-modifying routers until the MVP exit gate or
an approved ADR. This ADR decides Phase A only: a representation of user
intent and a conformance check, both subordinate to existing gates.

## Decision

1. **New `IntentSpec` type** (goal, desired outcome, requirements,
   compatibility requirements, constraints, preferences, non-goals,
   acceptance criteria, ambiguities, assumptions, evidence, confidence).
   All inferred requirements, compatibility requirements, and assumptions
   carry provenance (`user-stated` / `repo-derived` / `model-hypothesis`);
   nothing inferred ever outranks a hard constraint.
2. **Acceptance-criteria compilation** lowers applicable acceptance criteria
   to existing `Clause` variants (`tachyon-verify/src/contract.rs`).
   Anything not compilable becomes `Clause::Unresolved`, which **fails
   closed** per current semantics — intent coverage never silently passes.
3. **New `IntentConformanceReport`**, produced after technical verification,
   checking explicit requirements, evidence-backed inferred requirements,
   acceptance criteria, non-goal preservation, and compatibility. It is
   advisory to the operator, never a bypass around `VerificationReport`.
4. **Correction classification** (task-specific, project convention,
   persistent preference, model misunderstanding, missing project context,
   bad evidence); only the middle four classes may write durable knowledge,
   each item carrying evidence refs, confidence, and revalidation policy,
   stored via existing `tachyon-store` + `EvidenceItem` provenance shapes.
5. **Clarification** reuses the `tachyon-judgment` closed-question pattern
   with certainty policies; ask/skip follows an
   information-value-vs-interruption-cost policy with a high-confidence
   low-risk skip path.
6. **Explicit non-decision:** router weights, retrieval ranking, scheduler
   heuristics, and all policy self-modification stay deferred. No
   candidate-generation, replay, sealed-eval, or promotion machinery in
   this ADR.

## Alternatives considered

1. **Full pack at once (Phases A–F)** — rejected: violates the MVP freeze
   and the self-modifying-router deferral; unreviewably large.
2. **Intent as model prompt discipline only (no types)** — rejected:
   untestable, no provenance, repeats the "LLM as operating system" error
   the architecture freeze rules out.
3. **Conformance inside `VerificationReport`** — rejected for now: mixing
   advisory intent judgments with the completion gate risks softening a
   hard gate. Revisit after Phase A evidence.

## Evidence

- Pack: `tachyon-self-improving-pack.zip` (proposal, 2026-09-27), evaluated
  2026-09-27.
- Existing gates reused: `AcceptanceContract`/`Clause` (`tachyon-verify`),
  `JudgmentBatch` certainty policies (`tachyon-judgment`),
  `EvidenceItem`/`Provenance` (`tachyon-retrieval`), capability approvals
  (`tachyon-policy`), `RouteRecord` (`tachyon-telemetry`).
- Gap: no intent representation, no conformance report, no correction
  lifecycle, no cost tracking (blocks any future cost-per-verified-task
  objective).

## Consequences

- Three to four bounded follow-up issues (spec type → criteria compiler →
  conformance report → correction lifecycle), each a small test-first PR.
- `CONTEXT.md` gains: IntentSpec, intent conformance, correction classes.
- Cost tracking becomes a known prerequisite for anything later that
  optimizes cost.

## Migration/rollback plan

Pure addition; no existing behavior changes. All new checks default to
skip-or-fail-closed. Remove the new types and call sites to roll back; no
journal format change.
