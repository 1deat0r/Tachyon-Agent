# M25 Plan: First Live-Model ACP Prompt Turns (frozen)

## Purpose

First pinned live-model measurement through the full ACP boundary
(adapter stdio → gateway → shared driver → live model), on the
smallest fixture with an independent grader. All ACP turn evidence to
date is scripted-fake; all live evidence to date bypasses the adapter.
This is an EVALUATION: no production change, no adopt/reject — only
pre-registered reporting rules.

## Cells (3 rows, single arm)

3 sequential prompt turns, each with a fresh gateway, adapter, and
fixture copy:

- Fixture `duplicate-range` (class C; `cargo test` runs visible +
  holdout suites, so the default acceptance detection is a meaningful
  independent grader; holdout is runnable-but-protected).
- Prompt (frozen verbatim): `Fix equal_range so it returns the complete
  half-open range of entries equal to the query in a sorted slice. For
  absent values return the empty range at the insertion point. Preserve
  the public API and other functions. The defect is in
  subject/src/implementation.rs.`
- Model `meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
  temperature 0 (harness wire default).

Recorded per row: adapter `end_turn` presence, task status via gateway
`GetTask` (`Completed` ⟺ verified, the completion-gate invariant),
model calls, input/output tokens (`provider_reported`), wall ms.
`protected_unchanged` is enforced by the verification gate itself
(`ChangedPathsWithin` + `FileUnchanged` clauses); any protected write
fails the row as acceptance-refused.

## Reporting rules

- Report per-row outcomes + pooled verified count; no cross-task or
  cross-client generalization (one fixture, one model, one night).
- Chunks/response-shape observations are recorded, not graded: protocol
  conformance of live text is evidence, not a gate.
- Latency with host-load disclaimer; cost UNKNOWN (pricing unpinned).
- A turn that never reaches `end_turn` is reported as `error`
  (adapter-turn-failure), never retried blindly.

## Rollback (task 55)

Proxy outage, 402s, or adapter-harness failure: retain evidence, mark
void, no conclusions. Rows + manifest + report published regardless.

## Frozen inputs

This plan, `crates/tachyon-acp/tests/session_prompt_live_model.rs`,
`scripts/m25_manifest.mjs`, `scripts/m25_run.mjs`,
`docs/milestones/M25_MANIFEST.json`, `fixtures/duplicate-range` (+
`fixtures/solutions/duplicate-range` for the self-check reference),
`crates/tachyon-acp/src/server.rs`, `crates/tachyon-acp/src/turn.rs`,
`crates/tachyon-acp/tests/common/mod.rs`.
No production change for this measurement.

## Hardening

New test file only (`#[ignore]`, env-gated, never runs in
`cargo verify`); VERIFY green required (compile + clippy). The live run
goes through `cargo test --release -- --ignored`, never the default
suite.
