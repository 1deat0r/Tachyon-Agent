# M23b Plan: Exit-11 Speed Re-run With the Framing Fix (frozen)

## Claim under test

Same as EXIT-11, now with the adopted M23 framing fix on both arms:
Tachyon full mode (parallel evidence + journaled supervisor) beats the
serial reference (same driver, serial evidence, no speculation) on
verified completion time with equal verified success — same model,
same endpoint, same task, fix on everywhere.

## Design (6 rows, matched)

- Task: `reservation-ledger` samples 1-3 (the EXIT-11 cell).
- Modes: `full` × 3, `serial` × 3, interleaved (full, serial per sample).
- Variant: `m23-framing` on every row (8192 output reserve + terminal
  JSON-only reminder; production default after M23 adoption).
- Model: `meta/muse-spark-1.3-contributor` via local OMP proxy on both
  arms. Temperature 0.
- Metrics per row: outcome (verified completed/error), wall-clock
  completion, model calls, input/output tokens (provider_reported).

## Close rule (verbatim from EXIT11_PLAN)

- PROVEN (narrow): full verified success >= serial AND full median
  completion time lower. Stated limits: one fixture, n=3/mode, no
  general speed claim.
- WAIVED otherwise: record measurements (no speed overclaim on unequal
  success — the discipline that waived EXIT-11).

## Frozen inputs

This plan, `scripts/m23b_manifest.mjs`, `scripts/m23b_run.mjs`,
`docs/milestones/M23B_MANIFEST.json`, `docs/milestones/M23_PLAN.md`,
`docs/milestones/M23_DECISION_REPORT.md`, fixtures/reservation-ledger,
`crates/tachyon-core/src/driver.rs`,
`crates/tachyon-core/src/driver/model.rs`,
`crates/tachyon-core/src/runtime.rs`,
`crates/tachyon-core/examples/bench_matrix.rs`.
No code change for this measurement (fix adopted; variant pins it on).
