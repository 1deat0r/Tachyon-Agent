# Exit-11 Plan: Same-Model Speed Measurement (frozen for TASK 08)

## Claim under test

Tachyon full mode (parallel evidence + journaled supervisor) beats the
serial reference (same driver, serial evidence, no speculation) on
verified completion time with equal verified success — same model,
same endpoint, same task.

## Design (6 rows, matched)

- Task: `reservation-ledger` samples 1-3 (known cells with valid Spark
  behavior from the M22 batch).
- Modes: `full` × 3, `serial` × 3, interleaved (full, serial per sample).
- Model: `meta/muse-spark-1.3-contributor` via local OMP proxy on both
  arms. Temperature 0. Baseline proposal variant (no guidance).
- Metrics per row: outcome (verified completed/error), wall-clock
  completion, model calls, input/output tokens (provider_reported).

## Close rule

- PROVEN (narrow): full verified success >= serial AND full median
  completion time lower. Stated limits: one fixture, n=3/mode, no
  general speed claim.
- WAIVED otherwise: record measurements + formal waiver with 4/5 board.

## Frozen inputs

This plan, `scripts/exit11_manifest.mjs`, `scripts/exit11_run.mjs`,
`docs/milestones/EXIT11_MANIFEST.json`, fixtures/reservation-ledger,
`crates/tachyon-core/src/driver.rs`, `crates/tachyon-core/examples/bench_matrix.rs`.
No code change for this measurement (modes already exist).
