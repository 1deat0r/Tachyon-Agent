# M26 Plan: Adversarial Held-Out Evaluation (frozen)

## Purpose

Second held-out live evaluation (same discipline as M24): two fixtures
from the integer-boundary family that defeated model proposals in
M16/M17 (overflow via wrapping/saturating arithmetic, caught only by
hidden edge oracles), plus one transfer-logic cell. Either outcome is
evidence: verified success extends the M24 result to adversarial
ground; acceptance-refused rows prove the gate holds live against the
known failure mode.

## Cells (9 rows, single arm)

Full mode, `m23-framing` variant (= production default), samples 1–3
per fixture, sequential, fresh workspaces:

- `ceiling-division` × 3 (class C; `ceil_div` without overflow).
- `signed-midpoint` × 3 (class C; midpoint without overflow).
- `atomic-transfer` × 3 (class C; transfer logic).

All three pass `fixture-check` on the frozen tree. Model
`meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
temperature 0. Same metrics and taxonomy as M24.

## Reporting rules

Same as M24 (counts + taxonomy, no cross-task generalization, latency
with host-load disclaimer, cost UNKNOWN), plus: classify any failed
patch rows by whether visible tests passed while hidden/oracle checks
refused (the M17 signature) versus earlier failure (framing/transport).

## Rollback (task 55)

Standard: outage/402/unknown-usage → void, retain, publish regardless.

## Frozen inputs

This plan, `scripts/m26_manifest.mjs`, `scripts/m26_run.mjs`,
`docs/milestones/M26_MANIFEST.json`, `docs/milestones/M24_PLAN.md`,
`fixtures/{ceiling-division,signed-midpoint,atomic-transfer}` (+
solutions each), `crates/tachyon-core/src/runtime.rs`,
`crates/tachyon-core/src/driver.rs`,
`crates/tachyon-core/src/driver/model.rs`,
`crates/tachyon-core/examples/bench_matrix.rs`.
No code change for this measurement.

## Hardening

No IR, access-set, retry, deadline, or verification change. M21 replay
4/4 + VERIFY green on the frozen tree before the batch. Single arm;
model pin is the control.
