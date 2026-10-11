# M22 Candidate Plan (frozen for TASK 04)

## Candidate

Deadline-proximity observability only. No prompt, retry, or deadline change.

- New field `RuntimeBounds::m22_deadline_proximity_record: bool`, default `false`.
- When `false` (default): zero behavior change. No extra call, no retry,
  no deadline change, no new mutation, no prompt change.
- When `true` (opt-in only, future trials): the driver records a
  deadline-proximity marker (elapsed vs `model_deadline_ms`) in the attempt
  record. Still no retry and no deadline change.

## Why this candidate

M21 retained two model-stage deadline failures and one transport error.
Before changing any retry or deadline value, measure proximity: how close
attempts come to the deadline. This candidate adds only the measurement
hook, default off, so M21 regressions must stay green.

## Flag

`m22_deadline_proximity_record`, disabled by default (`Default::default()`
yields `false`). Proven by `m22_flag_default_off` test.

## Frozen inputs

M21 regressions (`m21_replay` 4/4), `docs/milestones/M21_MANIFEST.json`,
`fixtures/reservation-ledger`, this plan, `scripts/m22_manifest.mjs`,
`crates/tachyon-core/src/runtime.rs`, `crates/tachyon-core/src/driver.rs`,
`crates/tachyon-core/src/driver/model.rs`.

## Adopt/reject rule (for TASK 06, stated here so trials cannot reinterpret)

Adopt only if frozen live trials show strict improvement on verified success
with same-model harness discipline and M21 regressions stay 4/4.
Otherwise reject. Rule applied verbatim in the TASK 06 report.

## Hardening (E13-E18)

Validated-IR lowering unchanged; access sets unchanged; one-logical-writer
unchanged; M9/M12 re-proof required in TASK 05; no blind retry added;
same-model MUST for trials; implement S3 = FULL tier.
