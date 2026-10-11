# M24 Plan: Pinned Live-Model Evaluation on Held-Out Fixtures (frozen)

## Purpose

First pinned live-model evaluation of the adopted production stack
(M23 framing fix default-on) on fixtures with zero prior live coverage.
All live measurement to date is `reservation-ledger` only. This is an
EVALUATION, not a candidate trial: no production change is proposed, so
there is no adopt/reject rule — only pre-registered reporting rules.

## Cells (9 rows, single arm)

Full mode, `m23-framing` variant (= production default, pinned
explicitly), samples 1–3 per fixture, sequential, fresh workspaces:

- `duplicate-range` × 3 (class C, single-file integer boundary;
  M16 overflow history, independent holdout oracle).
- `multi-file-migration` × 3 (class D, cross-file tax rounding;
  M19 API-preservation history).
- `auth-refresh` × 3 (class C, refresh-race debugging flavor).

All three pass `fixture-check` on the frozen tree (broken-first fails,
solutions pass, protected paths intact).

Model: `meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
temperature 0. Metrics per row: verified outcome, model calls/retries,
input/output tokens (`provider_reported`), wall/model ms, failure
taxonomy (`malformed_output` / acceptance-refused patch / transport /
timeout / other), `protected_unchanged`.

## Reporting rules (stated here so the report cannot reinterpret)

- Report per-fixture verified counts plus the pooled rate; no
  cross-task generalization beyond these cells.
- Failure taxonomy counts every failed attempt's error class; retry
  behavior reported as-observed under the shared malformed-only policy.
- Latency as median + range with a host-load disclaimer (shared
  workstation, not a controlled host); no speed claim of any kind.
- Cost is UNKNOWN: provider pricing is unpinned, so only token counts
  are reported, never dollars.

## Rollback (task 55)

Provider outage, 402s, or unknown-usage abort: retain evidence, mark
the batch void, no conclusions. Samples + manifest + report published
regardless.

## Frozen inputs

This plan, `scripts/m24_manifest.mjs`, `scripts/m24_run.mjs`,
`docs/milestones/M24_MANIFEST.json`, `docs/milestones/M23_PLAN.md`,
`docs/milestones/M23_DECISION_REPORT.md`,
`fixtures/{duplicate-range,multi-file-migration,auth-refresh}` (+
`fixtures/solutions` for each), `crates/tachyon-core/src/runtime.rs`,
`crates/tachyon-core/src/driver.rs`,
`crates/tachyon-core/src/driver/model.rs`,
`crates/tachyon-core/examples/bench_matrix.rs`.
No code change for this measurement (production default already
adopted; variant pins it explicitly).

## Hardening (E13-E18)

No IR, access-set, retry, deadline, or verification change. M21 replay
4/4 + VERIFY green required on the frozen tree before the batch.
Single-arm evaluation: same-model discipline is vacuous (no arms to
mix); the model pin above is the control.
