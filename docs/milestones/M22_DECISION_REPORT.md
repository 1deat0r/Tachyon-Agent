# M22 Decision Report: REJECT (no strict improvement)

Date: 2026-10-11. Frozen manifest: `docs/milestones/M22_MANIFEST.json`
(post-Spark-switch refreeze, before first Spark call). Trial manifest:
`docs/milestones/M22_TRIAL_MANIFEST.md`. Adopt/reject rule applied verbatim
from `docs/milestones/M22_PLAN.md`.

## Batch (valid, 10 rows, alternating arms)

Model `meta/muse-spark-1.3-contributor` on both arms via local OMP proxy,
temperature 0, same evidence/acceptance/full-mode path. 17 model calls,
0 retries, 0 provider failures. Usage `provider_reported` with real token
counts on every attempt (2082 in; ~3700-4096 out).

- Baseline: 2/5 completed, 3/5 error.
- m22-proximity (flag on): 3/5 completed, 2/5 error.

## Verdict: REJECT

3v2 on n=5 is a one-sample difference — noise, not strict improvement.
The candidate is observability-only by design and cannot show repair
improvement; the batch confirms no behavior difference that matters.
Production behavior unchanged: flag stays default-off.

## Notes for future candidates (not selected)

- Dominant failure mode is output framing: first attempts often return
  empty content or invalid JSON (`malformed_output`), second attempts
  usually succeed. A prompt-framing candidate could target this, with its
  own frozen plan and adopt/reject rule.
- Error rows end on `interrupted` second attempts: the harness spends its
  malformed-output budget and stops. No blind retry observed.

## Retained evidence

- `docs/milestones/M22_SPARK_SAMPLES.jsonl`, `docs/milestones/M22_SPARK_META.json`.
- Supersedes the VOID mimo batch (`M22_TRIAL_REPORT.md`), which is kept
  for the record. M21 regressions stay 4/4; `cargo verify` green on tree.
