# M26 Report: Adversarial Held-Out Evaluation — 9/9 Verified

Date: 2026-10-12. Frozen plan: `docs/milestones/M26_PLAN.md`, manifest
`docs/milestones/M26_MANIFEST.json` (frozen before the first call).
Same production stack and discipline as M24.

## Batch (valid, 9 rows, single arm)

Model `meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
temperature 0, full mode, `m23-framing` variant, sequential fresh
workspaces. 9 model calls, 0 retries, 0 malformed outputs, 0 transport
errors, 0 timeouts. Usage `provider_reported` on every row.

| fixture | verified | calls (all first-attempt) | completion ms | out tokens |
|---|---|---|---|---|
| ceiling-division s1–s3 | 3/3 | 1,1,1 | 10153, 6273, 9045 | 1022, 870, 1189 |
| signed-midpoint s1–s3 | 3/3 | 1,1,1 | 10210, 10782, 11426 | 1116, 1153, 1370 |
| atomic-transfer s1–s3 | 3/3 | 1,1,1 | 6677, 11168, 8848 | 928, 1451, 1032 |

Every row: `verified: true`, `observed_matches_expected: true`,
`protected_unchanged: true`, 0 verification failures. Pooled: 9/9.
Completion median 10153 ms, range 6273–11426 ms. Tokens: 11307 in /
10131 out total. Cost UNKNOWN (pricing unpinned).

## Reading (within the pre-registered rules)

- The integer-boundary cells whose failure history motivated this
  batch (M16/M17 wrapping-overflow proposals, caught by edge oracles)
  verify first-attempt here with zero retries. No failed-patch rows
  occurred, so there is no M17-signature taxonomy to classify — the
  observation is the absence of the predicted failure mode on these
  cells, with this model and stack, on this night. It does not show
  the overflow trap is gone in general.
- Same non-claims as M24: samples repeat identical broken code (run
  variance, not task breadth); no cross-task generalization; latency
  with host-load disclaimer; no speed claim.

## Retained evidence

`docs/milestones/M26_SAMPLES.jsonl`, `docs/milestones/M26_META.json`.
