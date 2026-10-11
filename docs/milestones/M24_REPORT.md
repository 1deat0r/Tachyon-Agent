# M24 Report: Held-Out Fixture Live Evaluation — 9/9 Verified

Date: 2026-10-12. Frozen plan: `docs/milestones/M24_PLAN.md`, manifest
`docs/milestones/M24_MANIFEST.json` (frozen before the first call).
Production stack with the adopted M23 framing fix; no code change for
this measurement.

## Batch (valid, 9 rows, single arm)

Model `meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
temperature 0, full mode, `m23-framing` variant, sequential fresh
workspaces. 9 model calls, 0 retries, 0 malformed outputs, 0 transport
errors, 0 timeouts. Usage `provider_reported` on every row.

| fixture | verified | calls (all first-attempt) | completion ms | out tokens |
|---|---|---|---|---|
| duplicate-range s1–s3 | 3/3 | 1,1,1 | 5385, 6741, 6058 | 970, 1042, 933 |
| multi-file-migration s1–s3 | 3/3 | 1,1,1 | 17306, 26616, 22897 | 2128, 3289, 2641 |
| auth-refresh s1–s3 | 3/3 | 1,1,1 | 9660, 11941, 10622 | 1509, 1792, 1389 |

Every row: `verified: true`, `observed_matches_expected: true`,
`protected_unchanged: true`, 0 verification failures. Pooled: 9/9
verified. Completion median 10622 ms, range 5385–26616 ms;
`model_ms` ≈ `completion_ms` on every row (model-latency dominated).
Tokens: 13662 in / 15693 out total. Cost UNKNOWN (pricing unpinned).

## Reading (within the pre-registered rules)

- First live coverage beyond `reservation-ledger`: a class-C
  single-file boundary fix, a class-D cross-file migration, and a
  debugging-flavored refresh race all verify first-attempt under the
  adopted stack.
- Do NOT over-attribute to the budget raise: the largest output here
  (3289 tokens) fits the old 4096 reserve. What this batch shows is a
  clean framing run (zero malformed outputs in 9 calls), consistent
  with — but not isolating — the M23 mechanism claim.
- Samples within a fixture repeat identical broken code, so they
  measure run variance, not task breadth. No cross-task generalization
  is made: the claim is exactly these three cells on this night.
- Latency carries a host-load disclaimer (shared workstation, load
  13–40 during the day); no speed claim of any kind is made.

## Retained evidence

`docs/milestones/M24_SAMPLES.jsonl`, `docs/milestones/M24_META.json`.
