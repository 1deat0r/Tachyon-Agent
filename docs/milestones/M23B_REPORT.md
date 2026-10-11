# M23b Report: Exit-11 Re-run — WAIVED (measurable, speed not proven)

Date: 2026-10-12. Frozen plan: `docs/milestones/M23B_PLAN.md`, manifest
`docs/milestones/M23B_MANIFEST.json` (frozen before the first call).
Variant `m23-framing` on all rows (adopted production default);
binary rebuilt from the adoption commit before freezing.

## Measurement (same model both arms)

`meta/muse-spark-1.3-contributor` via local OMP proxy, temperature 0,
reservation-ledger samples 1-3, full × 3 + serial × 3 interleaved.
6 model calls, 0 retries, 0 malformed outputs, 0 provider failures.
Usage `provider_reported` throughout.

| row | outcome | calls | completion_ms | model_ms | out_tokens |
|---|---|---|---|---|---|
| full/s1 | completed | 1 | 30116 | 29611 | 3910 |
| serial/s1 | completed | 1 | 31458 | 30955 | 4008 |
| full/s2 | completed | 1 | 40000 | 39474 | 4393 |
| serial/s2 | completed | 1 | 50452 | 49919 | 5141 |
| full/s3 | completed | 1 | 63206 | 62692 | 6116 |
| serial/s3 | completed | 1 | 35945 | 35414 | 4249 |

All rows `verified: true`, `observed_matches_expected: true`,
`protected_unchanged: true`. Medians: full 40000 ms, serial 35945 ms.

## Outcome: WAIVED (close rule verbatim from EXIT11_PLAN)

Full success (3/3) >= serial (3/3) holds, but full median completion is
NOT lower (40000 > 35945). The narrow speed claim is therefore unproven,
not disproven — same discipline that waived EXIT-11. No speed overclaim
is made.

## What the re-run did establish

- The cell EXIT-11 could not measure (0/3 vs 1/3, all framing failures)
  is now 3/3 vs 3/3 with zero malformed outputs. The framing fix
  repaired measurability itself.
- Three of six outputs (4393, 5141, 6116 tokens) exceed the old 4096
  reserve — independent confirmation the budget raise was necessary.
- `model_ms` ≈ `completion_ms` on every row: wall clock is model-latency
  dominated, and per-row model variance (30 s–63 s) swamps any harness
  evidence-concurrency delta at n=3. A speed claim would need a larger
  frozen cell designed for it, not a re-interpretation of this one.

## Waiver expiry

The EXIT-11 waiver (granted with expiry: revisit after a framing fix
re-runs this cell) is satisfied by this re-run. The architecture speed
claim for this cell remains unproven; it is not re-waived here — it
simply stays unclaimed until a larger frozen measurement says otherwise.

## Retained evidence

`docs/milestones/M23B_SAMPLES.jsonl`, `docs/milestones/M23B_META.json`.
