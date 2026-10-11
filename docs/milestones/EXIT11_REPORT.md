# Exit-11 Report: WAIVER with Evidence (TASK 08 S2)

Date: 2026-10-11. Frozen plan: `docs/milestones/EXIT11_PLAN.md`, manifest
`docs/milestones/EXIT11_MANIFEST.json` (frozen before first call).

## Measurement (same model both arms)

`meta/muse-spark-1.3-contributor`, reservation-ledger samples 1-3,
full vs serial, 11 model calls, bounded framing re-attempts on 5/6 rows
(second attempt after `malformed_output` only; no blind repair retry),
provider_reported usage throughout.

- Full: 0/3 completed (all `malformed_output`: invalid JSON / empty content).
- Serial: 1/3 completed (same framing failure mode on the other two).

## Outcome: WAIVE (not proven)

Verified success is unequal (0 vs 1) and low on both arms, so no speed
comparison is valid — a speed claim on unequal success would be exactly
the cross-model-style overclaim the discipline forbids. Failure mode is
output framing (reasoning model exhausts the 4096-token output budget /
emits non-JSON), not verification or scheduling. The architecture speed
claim for this cell is therefore unproven, not disproven.

## Waiver request (E17: needs 4/5 board + expiry)

Waive the 11th exit for v1.0.0 with expiry: revisit after a framing fix
(prompt/budget engineering, its own frozen candidate) re-runs this cell.
Board vote `ses_ed57ddb66ffeTQsE3oF9lOCIyN`: 4/5 APPROVE (verification
blocked only on the since-corrected call counts), waiver GRANTED.
Limits: one fixture, n=3/mode, single model, single night.

## Retained evidence

`docs/milestones/EXIT11_SAMPLES.jsonl`, `docs/milestones/EXIT11_META.json`.
