# M22 Live-Trial Manifest (frozen for TASK 06 S1)

## Arms

- Baseline: `m22_deadline_proximity_record = false` (production default).
- Candidate: `m22_deadline_proximity_record = true` (observability only;
  no retry, no deadline, no prompt change in either arm).

## Rows (10 planned, matched pairs)

5 samples × 2 arms on `reservation-ledger/full/3` (retained partial-repair
cell: applies the BLAKE3-pinned partial patch, acceptance must refuse).
Arm order alternates by sample (candidate first on odd samples).
Same model, endpoint, temperature 0, evidence, acceptance, full-mode path.
Sequential, fresh workspaces. Model calls expected: 10, retries: 0.

## Adopt/reject rule (verbatim from M22_PLAN)

Adopt only on strict improvement in verified success under same-model
discipline with M21 regressions 4/4. This candidate is observability-only
by design and cannot show repair improvement; expected outcome is REJECT
(no behavior difference) with a proximity-measurement yield note.

## Rollback (task 55)

Provider outage or unknown-usage abort: retain evidence, mark trial void,
no adopt. Samples + manifest + report published regardless.
