# M22 Live-Trial Manifest (frozen for TASK 06 S1)

## Arms

- Baseline: `m22_deadline_proximity_record = false` (production default).
- Candidate: `m22_deadline_proximity_record = true` (observability only;
  no retry, no deadline, no prompt change in either arm).

## Rows (10 planned, matched pairs)

5 samples × 2 arms on `reservation-ledger`/full samples 1-5 (includes the
retained partial-repair cell sample 3: BLAKE3-pinned partial patch,
acceptance must refuse).
Arm order alternates by sample (candidate first on odd samples).
Same model, endpoint, temperature 0, evidence, acceptance, full-mode path.
Sequential, fresh workspaces. Model calls expected: 10, retries: 0.
Model: `meta/muse-spark-1.3-contributor` via the local OMP proxy
(`http://127.0.0.1:14000`, keyless — the proxy injects auth; the harness
sends no Authorization header). Same model on both arms (discipline
preserved). Switched from mimo-v2.6-flash on 2026-10-11 after the first
batch went VOID on provider 402 insufficient balance; no valid samples
existed under the old model, so nothing is mixed.

## Adopt/reject rule (verbatim from M22_PLAN)

Adopt only on strict improvement in verified success under same-model
discipline with M21 regressions 4/4. This candidate is observability-only
by design and cannot show repair improvement; expected outcome is REJECT
(no behavior difference) with a proximity-measurement yield note.

## Rollback (task 55)

Provider outage or unknown-usage abort: retain evidence, mark trial void,
no adopt. Samples + manifest + report published regardless.
