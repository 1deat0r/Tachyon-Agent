# M23 Live-Trial Manifest (frozen for the framing trial)

## Arms

- Baseline: `m23_framing_fix = false` (production default: 4096 budget,
  prompt byte-identical).
- Candidate: `m23_framing_fix = true` (8192 budget + one terminal JSON-only
  sentence; no retry, deadline, verification, or access-set change in
  either arm).

## Rows (10 planned, matched pairs)

5 samples × 2 arms on `reservation-ledger`/full samples 1-5 (includes the
retained partial-repair cell sample 3: BLAKE3-pinned partial patch,
acceptance must refuse).
Arm order alternates by sample (candidate first on odd samples).
Same model, endpoint, temperature 0, evidence, acceptance, full-mode path.
Sequential, fresh workspaces. Model calls expected: ~10-20 (one
malformed-only re-attempt allowed per row, same policy both arms).
Model: `meta/muse-spark-1.3-contributor` via the local OMP proxy
(`http://127.0.0.1:14000`, keyless — the proxy injects auth; the harness
sends no Authorization header). Same model on both arms (discipline
preserved).

## Adopt/reject rule (verbatim from M23_PLAN)

Adopt only on strict improvement in verified success under same-model
discipline with M21 regressions 4/4. On adopt, the EXIT-11 6-row speed
cell re-runs as M23b under the same freeze discipline; no speed claim is
valid on unequal verified success.

## Rollback (task 55)

Provider outage or unknown-usage abort: retain evidence, mark trial void,
no adopt. Samples + manifest + report published regardless.
