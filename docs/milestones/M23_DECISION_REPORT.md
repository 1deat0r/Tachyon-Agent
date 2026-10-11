# M23 Decision Report: ADOPT (strict improvement + mechanism match)

Date: 2026-10-12. Frozen plan: `docs/milestones/M23_PLAN.md`, manifest
`docs/milestones/M23_MANIFEST.json`, trial manifest
`docs/milestones/M23_TRIAL_MANIFEST.md` (all frozen before the first
call; `node scripts/m23_manifest.mjs` valid at batch start).

## Batch (valid, 10 rows, matched pairs)

Model `meta/muse-spark-1.3-contributor` on both arms via local OMP proxy
(keyless; proxy injects auth), temperature 0, same evidence/acceptance/
full-mode path. Sequential, fresh workspaces. 15 model calls (5 framing,
10 baseline), 0 provider failures, 0 interruptions. Usage
`provider_reported` with real token counts on every attempt.

| sample | baseline | m23-framing |
|---|---|---|
| 1 | error (2 calls, malformed x2) | completed (1 call) |
| 2 | completed (2 calls, malformed then ok) | completed (1 call) |
| 3 | error (2 calls, malformed x2) | completed (1 call) |
| 4 | error (2 calls, malformed x2) | completed (1 call) |
| 5 | completed (1 call) | completed (1 call) |

Verified success: baseline 2/5, framing 5/5. Every completed row (7/7):
`verified: true`, `observed_matches_expected: true`,
`protected_unchanged: true`, 0 verification failures. Every error row
ends `recovered_failed` (no blind retry; malformed-only second attempt
spent, then durable failure — policy unchanged).

## Mechanism match (predicted before the batch)

The plan predicted truncation at the 4096 reserve. Observed:

- All 6 failed baseline attempts report exactly 4096 output tokens with
  `invalid_json` / `empty_content` — truncated mid-JSON, same signature
  as EXIT-11.
- All 5 framing rows succeed on the FIRST attempt (1 call, 0 retries).
  Three of five framing outputs exceed the old budget (4559, 4680, 6152
  tokens) — they could not have completed before. The 8192 reserve did
  the work, not luck.
- Framing single-attempt input is 2098 vs baseline 2082 (+16 tokens):
  exactly the appended JSON-only sentence on the wire. No other prompt
  difference.
- Retry/deadline behavior unchanged: the one baseline row that needed a
  retry (s2, second attempt 3670 tokens) recovered the same way under
  both arms' shared policy.

## Verdict: ADOPT

5v2 is strict improvement on verified success under same-model
discipline, with M21 regressions 4/4, M9 re-proof 4/4, M12 seam 6/6,
VERIFY green, and FULL green on the frozen tree. The count alone on
n=5/arm would be thin; the per-attempt token evidence shows the fix
operating exactly as predicted, which is what lifts this above noise.
Production default becomes the framing fix (8192 reserve + terminal
JSON-only reminder); the flag remains as the explicit opt-out.
No retry, deadline, verification, or access-set change is adopted —
the batch confirms the policy edge (malformed-once then durable
failure) behaves identically.

## Limits (no overclaim)

One fixture (`reservation-ledger` cell 3), n=5/arm, single model, single
night, full mode only. Speed is not compared here (unequal success
forbids it). The EXIT-11 speed cell re-runs as M23b with the fix on
both arms under its own freeze.

## Retained evidence

`docs/milestones/M23_SAMPLES.jsonl`, `docs/milestones/M23_META.json`
(byte copies of the frozen batch output). Supersedes nothing: M22 DROP
and EXIT-11 waiver stand as recorded.
