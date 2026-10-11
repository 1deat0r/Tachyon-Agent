# M22 Live-Trial Report: VOID (infra failure, no adopt)

Date: 2026-10-11. Frozen manifest: `docs/milestones/M22_MANIFEST.json`.
Trial manifest: `docs/milestones/M22_TRIAL_MANIFEST.md` (10 matched rows,
alternating arms, same model/endpoint/evidence/acceptance).

## Outcome

VOID. All 10 rows returned provider `invalid_request` on the first attempt,
both arms equally (5 baseline + 5 m22-proximity, 10 model calls, 0 retries).
Direct probe of the same endpoint/model/key returned HTTP 402
`insufficient_balance`. Zero valid repair evidence in either arm.

## Rule applied (verbatim from M22_PLAN + task 55)

Provider outage abort: retain evidence, mark trial void, no adopt.
Adoption requires strict improvement; a void batch cannot adopt.
Production behavior unchanged (flag remains default-off).

## Retained evidence

- `docs/milestones/M22_SAMPLES.jsonl` (10 rows, no key material).
- `docs/milestones/M22_META.json` (source commit, binary + manifest hashes).
- `target/m22/raw.state.json` (batch accounting, complete=true with 10
  infra-error samples).

## Next step

Top up the provider account, then re-run `node scripts/m22_run.mjs`
with a fresh output path (runner refuses to overwrite). No code, plan, or
manifest change needed for the re-run. G1 enable-or-drop stays pending
until a valid batch exists.
