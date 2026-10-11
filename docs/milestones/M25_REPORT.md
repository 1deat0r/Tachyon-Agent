# M25 Report: First Live-Model ACP Prompt Turns — 3/3 Verified

Date: 2026-10-12. Frozen plan: `docs/milestones/M25_PLAN.md`, manifest
`docs/milestones/M25_MANIFEST.json` (re-frozen after a harness fix;
see voided attempt below). Live model through the full ACP boundary:
adapter stdio → gateway → shared driver → provider.

## Batch (valid, 3 rows, single arm)

Model `meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
temperature 0 (harness wire default). Fixture `duplicate-range`,
frozen prompt, fresh gateway + adapter + fixture copy per turn.
3 model calls, 0 retries, 0 malformed outputs, 0 transport errors.
Usage `provider_reported` on every attempt.

| turn | outcome | task status | end_turn | clean exit | calls | completion ms | out tokens |
|---|---|---|---|---|---|---|---|
| s1 | completed | Completed | true | true | 1 | 6317 | 739 |
| s2 | completed | Completed | true | true | 1 | 8069 | 952 |
| s3 | completed | Completed | true | true | 1 | 4682 | 559 |

Every row `verified: true` (= gateway `Completed`, which only the
verification gate can journal). The gate ran the fixture's own
`cargo test` (visible + holdout suites) under default acceptance
detection — broken-first fails per `fixture-check`, so completion is
non-vacuous. Input 2921 tokens/turn (ACP conversation framing exceeds
the gateway-direct 1329 for the same fixture; observed, not graded).
Cost UNKNOWN (pricing unpinned).

## Reading (within the pre-registered rules)

- First live evidence through the adapter: prompt → chunks →
  `end_turn` → verified completion, clean adapter exit, all three
  turns first-attempt. The ACP boundary imposes no framing penalty
  on this cell.
- No cross-task or cross-client generalization: one fixture, one
  model, one night, three turns. Latency with host-load disclaimer;
  no speed claim.

## Voided attempt (harness fault, zero live spend)

The first launch failed in 0.01 s before any model call: the test's
prompt line embedded unquoted JSON (`json!(PROMPT).as_str()`), so the
adapter answered `Parse error`. Fixed to `serde_json::to_string`,
manifest re-frozen, recommitted (`da2799d`), batch re-run at a fresh
output path. The voided zero-row files remain under ignored
`target/m25/` (not evidence). No live call was spent and no conclusion
was drawn from the void.

## Retained evidence

`docs/milestones/M25_SAMPLES.jsonl`, `docs/milestones/M25_META.json`.
