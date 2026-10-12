# M28 Report: Live Failure-Path Turns — 2/2 Exact-Match Refusals

Date: 2026-10-12. Frozen plan: `docs/milestones/M28_PLAN.md`, manifest
`docs/milestones/M28_MANIFEST.json` (re-frozen once after a lint fix;
no batch ran against the stale manifest). First live measurement of
the ResourceLink fail-closed taxonomy through the full ACP boundary.

## Batch (valid, 2 rows, single arm)

Model `meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
temperature 0 (model never contacted). Fixture `duplicate-range`.
Fresh gateway + adapter + fixture copy per turn.

| turn | link | outcome | task status | end_turn | error | expected | calls | completion ms |
|---|---|---|---|---|---|---|---|---|
| s1 | sibling-escape | completed | none | false | -32602/out_of_root | out_of_root | 0 | 6 |
| s2 | oversized-in-ws | completed | none | false | -32602/oversized | oversized | 0 | 4 |

"Completed" here means exact-match verified: the expected taxonomy
marker, zero turns in session history, zero model calls (asserted
in-test, any task or call would fail the row). Usage: none expended.
Cost UNKNOWN (pricing unpinned).

## Reading (within the pre-registered rules)

- Confinement holds live: an existing outside-root file refuses
  `out_of_root` (not a vaguer marker), a 64KiB+1 in-workspace file
  refuses `oversized` — both in single-digit milliseconds with no
  model involvement, exactly as designed.
- No cross-task or cross-client generalization: two cells, one model,
  one night. Remaining taxonomy rows (unsupported_scheme,
  invalid_encoding, unsafe_content, too_many_resources) stay proven
  by unit battery only.
- Latency with host-load disclaimer (shared workstation; single-digit
  millisecond reads are harness timing, not a performance claim). No
  speed claim of any kind.

## Retained evidence

`docs/milestones/M28_SAMPLES.jsonl`, `docs/milestones/M28_META.json`.
