# M27 Report: Live ResourceLink Turns — 3/3 Verified

Date: 2026-10-12. Frozen plan: `docs/milestones/M27_PLAN.md`, manifest
`docs/milestones/M27_MANIFEST.json` (frozen before the first call).
First live measurement of the ResourceLink resolve path (ADR-0018,
`resolve_resource_link` + prompt wiring) through the full ACP boundary.

## Batch (valid, 3 rows, single arm)

Model `meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
temperature 0. Fixture `duplicate-range`, frozen text objective PLUS one
`resource_link` block per turn pointing at `subject/tests/visible.rs`
inside the pinned workspace copy. Fresh gateway + adapter + fixture copy
per turn. 3 model calls, 0 retries, 0 malformed outputs, 0 transport
errors. Usage `provider_reported` on every attempt.

| turn | outcome | task status | end_turn | clean exit | calls | completion ms | out tokens |
|---|---|---|---|---|---|---|---|
| s1 | completed | Completed | true | true | 1 | 6324 | 951 |
| s2 | completed | Completed | true | true | 1 | 3896 | 555 |
| s3 | completed | Completed | true | true | 1 | 5513 | 960 |

Every row `verified: true` (= gateway `Completed`, journalled only by
the verification gate running the fixture's own `cargo test` with the
holdout suite). Input 2999 tokens/turn vs 2921 for the M25 text-only
turns (+78: the link block framing + 143-byte file — the resolve path
is visibly in the context). Cost UNKNOWN (pricing unpinned).

## Reading (within the pre-registered rules)

- The resolve+embed path works live end to end: link accepted at
  parse, file resolved against the pinned root, fenced block embedded,
  turn verified — three times, all first-attempt, zero taxonomy rows.
- No cross-task or cross-client generalization: one fixture, one link
  target, one model, one night. The confinement gates (escapes,
  oversize, delimiters) remain proven by unit battery only — this batch
  exercised the happy path.
- Latency with host-load disclaimer; no speed claim of any kind.

## Retained evidence

`docs/milestones/M27_SAMPLES.jsonl`, `docs/milestones/M27_META.json`.
