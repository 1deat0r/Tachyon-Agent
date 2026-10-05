# Milestone 18: boundary-guidance candidate experiment

Date: 2026-10-06 (Pacific/Auckland).

**Do not adopt the candidate.** Baseline and candidate both verified 15/15
runs. The development task tied at 5/5 per arm, so the declared strict
improvement requirement was not met. All 30 planned rows are valid evidence.
There were 30 model calls, no retries, no provider failures, and no rejected
patches in this batch. All protected-file and completed-recovery checks passed.

This tied result does not show equivalence or improvement in true reliability.
Five samples per task/arm support no speed or general coding-agent claim.
The candidate remains opt-in benchmark instrumentation. The production prompt,
model policy, acceptance, and retry behavior remain unchanged.

## Candidate and frozen protocol

The fixed instruction reads:

> Before proposing replacement code, check integer-limit, empty-input, and
> failure-state behavior against the required contract. Preserve public APIs
> and unrelated behavior. Do not rely on wrapping or saturating arithmetic
> unless the contract requires it.

The benchmark provider appends this text only to a trusted system block
when `TACHYON_BENCH_VARIANT=boundary-guidance`. The default baseline forwards
requests unchanged. Unknown variants and missing trusted system context are
rejected. Forwarding tests exercise both arms through the provider interface.
This is a post-assembly request transform. It is absent from the persisted
production `ContextSlice`; it is not a production-ready prompt feature.

The [plan](CANDIDATE_PLAN.md) and [manifest](CANDIDATE_MANIFEST.json) were
published before the first live call in
`14ffb3132c8921b98f72d0bb8dfb072f81523e05`. No candidate, task, or protocol
change occurred during measurement. Both arms used the same release binary,
model, endpoint, evidence, acceptance, and full-mode execution path.

- Binary SHA-256:
  `c4b3e5455a39377080bad7a67d9d620da3477507d3d82294b99ea67244267021`.
- Manifest SHA-256:
  `de910a7d70118b598d6794a90c4b35b762d9c3c4830ae8a92db2dbccbd38fb4d`.
- [All samples](CANDIDATE_SAMPLES.jsonl), [metadata](CANDIDATE_META.json),
  and [validated matrix](CANDIDATE_MATRIX.json).

Five matched arm pairs ran first on the reused `duplicate-range` development
task, then on each new held-out task. Arm order alternated by sample.
Calls ran sequentially in fresh workspaces. Provider settings were
`mimo-v2.6-flash`, `https://api.xiaomimimo.com`, temperature 0, 4096 output
tokens, a 120-second model-stage deadline with one-second measurement
allowance, and at most two attempts for malformed output only.

The runner checked source bytes against the committed manifest and checked
input/binary hashes before every row. The batch completed without interruption,
pilot, replacement sample, or missing usage. Child stderr and raw provider
bodies were not retained. Usage is provider-reported. No verified price was
supplied, so monetary cost is unavailable.

## Task oracles

Only `subject/src/implementation.rs` could change. Protected acceptance tests
were excluded from model evidence. Public API consumers and visible regression
checks were evidence. Known solutions stayed outside model workspaces.

| Task | Role | Acceptance |
| --- | --- | --- |
| duplicate-range | Reused development data | Complete equal/insertion range, minimum/maximum values, API preservation |
| signed-midpoint | New held-out data | Mathematical floor of `(a+b)/2` over signed limits; independent `i128` oracle |
| ceiling-division | New held-out data | Ceiling over unsigned limits; zero denominator yields `None`; independent `u128` oracle |

The new broken fixtures compiled and failed visible behavior. Known solutions
passed complete workspace tests. API-corruption controls failed compilation.
Rounding/zero-input mutants passed visible checks but failed hidden boundaries.
Every live row separately passed protected-path and recovery validation.

## Measured results

All-run task wall times below are descriptive milliseconds. Percentiles use
nearest rank. At five observations, p95 is the largest value. The matrix also
retains model and verified-first-edit timings. These samples do not support
a mode or arm speed claim; no statistical latency decision was registered.

| Task | Arm | Verified | Calls | Wall p50 | Wall p95 | Input tokens | Output tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| duplicate-range | baseline | 5/5 | 5 | 58,609 | 79,013 | 7,115 | 2,449 |
| duplicate-range | candidate | 5/5 | 5 | 6,652 | 9,900 | 7,330 | 2,530 |
| signed-midpoint | baseline | 5/5 | 5 | 9,009 | 22,132 | 6,330 | 4,354 |
| signed-midpoint | candidate | 5/5 | 5 | 22,373 | 47,097 | 6,545 | 6,176 |
| ceiling-division | baseline | 5/5 | 5 | 13,393 | 17,747 | 6,495 | 2,475 |
| ceiling-division | candidate | 5/5 | 5 | 11,854 | 14,498 | 6,710 | 2,438 |

Baseline usage: 19,940 input and 9,278 output tokens.
Candidate usage: 20,585 input and 11,144 output tokens.
Total usage: 40,525 input and 20,422 output tokens.
All 30 first proposals parsed and passed acceptance. No outcome was hidden
or pooled with the M16 baseline.

The candidate met the exploratory reference line and did not regress on
these new tasks. It failed the required development improvement condition.
`eligible_for_production_design` and `production_adopted` are both false.
Evaluation validity is separate from this negative adoption decision.

## Validation and next milestone

Forwarding tests, strict negative scoring tests, fixture/oracle controls,
VERIFY, and FULL passed before the frozen commit. FULL included the M17
integer-boundary controls, security/recovery checks, the existing scripted
matrix, dependency audit, and performance checks. Spec and standards reviews
cleared the implementation and final data/report. Post-documentation VERIFY
passed: 887 tests passed, 0 failed, and 15 ignored. The two
benchmark-forwarding tests and 13 candidate/legacy live-validator tests also passed.

M16 reports and raw samples remain unchanged. As benchmark instrumentation
has evolved, its historical frozen input hashes are checked against the
original recorded source commit. Running the old current-tree freeze checker
requires that original checkout. New experiment provenance uses its own manifest.

The next proposed milestone is a frozen multi-file/API-preservation eval.
The perfect ties on these small one-file tasks offer no success signal for
candidate selection. Increase task difficulty with independent regression,
API, authorized-change, and recovery checks. Do not lower acceptance or add
this unselected instruction to the production prompt. This experiment does
not justify semantic repair retries.
