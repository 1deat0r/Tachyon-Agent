# Milestone 16: frozen held-out bounded-task baseline

Date: 2026-10-06 (Pacific/Auckland).

The unchanged production prompt verified **29/30 runs (96.7%)** across
three new synthetic tasks. All 30 proposals parsed on their first attempt.
The baseline used 30 live model calls, with no retries or provider failures.
One duplicate-range serial patch failed the hidden integer-boundary oracle.
Tachyon refused completion and recovered that task as failed. All 30 runs
preserved protected files and stayed within the authorized change path.

The evaluation is valid. It meets the declared exploratory product reference
line: at least 95% overall and at least 4/5 in each task/mode cell. This is
an observed score, not evidence that true reliability is at least 95%.
Five samples per cell support no speed comparison or general coding-agent
reliability claim. The failed cell remains visible.

## Frozen protocol and evidence

Inputs were frozen and published in `1e3e2e178b549ce21d88d29c50b9b0f38e8a4046`
before the first live call. The runner checked each input against that commit,
checked input and binary hashes before each run, and retained all planned
samples in order. The production prompt and provider implementation did not
change during measurement. The only production Rust change in the evaluation commit
added task identity to benchmark failure records.

- [Plan](HELDOUT_PLAN.md) and [input manifest](HELDOUT_MANIFEST.json).
- [Raw samples](HELDOUT_BASELINE_SAMPLES.jsonl),
  [run metadata](HELDOUT_BASELINE_META.json), and
  [validated matrix](HELDOUT_BASELINE_MATRIX.json).
- Binary SHA-256:
  `c8f4da8000cce9fec741c6408c28ba633545b34c42820f2c85f704fd6c6ff43a`.
- Manifest SHA-256:
  `6e9705fbb10ea6adfd4aa67eeee96084fd420ef512d8eed3f38e9992ea6228f9`.

Each task ran five full and five serial samples. The runner rotated task
order and alternated mode order. Calls ran sequentially on one host with
`mimo-v2.6-flash`, `https://api.xiaomimimo.com`, temperature 0, and a 4096-token
output budget. The model-stage deadline was 120 seconds, with a documented
one-second measurement allowance. Only malformed proposals permit a second
attempt. Semantic verification failures do not trigger that retry.

This batch completed without interruption. No pilot or replacement sample
is included. Provider-reported usage totals are 43,750 input tokens and
19,954 output tokens. All attempts have usage records. Cost remains unknown;
no verified price was supplied. Child stderr was discarded. Credentials and
raw provider responses are absent from the corpus.

## Independent task oracles

Only `subject/src/implementation.rs` was editable. Evidence included that
implementation, public exports, visible tests, an API consumer, and the task
README. Hidden acceptance tests were excluded from model evidence. Known
solutions were outside the copied task workspace.

| Task | Required behavior | Hidden acceptance boundary |
| --- | --- | --- |
| utf8-boundary | Truncate to the maximal UTF-8 prefix within a byte budget; preserve the label | Every budget over ASCII, emoji, combining marks, CJK, and empty text |
| atomic-transfer | Validate both balances before mutation; preserve state on error | Zero, insufficient funds, and overflow cases against a `u128` oracle |
| duplicate-range | Return the complete equal range or insertion range; preserve other functions | Empty, duplicate, absent, minimum, and maximum values against linear counts |

Before live calls, each broken fixture compiled and failed its visible test.
Each known solution passed the complete workspace. Controls rejected API
breaks, protected-file drift, and hidden-edge mutants that passed visible
checks. A scripted bad patch also exercised the real benchmark failure
schema, task identity, durable failure, recovery, and protected-path checks.

## Results

Wall times include failures. Percentiles use nearest rank; at n=5, p95 is
the largest observation. Values below are descriptive milliseconds, not
speed claims. The matrix also retains verified-only timings and first-edit
measurements; failed runs have no verified-success timing.

| Task | Mode | Verified | Valid first proposals | Calls | Wall p50 | Wall p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| utf8-boundary | full | 5/5 | 5/5 | 5 | 12,851 | 15,012 |
| utf8-boundary | serial | 5/5 | 5/5 | 5 | 13,708 | 35,980 |
| atomic-transfer | full | 5/5 | 5/5 | 5 | 9,418 | 19,676 |
| atomic-transfer | serial | 5/5 | 5/5 | 5 | 9,586 | 33,787 |
| duplicate-range | full | 5/5 | 5/5 | 5 | 19,121 | 47,099 |
| duplicate-range | serial | 4/5 | 5/5 | 5 | 12,005 | 14,796 |

Totals: full 15/15; serial 14/15. Do not compare modes as equally reliable.
All rows passed the frozen corpus validator, including the failure row.
Evaluation validity does not convert a product failure into a success.

## Retained failure and next development eval

Duplicate-range serial sample 3 produced a valid typed proposal and an
authorized patch. It computed an upper bound through `needle + 1`. At
`i64::MAX`, this overflowed. The hidden test rejected the patch. Task
`01a10da0-a49d-703b-a567-eec9e79d1894` recorded `verification_failed`, durable
failure, and `recovered_failed`. The protected files remained unchanged.

The [exact failed implementation](HELDOUT_FAILED_DUPLICATE_RANGE_SERIAL_3.rs.txt)
and [deterministic failure reproduction](HELDOUT_FAILED_DUPLICATE_RANGE_SERIAL_3.log.txt)
remain published. Re-running the hidden test against that retained workspace
exited 101 with `attempt to add with overflow`. This reproduction made no
model call and was not a new baseline sample.

The next proposed milestone is a development eval for integer-boundary
patches. Use this failure to test improvements without weakening acceptance.
Once used for tuning, these tasks become development data. Freeze new held-out
tasks before making a later reliability claim. This milestone establishes
an honest baseline; it does not implement semantic repair retries.

## Validation

`cargo verify` and `cargo verify full` passed before the frozen commit.
The full run included security/recovery checks, the existing 150-sample
scripted matrix, fixture checks, dependency audit, and performance gates.
The historical M14 progress checker was corrected to permit later milestones;
the separate freshness tests still require the latest completed milestone
and its successor. Both independent review axes cleared the implementation.
The seven held-out validator tests and three task-oracle controls passed.
Both review axes cleared the final data and report. Post-documentation
VERIFY passed: 887 tests passed, zero failed, and 15 ignored. The combined
held-out and existing live-validator suite passed all 16 tests.
