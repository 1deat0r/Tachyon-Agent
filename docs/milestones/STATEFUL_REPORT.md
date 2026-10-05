# Milestone 20: frozen stateful cross-module repair baseline

Date: 2026-10-06 (Pacific/Auckland).

**The baseline verified 16/20 runs (80%). The reference line was not met.**
Reservation-ledger verified 3/5 full and 3/5 serial runs. Indexed-catalog
verified 5/5 in each mode. All 20 planned rows are valid evaluation evidence.
There were 20 model calls and no retries. Failures were one incomplete repair,
one transport error, and two model-stage timeouts. All protected files stayed
unchanged. All 16 successes recovered as completed; all four errors were
durable and recovered as failed. No failure was replaced or omitted.

The registered reference was >=95% overall and >=4/5 in each cell. Both
conditions failed. Evaluation validity is separate from this negative product
result. These two synthetic tasks and five trials per cell establish no true
reliability probability, general coding-agent reliability, or speed advantage.
Do not pool earlier milestones. Production prompt, acceptance, model policy,
and malformed-only retry remained unchanged. M18 guidance stayed disabled.

## Frozen protocol and provenance

The [plan](STATEFUL_PLAN.md), [manifest](STATEFUL_MANIFEST.json), fixtures,
controls, and runner were published before the first live call in
`2377df6baba42fcf2668406394f6e461ceaa1f19`.

- Binary SHA-256:
  `c4b3e5455a39377080bad7a67d9d620da3477507d3d82294b99ea67244267021`.
- Manifest SHA-256:
  `4e7122c55593004e7710ad9529217c6b8ee7f4d9c9426e61495e30544d060b3d`.
- [All samples](STATEFUL_SAMPLES.jsonl), [metadata](STATEFUL_META.json),
  and [validated matrix](STATEFUL_MATRIX.json).

Each task ran five full and five serial trials in fresh workspaces. Task
order rotated by round; mode order alternated. Calls ran sequentially.
All rows used the same binary, unchanged baseline request path,
`mimo-v2.6-flash`, `https://api.xiaomimimo.com`, temperature 0, 4096 output
tokens, and a 120-second model-stage deadline with 1000 ms measurement
allowance. At most two attempts were allowed only for malformed output.
No malformed output occurred, and no retry was used.

The runner checked frozen input bytes against committed source before calls
and checked input/binary hashes before every row. It flushed each call record
before optional failure retention. The batch completed without interruption,
pilot, replacement trial, or missing row. Child stderr and raw provider bodies
were discarded. Three failed provider attempts have explicitly unknown usage.
Attempt accounting remains complete; token accounting does not invent values.
No verified price was supplied, so monetary cost is unavailable.

## Independent acceptance and controls

| Task | Authorized implementation files | Acceptance |
| --- | --- | --- |
| reservation-ledger | reserve.rs, release.rs | Aggregate duplicate quantities; validate unknown/overflow/insufficient errors in declared order and smallest-SKU order; atomic stock/held state; empty and zero-quantity batches; integer limits; public APIs |
| indexed-catalog | insert.rs, remove.rs, rename.rs | Exact names; duplicate/unknown errors preserve both indexes; same-name rename; removal and name reuse; public APIs and snapshots |

Both broken fixtures compiled and failed visible behavior. Known complete
solutions passed full workspace acceptance. Every proper nonempty subset of
repaired files compiled and failed visible acceptance for each remaining
defect: two ledger subsets and six catalog subsets. API-corruption controls
failed compilation, and protected-file drift was rejected. Hidden tests stayed
outside model evidence. Known solutions stayed outside model workspaces.

Independent public-interface oracles use vector reference states rather than
the implementations' maps. Ledger checks enumerate 2,187 length-three
operation sequences across three initial states, with 6,561 checked transitions.
Catalog checks enumerate 729 length-three sequences, with 2,187 transitions.
Additional literal sequences cover integer limits, reversed error order,
exact Unicode/empty names, name reuse, and error precedence. These are bounded
exhaustive sequence sets, not proofs for arbitrary-length inputs.

A last-write-wins quantity mutant passed visible ledger tests but failed the
hidden sequence oracle. A stale reverse-index rename mutant passed visible
catalog tests but failed its hidden sequence oracle. Safe failure-retention
controls rejected linked roots/ancestors/files and unsafe paths/task IDs.

## Failures retained and reproduced

| Task/mode/sample | Outcome | Observed changes | Usage | Recovery |
| --- | --- | --- | --- | --- |
| reservation-ledger/full/3 | verification_failed | reserve.rs only | Provider-reported | recovered_failed |
| reservation-ledger/serial/3 | transport | None | Unknown | recovered_failed |
| reservation-ledger/serial/4 | timeout | None | Unknown | recovered_failed |
| reservation-ledger/full/4 | timeout | None | Unknown | recovered_failed |

The incomplete repair's [reserve source](STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.reserve.rs.txt)
and [unchanged release source](STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.release.rs.txt)
are retained. A fresh workspace replay compiled, then failed both visible and
hidden acceptance; the [replay log](STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.log.txt)
records these results. Visible reserve behavior passed, but unchanged release
behavior still mutated state before returning an error. This is a partial
repair, not a provider-format failure. The recorded live call lasted 112.6 s.

The transport call lasted 112.0 s. Each timeout occurred at 120.0 s. These
three errors made no file changes; retained workspace sources matched the
broken fixture, so there is no failed patch for them. Failure class and call
records are retained even when usage is unavailable. The data does not explain
the upstream transport failure or provider delay. Do not infer that a retry
or longer deadline would have succeeded.

## Measured results

All-run wall times below include failed trials and are descriptive milliseconds.
Percentiles use nearest rank; with five values, p95 is the largest observation.
The matrix separately retains verified-only wall and first-edit timings.
`comparison_allowed` is false. No statistical latency comparison was registered.

| Task | Mode | Verified | Calls | Wall p50 | Wall p95 | Known input | Known output | Unknown usage calls |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| reservation-ledger | full | 3/5 | 5 | 94,528 | 120,006 | 8,952 | 9,210 | 1 |
| reservation-ledger | serial | 3/5 | 5 | 33,382 | 120,006 | 6,714 | 4,908 | 2 |
| indexed-catalog | full | 5/5 | 5 | 13,630 | 26,539 | 12,320 | 7,768 | 0 |
| indexed-catalog | serial | 5/5 | 5 | 30,604 | 36,811 | 12,320 | 10,287 | 0 |

Known provider-reported usage was 40,306 input and 32,173 output
tokens across 17 calls. The complete batch token totals are unavailable because
three calls have unknown usage. All 17 successful provider responses parsed;
16 passed acceptance. Provider success is not task success.

## Preflight checks and unresolved performance behavior

Fixture/partial-repair/state controls, 14 strict/legacy scoring tests, and
three retention tests passed. Spec and standards reviews cleared the implementation
and final data/report. VERIFY passed. FULL passed before source publication and live calls, including
security/recovery, existing scripted matrix, integer-boundary, dependency audit,
and release performance checks. Post-documentation VERIFY passed: 887 tests
passed, 0 failed, and 15 ignored. Both strict Clippy checks passed.

Three earlier FULL invocations failed. The first two wrapper summaries did not
retain enough detail to identify their failing check. The third complete
capture identifies verification-run latency: p50 39.992993 ms exceeded its
unchanged 31.512 ms budget. The [failure log](STATEFUL_PREFLIGHT_PERF_FAILURE.log.txt)
is retained. The same FULL test binary passed in isolation at 20.824079 ms;
the single-package binary passed at 20.349352 ms. The complete performance gate
then passed in isolation at 18.368900 ms ([log](STATEFUL_PREFLIGHT_PERF_ISOLATED.log.txt)).
A fourth complete FULL run passed with verification p50 19.668307 ms and
process-wait p50 4.042505 ms ([recheck log](STATEFUL_PREFLIGHT_PERF_RECHECK.log.txt)).

No production code, threshold, feature selection, CPU affinity, or timeout was
changed to obtain that pass. Load/context is an unconfirmed hypothesis.
The passing rerun does not establish stable performance or explain the prior
failures. This preflight issue remains separate from the live product score.

## Next milestone

The next proposed milestone is deterministic production-driver replay of the
M20 failure classes. Pin the exact partial patch and prove rejection, durable
failure, and recovery through the production driver. Also exercise typed
transport and deadline failures with unknown usage, zero writes, and no blind
retry. Use no live calls for these regressions. This creates development data
before any prompt, retry, or deadline candidate is selected.

The intermittent verification performance failure also needs a bounded
context diagnosis. Preserve its limits and failure evidence. Do not claim
that M20 fixed it or that a larger budget would explain it.
