# Milestone 19: frozen multi-file API-preservation baseline

Date: 2026-10-06 (Pacific/Auckland).

**All 20 planned runs verified.** Each task verified 5/5 full and 5/5 serial
runs. There were 20 model calls, no retries, no provider errors, and no
acceptance failures. All runs changed both required implementation files,
kept protected files unchanged, and recovered as completed tasks.

The registered exploratory reference line was met: at least 95% overall
and at least 4/5 in each task/mode cell. This result covers two bounded
synthetic tasks. It does not establish a true reliability probability,
general coding-agent reliability, or a speed advantage. Do not pool earlier
milestones. Production prompt, acceptance, and retry behavior are unchanged.
The unselected M18 guidance remained disabled.

## Frozen protocol and provenance

The [plan](MULTIFILE_PLAN.md), [input manifest](MULTIFILE_MANIFEST.json),
fixtures, controls, and runner were published before the first live call in
`f22ebc711f17bbbe798ffa3a425fe673b234961c`.

- Binary SHA-256:
  `c4b3e5455a39377080bad7a67d9d620da3477507d3d82294b99ea67244267021`.
- Manifest SHA-256:
  `409404d2f5e62c9aec0555ee6ecaeea9475ff0004ebdd48eeef07b58a79d4ed9`.
- [All samples](MULTIFILE_SAMPLES.jsonl), [metadata](MULTIFILE_META.json),
  and [validated matrix](MULTIFILE_MATRIX.json).

Each task ran five full and five serial trials in fresh workspaces. Task
order rotated by round; mode order alternated. Calls ran sequentially.
All rows used the same release binary and unchanged baseline request path,
`mimo-v2.6-flash`, `https://api.xiaomimimo.com`, temperature 0, 4096 output
tokens, and a 120-second model-stage deadline with one-second measurement
allowance. The existing malformed-only policy allowed at most two attempts.
No trial used its retry allowance.

The runner compared frozen inputs with committed source before calls and
checked input/binary hashes before each row. The batch completed without
interruption, pilot, replacement, or missing row. Call records were flushed
before optional failure-artifact retention. Child stderr and raw provider
bodies were discarded. Usage is provider-reported. No verified price was
supplied, so monetary cost is unavailable.

## Independent acceptance and controls

Each task allows exactly two implementation paths to change. Public interfaces,
API consumers, acceptance tests, descriptors, and unrelated workspace members
are protected. Hidden acceptance tests stay outside model evidence. Known
solutions stay outside model workspaces.

| Task | Required repair | Independent acceptance |
| --- | --- | --- |
| canonical-frame | Encoder and decoder use the canonical big-endian length header | Golden wire bytes; payload lengths 0, 1, 2, 255, 256, 257, and 65535; oversize, truncated, missing, and extra data; public API signatures and labels |
| normalized-registry | Writer and reader trim Unicode whitespace and fold ASCII case | Literal key pairs including non-ASCII names; empty and duplicate rejection without state change; lookup, length, labels, and public API signatures |

Both broken fixtures compiled and failed visible behavior. Each complete
known solution passed its full workspace. Each of the four one-file repairs
compiled but failed the visible test for the remaining defect. API-corruption
controls failed compilation. Protected-file drift was rejected.

A mutually consistent little-endian encoder/decoder passed roundtrip checks
but failed the independent wire oracle. Unicode-wide case folding passed
visible registry checks but failed the hidden ASCII-only contract. These
controls show why roundtrips and visible examples alone cannot verify repair.

Failure retention tests rejected a linked workspace, linked source ancestors,
linked final files, unsafe relative paths, and unsafe task IDs. Regular
allowed sources were retained. No live failure occurred, so no failed live
patch artifact exists in this batch.

## Measured results

All-run wall times are descriptive milliseconds. Percentiles use nearest
rank; with five values, p95 is the largest observation. The matrix also
retains model and verified-first-edit timings. No statistical latency
comparison was registered, and `comparison_allowed` is false.

| Task | Mode | Verified | Calls | Wall p50 | Wall p95 | Input tokens | Output tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| canonical-frame | full | 5/5 | 5 | 11,850 | 20,864 | 8,470 | 3,883 |
| canonical-frame | serial | 5/5 | 5 | 17,095 | 42,426 | 8,470 | 4,783 |
| normalized-registry | full | 5/5 | 5 | 39,014 | 76,336 | 8,755 | 8,475 |
| normalized-registry | serial | 5/5 | 5 | 20,932 | 67,966 | 8,755 | 5,628 |

Total usage was 34,450 input and 22,769 output tokens. All 20 first proposals
parsed and passed acceptance. No usage attempt was unavailable. No provider,
verification, protected-path, or recovery failure was removed from scoring.

## Validation and next milestone

Fixture and partial-repair controls, 13 strict/legacy validator tests,
three failure-retention tests, VERIFY, and FULL passed before live calls.
FULL included the existing security/recovery, scripted matrix, integer-boundary,
dependency audit, and release performance checks. Spec and standards reviews
cleared the implementation and final data/report. Post-documentation VERIFY
passed: 887 tests passed, 0 failed, and 15 ignored. Both Clippy checks passed.

The next proposed milestone is a frozen stateful cross-module repair eval.
Add new tasks whose failure paths must preserve state across modules. Use
independent operation-sequence oracles, public API consumers, partial-repair
controls, protected-path checks, and recovery evidence. This batch offers no
failed patch to justify prompt selection or semantic repair retries. Increase
task difficulty before changing production behavior.
