# Live reliability candidate 1

This complete batch fails the reliability gate. Full mode verified 15/20
runs. Serial mode verified 17/20. The target is at least 19/20 in each mode.
No latency comparison is supported.

## Method

Code commit: `8dcd7e3c46d19db561cc44cba5e0d7bc401f1c00`. The benchmark
used `auth-refresh`, `mimo-v2.6-flash`, `https://api.xiaomimimo.com`, a
4096-token output reserve, and temperature 0. Each sample used a fresh
workspace. Full and serial modes alternated, with sequential live calls.
Malformed output received at most one retry with the same request and one
total deadline. The prompt included a full patch structure and a separate
file-string escaping example.

All 40 runs remain in [the sample corpus](LIVE_RELIABILITY_CANDIDATE_1_SAMPLES.jsonl).
The [run metadata](LIVE_RELIABILITY_CANDIDATE_1_META.json) identifies the
binary. The [aggregate](LIVE_RELIABILITY_CANDIDATE_1_MATRIX.json) was generated
with `scripts/live_check.mjs`. Published samples omit temporary workspace
paths. They contain no model response text or provider error bodies.

## Results

| Metric | Full | Serial |
|---|---:|---:|
| Verified runs | 15/20 | 17/20 |
| First-attempt verified runs | 13/20 | 14/20 |
| Model calls | 27 | 26 |
| Retries | 7 | 6 |
| Failed model calls | 12 | 9 |
| Malformed calls | 12 | 8 |
| Transport failures | 0 | 1 |
| Acceptance failures after a valid proposal | 0 | 0 |
| Calls with unavailable usage | 0 | 1 |
| Known input tokens | 47,061 | 43,575 |
| Known output tokens | 25,814 | 21,541 |

The serial token counts exclude the call with unavailable usage. No verified
price was supplied, so monetary cost remains unavailable. All failed runs
reported durable failed-task recovery, unchanged protected files, and no
workspace changes. Successful runs passed acceptance and recovered as
completed. The checker validated all samples, including failures.

## Decision

Keep this batch as failed evidence. Do not select its successful runs for a
new gate. Most rejected responses were valid JSON that failed the typed
decision contract. The recorded `invalid_decision` class does not identify
which field failed. Add safe field-shape categories and use one complete
example serialized from `AgentDecision`. Then run a separate matched batch.

The provider's [structured-output guide](https://mimo.mi.com/docs/en-US/quick-start/usage-guide/text-generation/structured-output)
states that JSON mode controls syntax; the prompt must define the schema.
Strict parsing, patch checks, and acceptance remain required.
