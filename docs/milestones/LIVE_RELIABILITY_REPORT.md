# Live-model reliability — 2026-10-06

The bounded `auth-refresh` gate passes: full mode verified 19/20 runs and
serial mode verified 20/20. The predeclared target was at least 19/20 in
each mode. This result covers one fixture and one provider/model. It does
not establish general coding-agent reliability.

## Candidate and method

Production code: `3cd7b5b20ed46966c4e89986847fa12c77f66637`. The source
tree also contains the documentation-only commit `455306e`. The benchmark
used `mimo-v2.6-flash` at `https://api.xiaomimimo.com`, temperature 0, a
4096-token output reserve, and a 120-second total model-stage deadline.
Full and serial samples alternated sequentially. Every run used a fresh
workspace and the same acceptance contract. Builds finished before live
measurement. Other machine and provider activity was not controlled.

The model receives a complete example serialized from `AgentDecision`.
Parsing, operation validation, policy checks, revision fences, preimage
checks, and acceptance remain strict. Only malformed output receives one
pre-mutation retry. Both attempts use identical requests and one deadline.
Tachyon records actual calls and usage, including rejected responses.

The [40-sample corpus](LIVE_RELIABILITY_SAMPLES.jsonl),
[run metadata](LIVE_RELIABILITY_RUN_META.json), and
[aggregate](LIVE_RELIABILITY_MATRIX.json) are the durable evidence. The
metadata identifies the binary and completed runner state. Published rows
omit temporary workspace paths and contain no model response text, provider
error bodies, or credentials.

## Measured result

| Metric | Full | Serial |
|---|---:|---:|
| Verified runs | 19/20 | 20/20 |
| First-attempt verified runs | 19/20 | 19/20 |
| First-attempt valid decisions | 20/20 | 19/20 |
| Actual model calls | 20 | 21 |
| Retries | 0 | 1 |
| Failed provider calls | 0 | 1 |
| Acceptance failures | 1 | 0 |
| All-run wall p50 / p95 | 12,189 / 46,089 ms | 11,579 / 25,058 ms |
| Verified-run wall p50 / p95 | 12,189 / 50,810 ms | 11,579 / 25,058 ms |
| Verified first-edit p50 / p95 | 11,876 / 50,513 ms | 11,283 / 24,772 ms |
| Model-stage p50 / p95 | 11,872 / 45,802 ms | 11,280 / 24,767 ms |
| Input tokens | 36,700 | 38,535 |
| Output tokens | 17,682 | 18,352 |
| Calls with unavailable usage | 0 | 0 |

Percentiles use nearest rank: sort the samples and select `ceil(n × p)`.
The 19-success full-mode p95 therefore selects its largest success latency;
the 20-run p95 selects rank 19. These are different populations. The counts
include the rejected serial response. All 41 calls reported usage. No
verified price was supplied, so monetary cost remains unavailable.

Verified success differs between modes. The comparison rule therefore
prohibits a full-versus-serial speed claim. The table reports observations
only. Provider latency varied markedly across the exploratory and formal
runs, so this report also makes no causal speed claim against older batches.

## Failures drive the next eval

- Serial sample 2 omitted a required decision field. The first call was
  classified `malformed_output / missing_decision_field`. The one allowed
  retry produced a valid patch that passed acceptance. Both calls and their
  usage remain in the corpus.
- Full sample 14 returned a valid decision and an authorized patch, but
  changed `generation(&self)` to `generation(&u64)`. This breaks compilation.
  Acceptance refused completion. The task recovered as failed, and protected
  files stayed unchanged. The expected session file was changed; this was a
  authorized mutation that failed verification. The
  [exact failed replacement](LIVE_RELIABILITY_FAILED_PATCH.txt) is retained
  as an eval counterexample. Its SHA-256 is
  `11749d4fb3fd763245fb37a0f51c471b58dfcb9a3a1338a34cc694f4e22d6de4`.

Do not repair this counterexample by weakening acceptance or adding a
fixture-specific prompt hint. A later held-out coding eval should measure
preservation of unrelated methods, compilation, regression tests, allowed
change paths, and durable completion. Valid JSON is only an intermediate
check; verified task success remains the scoring rule.

## All trials remain visible

The [first complete candidate](LIVE_RELIABILITY_CANDIDATE_1.md) failed at
15/20 full and 17/20 serial. It used the earlier prompt. All 40 rows remain
published. The refined prompt was frozen before the current formal batch.

An earlier run of that refined candidate was interrupted by the user after
20 recorded successes (10 per mode). Its
[samples](LIVE_RELIABILITY_CANDIDATE_2_INTERRUPTED_SAMPLES.jsonl) and
[metadata](LIVE_RELIABILITY_CANDIDATE_2_INTERRUPTED_META.json) remain separate.
It is incomplete and cannot pass the gate. An in-flight call may lack a
receipt; its usage is unreconciled. The current gate started afresh rather
than treating missing samples as successes or merging selected rows.

The [exploratory pilot archive](LIVE_RELIABILITY_PILOTS.json) records the
retry-only pilot (5/6), explicit escaping pilot (6/6), diagnostic probe
(1/1), and complete-response pilot (5/6). The last pilot had one deadline
timeout with no edits, durable failed recovery, and unavailable usage.
Early pilots used evolving builds and lack complete binary provenance.
None is pooled into the gate. Small pilot success did not justify a
reliability claim. These artifacts do not reconcile total provider billing.

## Verification and limits

Workspace VERIFY passed with 887 tests, zero failures, and 15 ignored tests.
FULL passed its security/recovery suites, fixture gate, 150-run scripted
matrix, audit, and release performance checks. The checker has nine tests
that reject missing, contradictory, unsafe, and mixed samples. Independent
standards and spec reviews found no material defect in the implementation.

The capability's retry, cancellation, crash, and usage limits are stated in
[the capability contract](../agents/model-retry-capability.md). A dropped
request does not prove that remote generation stopped. Calls lost during
process termination may have unreconciled billing. No consequential effect
is blindly replayed. No model self-report can pass acceptance.

The empirical fixture gate is complete. Twenty runs per mode provide a
small estimate. They do not prove that the underlying success probability
is at least 95%, and the reused fixture is not a held-out general coding
eval. The next useful eval milestone is a frozen set of unseen bounded
tasks with API-preservation and regression oracles, scored before prompt
changes are selected.

## Reproduce the artifact check

```sh
LIVE_MATRIX_OUT=target/live-reliability/recomputed.json \
  node scripts/live_check.mjs \
  docs/milestones/LIVE_RELIABILITY_SAMPLES.jsonl --require-reliable
```

Live rerun instructions are in [the plan](LIVE_RELIABILITY_PLAN.md). JSON
mode controls syntax; the prompt must define the response schema, as the
provider's [structured-output guide](https://mimo.mi.com/docs/en-US/quick-start/usage-guide/text-generation/structured-output)
explains. Tachyon still validates the result and verifies the task.
