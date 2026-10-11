# Preflight verification performance failure — bounded context diagnosis

Date: 2026-10-11 (Milestone 21).
Subject: the intermittent
`comp[verification_plan_and_run_latency]` failure recorded in
`docs/milestones/STATEFUL_PREFLIGHT_PERF_FAILURE.log.txt`
during M20 preflight.

**Status: root-caused as an environmental, load-sensitive
wall-clock measurement. Not fixed, and no budget was changed.**
The failure evidence, its limits, and this diagnosis are
preserved as-is. No production code, threshold, feature
selection, CPU affinity, or timeout was changed to obtain any
pass.

## What failed

One FULL preflight invocation during M20 (2026-10-06):

```
comp[verification.run] n=20 p50=39.992993ms p95=54.972681ms
thread 'comp_verification_plan_and_run_latency' panicked:
verification regression: run p50=39.992993ms >= 31.512ms
```

The same test binary passed in isolation at 20.824079 ms, the
single-package binary passed at 20.349352 ms, the complete
performance gate passed in isolation at 18.368900 ms, and a
fourth complete FULL run passed with verification p50 19.668307
ms and process-wait p50 4.042505 ms (all numbers from the M20
report and its retained recheck log).

## What the test measures

`crates/tachyon-verify/tests/perf.rs` runs 20 full
verification runs against a trivial always-passing `python3 -c
pass` command in a scratch workspace. Each run exercises the
production path end to end: contract/graph validation, fresh
private evidence, the policy-bound command spawn, process-group
drain, and report building. The measured quantity is real
wall-clock child-process spawn latency plus harness overhead —
by design, the report states both halves
(`comp[verification.plan]` ≈ 40–55 µs; the run is dominated by
the child).

## What the budget is

The assertion budget is `linux_process_drain_budget(18 ms)`:

```
18 ms base + (visible /proc process count × 12 µs) + 6 ms
```

The process-count term scales the allowance for the process
runner's `/proc` drain scan. At the M20 failure the budget was
31.512 ms, which derives to ≈ 626 visible processes. The budget
therefore scales with process-table size but has **no allowance
for machine load, CPU scheduling contention, or page-cache
state**.

## Diagnosis

The failure is a transient roughly-2× increase in real child
spawn latency (39.99 ms measured vs ≈ 20.8 ms for the same
binary in isolation), crossing a budget that only models the
drain-scan cost. The mechanism is environmental contention, not a
code-path regression:

- The identical binary passed in isolation, passed on rerun, and
  passes now; nothing in production code, the threshold, feature
  selection, CPU affinity, or timeouts changed between the
  failing and passing runs.
- The gate runs eight crates' release perf suites back-to-back
  on a single-user development machine that also runs the
  development harness itself; spawn latency is a tail-sensitive
  wall-clock quantity, so a contention window coinciding with
  the 20-sample window roughly doubles the p50.
- Reproduction is intermittent by nature: it occurred once in
  the four FULL invocations recorded across M20, and did not
  recur in two full-gate runs and three isolated runs during
  this diagnosis (see measurements below).

## Measurements gathered during this diagnosis (2026-10-11)

Host: 16 logical CPUs; visible process count 3884–3987 (budget
≈ 70.6 ms at that count); load average 6.1–11.4 during the
runs (this diagnosis session itself contributes load).

| Run | verification.run p50 | Result |
| --- | --- | --- |
| Isolated ×3 | 20.322 / 20.861 / 20.648 ms | pass (budget ≈ 70.6 ms) |
| Full gate ×2 | 20.072 / 20.208 ms | pass, `perf gate ok` |
| Raw `python3 -c pass` ×20 | p50 9.654 ms, min 8.892, max 10.392 ms | tight distribution under current load |

The raw probe shows the child's own startup is ≈ 10 ms with a
narrow spread; the remaining ≈ 10 ms of each verification run
is the production process runner (fork/exec, process-group
setup, drain, report) — the same split the M13 report recorded.

## Limits (preserved)

- Not reproduced on demand during this diagnosis; the
  load/context hypothesis is consistent with the evidence but
  unconfirmed, because **no system metrics (load average, CPU
  frequency, page-cache residency) were captured during the
  original M20 failure**.
- Single host, single-user machine; the development harness's
  own load is present in every measurement here.
- n=20 samples per invocation with nearest-rank percentiles;
  the p50 is the 10th of 20 sorted samples, so a short
  contention window can move it materially.
- The M20 product score (16/20) is separate from this preflight
  issue; this diagnosis neither explains nor changes it.

## Explicitly not done

No production code change, no budget or threshold increase, no
CPU affinity, no timeout change. A larger budget would mask
load spikes without explaining them, and the retained failure
evidence stands unchanged.

## Candidate follow-ups (not selected; each needs measured evidence first)

1. Machine-load-scaled budget: measure a spawn-cost baseline (or
   load average) at runtime and scale the allowance the same way
   the process-count term scales the drain scan.
2. Repeated-measures design: median of per-invocation p50s
   across gate invocations, separating transient contention from
   a true regression.
3. Isolated invocation: run the verify perf suite in its own
   gate invocation to remove cross-suite cache pressure.

Selection among these belongs to a future milestone with
measured evidence, per the repository's performance discipline.
