# Regression Budget (TASK 52)

## Thresholds

- Warmed symbol/reference query: <250 ms p50, <500 ms p95 (acceptance spec).
- Router deterministic path: <2 ms p95. Scheduler dispatch: <1 ms p95.
- Alert on >20% p95 regression over a 7-day rolling median, n >= 20 samples
  on the controlled perf host.

## Rules

- No wall-clock microbenchmark blocks CI on shared/noisy hosts.
- Trends tracked nightly; alerts are tickets, not gates.
- Every report records median + p95 + N + environment + hardware.
