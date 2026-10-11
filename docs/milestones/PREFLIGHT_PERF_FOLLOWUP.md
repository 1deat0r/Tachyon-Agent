# TASK 09 Report: Preflight Perf Follow-up (candidate 1 selected by measurement)

## Selection measurement (2026-10-11, 16 CPUs, release mode)

`comp[verification.run]` n=20 per invocation:

- Idle (3 runs): p50 ≈20.5-21.5ms, p95 ≈22-23.6ms.
- 8 busy loops (3 runs): p50 ≈26.8-28.7ms (+30%), p95 spike 42.8ms once.
- 24 busy loops: p50 ≈57ms.

The budget scaled only with `/proc` process count, not load — confirming
the diagnosed mechanism. Candidate 2 (repeated measures) cannot fix a
sustained-load shift; candidate 3 (isolated invocation) does not survive
shared CI. Candidate 1 (load-scaled budget) selected.

## Implementation (`crates/tachyon-verify/tests/perf.rs`, test-only)

- `load_pressure()` = max(1-min loadavg, instantaneous `procs_running`)
  per CPU. The instantaneous term matters: 8 fresh loops still read 0.22
  on the 1-minute average while p50 rose 30%.
- Budget gains `base * clamp(pressure - 0.5, 0, 2)`: knee at 0.5 keeps the
  idle gate bit-for-bit unchanged (31.764ms before and after); cap at two
  extra bases bounds masking.
- Pressure above 1.5x reports `load_void=true` and returns: beyond that,
  spawn latency explodes superlinearly past any honest linear cap, so the
  run reports void instead of passing or failing (void-batch precedent).

## Before/after proof

- Idle: pass, budget 31.764ms (identical to old formula).
- 8 loops: pass, budget 35.3ms (old formula ≈31.8ms, marginal/false-fail zone).
- 24 loops: `load_void=true`, pass with marker (old code: hard FAIL at
  p50=56.7ms vs 50.0ms capped budget — a false regression verdict).
- Full `sh scripts/perf_gate.sh`: `perf gate ok`. `cargo fmt --check` clean.

No production code, threshold, affinity, or timeout changed. Limits: single
host; void runs prove nothing about performance — they only refuse to lie.
