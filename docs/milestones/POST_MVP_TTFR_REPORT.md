# Post-MVP TTFR Composition Measurement

**Date:** 2026-09-28  
**Issue:** [#55](https://github.com/1deat0r/Tachyon-Agent/issues/55)  
**Purpose:** Measure the gateway path that was missing from the M14 report: task creation through the first replayed creation entry. This follow-up does not change the frozen M14 matrix or any MVP exit disposition.

## Measurement boundary

The benchmark starts `t0` immediately before writing an encoded `CreateTask` request to an already-running local gateway. The task-creation response is decoded, a fresh client connection is opened, and that client subscribes to the new task with `after_seq: -1`. This cursor includes the task's sequence-0 creation entry.

The benchmark stops `t1` when the `Subscribe` response frame has been read and decoded, including its replay `events` array. It then checks that the acknowledgment names the new task and that the first replay row has `seq: 0` and `kind: "created"`. Replay entries are carried in the `Subscribe` response payload; they are not separate `ServerFrame::Event` frames.

The timed path includes the CreateTask write and response, task-id extraction, subscriber connection setup, Subscribe request, journal replay query, response transfer, and response decoding. Gateway startup, session creation, warmup, UI rendering, model calls, and task execution are outside the interval.

This metric is separate from M13 T4, which starts with a task and subscription already in place and measures a `SendMessage` request through the first live journal frame. It has no pass/fail target; the §43 <50 ms target is not extended to this composite path.

## Setup and results

- Host: AMD Ryzen 7 5800X (8 cores / 16 threads), 30 GiB RAM, Linux 7.0.0-34-generic, ext4.
- Toolchain: `rustc 1.98.1 (48a229cea 2026-09-01)`, `cargo 1.98.1 (797e8a9bc 2026-08-05)`.
- Profile: Cargo `release`.
- Each invocation used 10 unmeasured warmup tasks, then 100 measured samples. Three invocations ran sequentially on the same host.
- Percentiles use the gateway harness's existing sorted-sample helper: `samples[n * p / 100]` with a zero-based index (the 51st and 96th sorted samples for n=100).

| Run | Samples | p50 | p95 |
|---:|---:|---:|---:|
| 1 | 100 | 402.167 µs | 526.429 µs |
| 2 | 100 | 357.786 µs | 439.158 µs |
| 3 | 100 | 388.847 µs | 488.949 µs |

The observed p50 range was 357.786–402.167 µs and the p95 range was 439.158–526.429 µs across these runs. These are measurements from one local machine and one synthetic local workload; they do not establish a cross-machine service target or a general Tachyon speed advantage.

## Reproduction

Run the ignored benchmark in release mode:

```sh
cargo test --release -p tachyon-gateway --test perf e2e_create_task_to_first_replayed_entry -- --ignored --nocapture
```

The benchmark reports `perf[T6]` without a `PASS` marker or threshold. M13 T1–T5 and their gate semantics remain unchanged.
