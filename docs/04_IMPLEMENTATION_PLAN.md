# 04 — Dependency-Ordered Implementation Plan

Milestones remain dependency ordered; do not start a later capability before its earlier gates pass. The normal local workflow is documented in `docs/DEVELOPMENT_WORKFLOW.md`.

## Milestone 0 — Foundation

Build:

- workspace manifests;
- CI;
- `tachyon-types`;
- `tachyon-protocol` skeleton;
- configuration loading/precedence;
- `tachyon` binary with `--version` and `doctor`;
- tracing bootstrap.

Gate:

```bash
cargo verify
tachyon doctor
```

Update `PROGRESS.md`.

## Milestone 1 — Durable task kernel

Build:

- SQLite migrations;
- StoreWriter;
- sessions/tasks;
- append-only event journal;
- task snapshots;
- Task Supervisor actor;
- local gateway lifecycle;
- basic CLI client.

Gate: create a dummy task, restart gateway/process, recover same task/session/state and continue.

## Milestone 2 — Execution IR + scheduler

Build:

- ExecutionGraph/Node;
- DAG validation/cycle detection;
- access set hierarchy;
- atomic conflict grants;
- resource claims;
- scheduler loop;
- structured cancellation;
- fake executor.

Gates:

- independent fake nodes run concurrently;
- dependencies are honored;
- conflicting writes never overlap;
- random/property scheduler tests pass;
- task cancellation leaves no child running.

## Milestone 3 — Policy + native tools

Build:

- capability registry;
- policy engine;
- approvals bound to operation hash;
- filesystem read/list/metadata;
- direct process runner;
- Git status/diff/log;
- artifact spool;
- credential-handle skeleton/redaction.

Gate: workspace-local normal reads/build tools can run automatically; outside-workspace write produces explicit approval/deny according to policy; traversal/symlink escape tests pass.

## Milestone 4 — Repository intelligence

Build:

- workspace inventory + BLAKE3;
- watcher/incremental invalidation;
- Tree-sitter language interface;
- initial Rust/TypeScript/Python symbol extraction if practical;
- lexical search abstraction;
- symbol/reference index;
- freshness/hash verification.

Vertical Slice A:

```text
> Where is refreshToken defined and used?
```

Requirements:

- zero LLM calls;
- zero Jev calls;
- structured source locations;
- warmed target: <250 ms p50, <500 ms p95 on representative fixtures.

Do not begin model routing until this slice is good.

## Milestone 5 — Predictive router

Build:

- DirectNative/EvidenceFirst/ReasoningFirst/Hybrid classification;
- deterministic rules;
- historical EWMA estimates;
- evidence grace-window mechanism;
- route telemetry;
- serial reference mode.

Gate: simple repo/search/git commands never invoke a primary model; clearly complex requests can start local evidence and model preparation concurrently.

## Milestone 6 — Model layer

Build:

- ModelProvider/registry;
- capability negotiation;
- role mapping;
- context blocks/trust;
- EvidencePackage→context assembly;
- structured AgentDecision;
- streaming model event adapter;
- FakeModelProvider;
- first real provider adapter selected by implementer/configuration.

Vertical Slice B:

```text
> Explain why these two implementations behave differently.
```

Use existing repo evidence and one reasoning call where sufficient.

## Milestone 7 — Judgment/OpenJEV

Build:

- JudgmentProvider;
- batched judgment request/response;
- OpenJEV adapter behind optional feature/config;
- certainty policies per judgment;
- provider outage fallback.

Gate: A/B benchmark must show at least one workload where judgment routing improves latency/cost/model-call avoidance without reducing verified success. If no such workload exists yet, keep the provider but do not put it on default critical paths.

## Milestone 8 — Mutation engine

Build:

- PatchSpec + base hash;
- MutationBatch journal;
- preimage artifact retention;
- temp write + per-file replace;
- crash recovery/rollback;
- changed-file events.

Vertical Slice C:

```text
> Fix the incorrect implementation.
```

Gate: crash injection between every file commit recovers to a coherent documented state; stale preimage refuses write.

## Milestone 9 — Verification

Build:

- AcceptanceContract;
- project detector interface;
- verification planner;
- affected checks first;
- command/verifier nodes;
- completion gate.

Gate: a model-proposed patch cannot become Completed while required verifier fails.

## Milestone 10 — Full debugging task

Benchmark target:

```text
> Find why authentication occasionally fails after token refresh and fix it.
```

Must demonstrate:

- concurrent evidence acquisition;
- compact reasoning context;
- validated IR;
- recoverable mutation;
- selective/parallel verification where useful;
- durable state and steering.

## Milestone 11 — TUI

Only now polish Ratatui UX:

- conversation;
- live operations;
- changed files;
- approval UI;
- streaming output;
- task status;
- pause/resume/cancel;
- steering;
- optional execution graph inspection.

Gate: disconnect/close TUI without cancelling task; reconnect/attach and replay state.

## Milestone 12 — Recovery hardening

Systematic process-kill/fault injection across:

- native reads;
- model/Jev calls;
- mutation;
- verifier;
- approval wait;
- keyed/queryable effect fixture.

No broad feature work until recovery suite passes.

## Milestone 13 — Performance campaign

Profile and optimize actual critical path:

- routing;
- IPC;
- persistence;
- indexing;
- scheduler dispatch;
- model wait;
- verification;
- process output handling.

Do not optimize via unmeasured rewrites.

## Milestone 14 — MVP freeze

Run full benchmark matrix and security/recovery suites. Produce `MVP_REPORT.md` with:

- verified-success comparison;
- median/p95 TTFR and completion;
- critical-path breakdown;
- model/Jev/tool calls;
- known limitations;
- deferred work recommendations.

Only after this gate should workflow compilation, browser/computer-use, distributed workers or multi-agent specialization be considered.
