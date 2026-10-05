# Tachyon Progress

This file is updated by the implementing agent after every milestone.

## Current milestone

Milestone 19 — proposed frozen multi-file/API-preservation eval.
Milestone 18 is complete. Baseline and generic boundary-guidance candidate
both verified 15/15 full-mode runs, including two new held-out tasks.
The declared strict-improvement rule blocks adoption on this tie.
Production prompt and acceptance remain unchanged. The next eval should
increase task difficulty beyond one-file arithmetic and retain independent
API, regression, change-path, and recovery checks. See
`docs/milestones/CANDIDATE_REPORT.md`.

## Completed gates

- 2026-10-06 Milestone 18 — froze one opt-in benchmark guidance candidate,
  a reused development task, and two new held-out arithmetic tasks before
  30 matched full-mode runs. Baseline and candidate each verified 15/15,
  with 30 model calls and no retries. All protected paths and recovery
  checks passed. Strict development improvement was absent, so adoption
  remains blocked. The candidate is outside persisted production context
  provenance; no production prompt change was made. VERIFY/FULL, forwarding
  and negative scoring controls, and both review axes passed. Report:
  `docs/milestones/CANDIDATE_REPORT.md`.

- 2026-10-06 Milestone 17 — converted the retained M16 integer-overflow
  failure into a deterministic development eval at the public `equal_range`
  interface. Enumerated 126 sorted sequences and nine queries (1,134 cases
  per profile). The exact patch, wrapping-add, and saturating-add variants
  compile and pass visible tests but fail the independent boundary oracle
  in debug and release. The safe solution passes the full workspace/API
  tests in both profiles. The eval is part of FULL. Zero model calls;
  no claim that model proposals improved. Report:
  `docs/milestones/INTEGER_BOUNDARY_REPORT.md`.

- 2026-10-06 Milestone 16 — froze three new synthetic tasks, API consumers,
  protected files, hidden edge oracles, known solutions, and safe call
  accounting before live measurement. The unchanged production prompt
  verified 15/15 full and 14/15 serial runs, with one overflow failure and
  no retries. All 30 proposals parsed; all protected files remained intact.
  The failure recovered as failed. The corpus validator, oracle controls,
  VERIFY, FULL, and both review axes passed. The exploratory reference line
  was met; five samples per cell do not support a speed or general
  reliability claim. Report: `docs/milestones/HELDOUT_REPORT.md`.

- 2026-10-06 Milestone 15 — added one bounded pre-mutation malformed-output
  retry, a complete typed response example, safe error classes, and actual
  call/usage accounting. Strict parsing and acceptance remain unchanged.
  The final matched live batch verified 39/40 runs with 41 calls. One
  malformed response recovered through retry; one patch with a compile error
  was refused by acceptance and recovered as failed. The failed earlier
  batch, interrupted batch, pilots, and failed patch remain separate
  evidence. VERIFY: 887 passed, zero failed, 15 ignored. FULL passed.
  Unequal verified success prohibits a speed comparison. Report:
  `docs/milestones/LIVE_RELIABILITY_REPORT.md`.

- 2026-09-28 Post-MVP follow-up #55 — added ignore-gated release
  measurement T6 for the gateway composition from the CreateTask request
  write through the decoded Subscribe replay acknowledgment. The cursor is
  `after_seq: -1` so sequence-0 `created` is included and asserted on every
  sample. Three 100-sample runs on one Linux host measured p50
  358–402 µs and p95 439–526 µs; this is a report-only measurement with no
  new target or speed claim, separate from M13 T4. Report:
  `docs/milestones/POST_MVP_TTFR_REPORT.md`.

- 2026-09-28 Post-MVP follow-up #26 — general effect barriers are now
  journalled through the core Supervisor protocol and committed atomically
  with the effect-table projection. The internal test seam seeds durable
  node statuses; conflicting access sets are refused before dispatch.
  Recovery puts interrupted nonterminal node tasks in `Recovering`, resets
  unprepared `Running` nodes to `Pending`, preserves reconcilable prepared
  effects, and marks non-idempotent/unknown effects plus their nodes
  `UnknownAfterCrash`; terminal tasks retain their terminal status while
  those classifications are journalled. Process-kill coverage now covers
  prepared, remote return and committed seams; failed effect commits roll
  back the journal, task snapshot and effect row together. The unchecked graph
  constructor is test-only; no production validation token or scheduler
  call path exists yet. The legacy M12 direct store helpers are deprecated.
  Runtime integration still needs trusted graph validation, exact-operation
  authorization permits, and Supervisor-owned worker draining. Report:
  `docs/milestones/M12_REPORT.md`.

- 2026-09-25 Milestone 14 — MVP freeze: full spec §44 benchmark matrix
  (3 fixtures × 5 modes × n=10 = 150 driver runs + Class A/B composed
  legs)   through the descriptor-driven `bench_matrix` host (generalizes
  the M10 `auth_refresh` example); 150/150 verified success at equal
  full/reference rates, median completion full p50 318/287/360 ms vs
  serial 296/308/337 ms vs reference 155/144/148 ms (serial control
  fastest; scripted model ≈ 0.01 ms leaves the pipeline nothing to
  amortize), TTFR ties at 1 ms granularity (fe/edit 0–1 ms), verification dominates
  completion (~99%); §42 security escape + recovery fault-injection
  suites run explicitly as a gate incl. a structural no-TCP-exposure
  check with positive controls; two new fixtures (`multi-file-migration`
  Class D, `architecture-plan` Class E) with broken-first/solution
  self-check gates; M13 watch item closed — content-addressed text
  projection, warm queries read 0 corpus bytes over 478 files (100
  iterations), T5 re-measured 14.43→10.11 ms p50 (release transcript
  `perf[T5] n=100 p50=10.109617ms p95=10.587723ms PASS`); docs/11 decisions
  recorded (#2 TUI-first/ACP post-MVP, #1 kill criteria in MVP_REPORT,
  #11 pinned scripted provider); §45 dispositions 10 MET / 1 PARTIAL
  (performance claim stated as PARTIAL). Report: `MVP_REPORT.md`,
  artifact: `docs/milestones/M14_MATRIX.json`.

- 2026-09-25 Milestone 13 — Performance campaign: release-mode,
  ignore-gated harness (`tests/perf.rs` in eight crates) measuring all
  five spec §43 targets plus component baselines for every docs/04
  critical-path area. All five targets PASS on the first measured
  baseline: T1 router p95 1.65µs (<2ms), T2 scheduler dispatch p95
  34.8µs (<1ms), T3 gateway command p95 16.2µs (<5ms), T4 first visible
  task event p95 201.6µs (<50ms), T5 warm symbol/reference p50 14.4ms
  / p95 15.2ms (<250/<500ms). strace attribution: hot paths are tokio
  futex/epoll parking + ~1 fsync per WAL commit (≈80µs), no syscall
  pathology. Two measured 10ms poll latencies fixed on the named
  critical paths — process-runner `wait_for_exit` fast window (empty
  child 11.88ms → 1.91–2.85ms p50, ≈4–6×) and scheduler
  `wait_finished` fast window (verification run 22.47ms → 13.04ms
  p50, 1.7×); no other rewrites because no other area missed a target.
  Indexing re-reads the corpus per query (14.4ms warm = 6.0ms lookup +
  8.5ms search over 441 files) — documented as an M14 watch item with
  projection. One-off `cancellation_drains` reaped-assert failure
  root-caused by the expert board's executed disproof (test-side pid
  file empty-read TOCTOU, mechanism predates the PR; production ruled
  out via the drain-before-reap invariant chain; test fixed). Review:
  5-seat expert board (standards, spec, adversarial, concurrency,
  test-honesty) + disproof round — 0 confirmed blockers; majors all
  test/docs-layer and fixed in-round: optimization-pinning asserts
  (mutation-tested red), `scripts/perf_gate.sh` requires all five
  `perf[T*] PASS` markers (zero-run masking closed), GATES OWNS
  narrowed, scheduler poll delays named, report discloses
  single-repository + T4-ambiguity + T5-layer. Report:
  `docs/milestones/M13_REPORT.md`.

- 2026-09-24 Milestone 12 — Recovery hardening: env-gated fault points
  (`TACHYON_FAULT_POINT`, ADR 0001) at evidence/model/mutation/verify/
  approval seams; effect fixture writing `effects.state` prepared→
  committed with production `recover_task` §19 reconcile (Keyed/
  Queryable stay prepared; NonIdempotent/Unknown →
  `unknown_after_crash`); driver re-entry (ADR 0002): `Resume` on
  `Recovering` with pin respawns `start_run`, without pin lands
  `Paused`, continuation approvals use fresh ids; checked-in gateway
  SIGKILL restart test (`tachyon-app` kill_restart); six-domain seam
  gates; M11 restart_approval flip to Paused. Spec §42 seven-point
  matrix in `docs/milestones/M12_REPORT.md`. Tests: fault-point units,
  6 effect-fixture, 2 reentry, 6 seam gates, kill_restart, full
  workspace green; `fmt`, `check`, strict `clippy` clean. Deferred →
  follow-up: general effect journal protocol + node-level
  `UnknownAfterCrash`. Report: `docs/milestones/M12_REPORT.md`.

- 2026-09-23 Milestone 11 — TUI + live gateway events + run path:
  protocol v2 tagged `ServerFrame` with `Subscribe` streaming (ack +
  gapless journal replay, bounded queue with last-written resync),
  `tachyon-tui` Ratatui client (all nine docs/04 panes, honest empty
  states), CLI `attach` + `run`/`ps`/`pause`/`resume`/`cancel` aliases,
  operator provider config with redaction, supervisor-owned `StartRun`
  on the shared driver (example refactored onto it), five new journal
  kinds, approval wait with one-shot durable grants, run-held workspace
  lease with inner reuse + `workspace_busy` refusal, detected Cargo
  acceptance. Gate: disconnect/close leaves tasks untouched; reconnect
  replays gapless (protocol + TUI-client legs). Parent-fixed stale-handle
  `supervisor_gone` race with regression. G7: commit→frame n=200
  p50 ~259µs / p95 ~311µs (<50ms PASS; run-to-run variance, both this
  and the prior ~272/~362 run pass). Tests: 86 suites, 466 passed /
  0 failed default (474 all-features); `fmt`, `check`, strict `clippy`
  clean both feature sets. Plan r4 unanimous BUILD; code G8 R1:
  2×BUILD+2×CONDITIONAL+1×REJECT → ten blockers fixed or amended;
  R2: 2×BUILD+2×CONDITIONAL (closures landed: recovery-election
  ordering, stale pin-then-lease doc order, amendment wording), seat5
  pending. Report: `docs/milestones/M11_REPORT.md`.

- 2026-09-22 Milestone 10 — Full debugging task: provider-neutral core
  runtime joining evidence, model, recoverable mutation and supervisor-owned
  verification on the production supervisor path (bounded `fs.read`
  evidence with hash provenance, validated stage IR with lowered router
  placeholders, typed proposals with conjunctive hard-binding and
  `base_hash` gates before mutation, typed mutation intent, exact-hash
  approval binding, FNV-1a freshness with a pinned BLAKE3 boundary).
  One supervisor per (state-database, TaskId) with owned jobs, ack-after-
  drain steering, Recovering-not-Completed restarts, and the shared
  workspace lease held from baseline through rehash to the durable
  completion transaction (alias spellings share the key). Strict
  task-scoped recovery with zero writes on denied/corrupt/unknown input;
  aliased dependency selection resolves or broadens, never ignores.
  Gate: the checked-in broken auth-refresh fixture fails its regression
  first; the runtime applies the model-proposed repair via authorized M8
  mutation and selected M9 checks and reaches durable `Completed`
  (measured: full concurrency 4, serial 1, unknowns null, n=1 per mode;
  no speed claim). Wrong patch, Complete-alone, unknown capability,
  denied/escaped path, migration write, stale evidence and unbound
  constraints all refuse with zero writes; delayed-provider steering and
  real `exit(137)` crash recovery preserve identity and contract.
  Tests: full workspace 65 suites, 330 passed / 0 failed default
  (338 all-features); `fmt`, `check`, strict `clippy` clean.
- 2026-09-22 R1 board (5 seats): 1 BUILD + 4 CONDITIONAL with executed
  proof — pre-mutation gate enforced only the first hard binding,
  scopeless contracts authorized any path, mutation intent accepted
  arbitrary JSON, no full-mailbox burst barrier, unpinned FNV/BLAKE3
  boundary, undocumented example orchestration. Adjudication: all
  accepted as real. Fixes: conjunctive binding enforcement with
  regression, scopeless-write denial, typed `MutationIntent`, exact-hash
  foreign-grant regression, 600-sender burst barrier, portable socket-EOF
  liveness probe (no `/proc`), boundary pin and driver note.
  Competing-mutation and pending-ack barriers already held as unit tests.
- 2026-09-22 R2 board: unanimous BUILD — each fix verified by quoted
  source plus rerun outputs. Milestone 10 GATED. Report:
  `docs/milestones/M10_REPORT.md`.

- 2026-09-22 Milestone 9 — Verification-gated completion: `tachyon-verify`
  (typed `AcceptanceContract` with legacy fail-closed recovery, authorized
  source snapshots, Rust affected-first planning with reverse-dependent
  closure, validated verification IR through the scheduler, policy-bound
  `verify.command` on the resolved canonical cwd, fresh private evidence,
  serialized reports never re-authorize) plus supervisor-owned durable
  completion in `tachyon-core` (immutable baseline/contract binding,
  hard-constraint executable bindings, atomic journal/status/snapshot
  transitions, interruption without blind replay, post-report rehash) and
  the `tachyon-tools` process-ownership prerequisite (owned process groups,
  TERM grace then KILL, inherited-pipe deadlines, contained cwd, bound
  approvals, redact-before-spool). Gate: model-proposed wrong patch fails
  the real regression (`command exited Some(101)`) and refuses `Completed`;
  the corrected patch passes, persists `Completed`, and recovers it.
  Tests: 32 verifier (2 unit + 4 acceptance + 6 planner + 15 runner +
  5 snapshot), core 4 unit + 6 integration, store 4, tools 29;
  full workspace 53 suites green; `fmt`, `check`, `clippy -D warnings` clean.
- 2026-09-22 R1 board (5 seats): all REVISE with executed proof — cwd
  symlink alias bypassing the `verify.command` denial, cross-run workspace
  write-claim overlap via independent schedulers, scheduler-timeout grant
  release before worker cleanup, snapshot cadence reset across recovery,
  root metadata read without authorization, affected selection missing
  reverse dependents. Adjudication: all accepted as real. Fixes: canonical
  cwd resolution before authorization with invocation+resolved-scope approval
  binding and same-target execution; process-wide per-workspace async lease
  held through scheduler shutdown and worker drain; `snapshot_base`
  threaded separately from replayed journal position; root `fs.metadata`
  authorization before `is_dir`; reverse-dependency closure with
  conservative workspace broadening. 6 new regression tests.
  R2 verify-by-quote dispatched on the fixed tree.
- 2026-09-22 R2 board (5 seats): unanimous BUILD — each fix verified by
  quoted source plus executed regression and a novel scratch probe
  (nested alias chain, aliased-spelling lease contention, double recovery
  cadence, broad metadata/list denials, transitive a<-b<-c closure and
  dependency cycles); M9 wrong-patch/correct-patch gate re-confirmed
  6/6. Milestone 9 GATED.

- 2026-09-21 Milestone 8 — Mutation engine: `tachyon-mutation`
  (PatchSpec + base-hash guard, fsync'd batch journal with torn-tail
  repair, preimage retention in the artifact spool, temp staging beside
  targets, per-file rename with commit-time re-verification, finish or
  compensate recovery with divergence freeze, changed-file events).
  Vertical Slice C ("Fix the incorrect implementation") replaces the
  `==` token compare with constant-time equality end to end. Gate:
  crash injection between every file commit (k=0..=3) recovers coherent;
  stale preimages refused at prepare and at commit; 19 tests (8 unit +
  11 gate); `fmt`, `check`, `clippy -D warnings` clean.
- 2026-09-21 R1 board (5 seats): spec BUILD, security BUILD, API
  conditional BUILD, correctness HOLD, adversarial HOLD — both HOLDs
  with executed proof (symlink-cycle sweep hang, `contains` sweep
  deleting user files, poisoned batch denying siblings, aliasing dup
  paths, torn restore, vacuous empty completion, commit check-then-act).
  Adjudication: all findings accepted except race preventability
  (inherent to recoverable-not-atomic; post-rename detection added).
  Fixes: symlink-blind exact-pattern sweep keyed by relative temp path,
  per-batch isolation with `batch_errors`, canonical normalization,
  single-read prepare, retained postimage spool with temp re-staging,
  post-rename verify (`Diverged` abort), atomic restore via temp+rename,
  post-action sweep, lenient replay with `journal_gaps`,
  `UnknownBatch` plan cross-check, containment into `InvalidPath`,
  serde-stable reports, `MutationBatch` struct removed. R2 verify
  dispatched on the fixed tree.
- 2026-09-21 R2 board (5 seats): 4 BUILD (spec-fixes, security,
  API, adversarial) + 1 HOLD (correctness, 8 fresh executed bugs in
  the new code). Adjudication: 7 accepted as real (P1 same-ms temp
  collision, P2 compensated batch finished, P3 vacuous empty
  completion, P5 dotless restore temps, P6 rmdir of user dirs, P7
  unbound `post_artifact`, P8 sweep IO aborting recovery); P4
  (corrupt journal bricks finish) stays fail-closed per security +
  adversarial seats, docs rescoped to name the hand-repair path.
  Fixes: full-id temp names, finish refuses compensated batches and
  completes only non-empty all-`Committed`, `post_artifact` bound in
  plan check, dot-prefixed restore temps, rmdir removed, best-effort
  sweep with `sweep_errors`. 2 new gate tests (same-ms temps,
  no-resurrect); 21 tests (8 unit + 13 gate), full workspace gates
  green (46 suites). R3 verify dispatched.
- 2026-09-21 R3 board (3 seats): 2 BUILD (correctness re-probe of all
  7 Ps with executed proof, spec) + 1 narrow HOLD (adversarial:
  stale-descriptor resurrection via commit after compensate —
  re-applies and seals against future recovery, executed). Fix:
  `commit_up_to` refuses journaled-rolled-back batches with new
  non-retryable `MutationError::Compensated`; resume is fresh prepare
  only. Gate test `stale_descriptor_commit_after_compensate_refused`;
  22 tests (8 unit + 14 gate), full workspace gates green
  (46 suites). R4 verify dispatched.
- 2026-09-21 R4 single seat (correctness + spec): BUILD — resurrection
  fix re-probed with independent executed checks (stale commit refused
  `Compensated`, non-retryable, disk untouched, future recovery clean;
  fresh prepare on the same path still completes; no false
  `Compensated` on normal/partial flows), M8 Build items + Slice C +
  crash gate all green. Milestone 8 GATED: 22 tests (8 unit + 14
  gate), 46 workspace suites green.

- 2026-09-21 Milestone 7 — Judgment/OpenJEV: `tachyon-judgment`
  (provider-neutral `JudgmentProvider`, boolean/choice/score items with
  per-item certainty policies, batched resolution, capability-routed
  registry with outage fallback to evidence, `FakeJudgmentProvider`,
  `OpenJEV` adapter behind the `openjev` feature over plain-HTTP JSON,
  opt-in router bridge closing the M5 `JudgmentFirst` loop).
  A/B gate (synthetic fakes): 20 ambiguous requests avoid 14 model
  calls (6 vs 20) at equal 20/20 verified success. Default
  `JudgmentFirst` still resolves to evidence — real-workload A/B lands
  in M13/M14. Gate: 22 tests default (15 unit + 7 gate) + 30 with
  `openjev`; `fmt`, `check`, `clippy -D warnings` clean both ways.
  Review: R1 1 BUILD/4 HOLD (auth flags dismissed as redaction
  phantoms, one retracted after TCP capture); fixes — fail-closed
  certainty, OOB bounds, resolve fallback for all provider errors,
  policy-threaded bridge, source factory, 408/timeout mapping, bounded
  reads; R2 4 BUILD/1 HOLD (wire-confidence laundering); fix —
  non-finite confidence rejected at parse; R3 unanimous BUILD.

- 2026-09-21 Milestone 6 — Model layer: `tachyon-retrieval`
  (evidence structures with provenance, deterministic merge/rank),
  `tachyon-models` (provider-neutral `ModelProvider`, capability
  negotiation, role mapping, trusted context assembly with
  `WorkspaceData`-never-authority, structured `AgentDecision` with
  boundary repair, streaming event sink, `FakeModelProvider`,
  OpenAI-compatible HTTP adapter for local inference, plain-`http`
  only). Vertical Slice B ("Explain why these two implementations
  behave differently") answered in one reasoning call over repo
  evidence, citing both provenances. Gate: 34 models tests (29 unit +
  5 gate incl. Slice B) + 7 retrieval tests green; `fmt`, `check`,
  `clippy -D warnings` clean. R1 board (1 BUILD / 4 HOLD) adjudicated:
  auth-key flag dismissed as display-redaction phantom (byte-verified
  `{key}` interpolation, `grep -c` = 1); real findings fixed —
  converging marker-aware budget fitter, pinned-never-dropped, newest
  history survives, fail-closed trust, provenance-aware merge, NaN-safe
  ranking, `ContextOverflow` taxonomy, usage accounting, `Retry-After`
  support, CRLF rejection, role-carrying selection with request
  constructor. R2 verify-by-quote: 2 BUILD (spec, adversarial) / 3 HOLD.
  Real hang confirmed by 2 seats (fitter spun at marker floor when
  allowance saturated to 0 with non-empty pinned content) — fixed by
  making `truncate_block_chars` return `false` at identical-content
  floor plus `output_exceeding_total_terminates_with_pinned` regression
  test. API-seat auth HOLD dismissed: self-refuting (it reports 40/40
  green including the header-carries-key test, which can only pass if
  the key hits the wire) plus independent `od` verification. R2: 2
  BUILD / 3 HOLD; hang fix + regression test landed. R3 unanimous
  BUILD: all 3 HOLD seats flipped with quotes + live runs (API seat
  confirmed the `***` was display redaction via `od` comparison).

- 2026-09-21 Milestone 5 — Predictive router: `tachyon-router`
  (deterministic rule classification DirectNative/EvidenceFirst/
  JudgmentFirst/ReasoningFirst/Hybrid, candidate extraction with stoplist,
  EWMA-priced evidence plans, 75 ms grace window, serial mode,
  JudgmentFirst resolving to evidence until M7), `tachyon-telemetry`
  (bounded recorder, EWMA, route audit records). Gate: simple
  repo/search/git routes plan zero model calls; complex routes still
  launch evidence first; 7 router-gate + 2 telemetry tests green.

- 2026-09-21 Milestone 4 — Repository intelligence: `tachyon-repo`
  (walkdir inventory with BLAKE3 identity + prune rules, heuristic
  `LanguageBackend` for Rust/Python/TS/JS symbols, word-boundary reference
  index, deterministic lexical search, notify watcher as invalidation hints,
  hash-authoritative verify/refresh with generations). Vertical Slice A
  ("where is refreshToken defined and used?") answered with zero LLM calls
  in structured locations. Gate: 7 repo-gate tests green, full workspace
  gate green.

- 2026-09-21 Milestone 3 — Policy + native tools: `tachyon-policy`
  (capability/scope globs, trusted-workspace defaults, denials-first
  decisions, BLAKE3 canonical operation hashes, hash-bound approvals,
  real path containment with traversal/symlink rejection), `tachyon-tools`
  (capability registry, contained fs read/list/metadata/write, concurrent
  process runner with bounded inline + artifact spool + redaction,
  allowlisted read-only git, content-addressed artifact store with zstd
  above 64 KiB, credential-handle broker with output redaction). Gate:
  `fmt --check`, `check`, `test` (all suites green incl. 7 policy tests +
  11 tools-gate tests: local auto-allow, outside-write approval flow,
  deny posture, traversal/symlink escape, git allowlist, artifact
  roundtrip, secret redaction), `clippy -D warnings`.

- 2026-09-21 Milestone 2 — Execution IR + scheduler: `tachyon-ir`
  (validated DAG: identity/invocation/dataflow/bindings/purity/effects,
  conditional deps, cardinality, resource-key grammar + segment-based
  overlap, critical-path estimates), `tachyon-scheduler` (loop owns
  readiness, atomic conflict/resource grants, CP-priority scoring,
  retries/backoff, timeouts, structured cancellation, duration EWMA;
  `FakeExecutor` + `Tracker` for order/violation assertions). Gate:
  `fmt --check`, `check`, `test` (all suites green incl. 8 scheduler
  tests + 24-case proptest of conflict-freedom and dependency order),
  `clippy -D warnings`. Two real bugs found by testing and fixed:
  `JoinSet::join_next` on an empty set never pends (busy-spun the loop
  and starved commands — now guarded by `is_empty`); proptest spawned
  the loop outside a runtime (moved inside `block_on`).

- 2026-09-20 Milestone 1 — Durable task kernel: SQLite `state.db`
  (WAL/FULL/FKs/busy-timeout, single-writer `StoreWriter`, migrations),
  append-only journal + snapshots (every 100 events, terminal states),
  `TaskState`/supervisor actor (mailbox 256, journal-before-state,
  revision bumps, terminal discipline), gateway lifecycle (0700 dir,
  endpoint file, stale eviction, Unix socket, framed JSON dispatch),
  CLI client (`gateway`, `session create`, `task create/list/get/send/
  pause/resume/cancel`). Gate: `fmt --check`, `check`, `test` (21 passed,
  0 failed incl. gateway restart-recovery test), `clippy -D warnings`,
  plus live `kill -9` gate against the gateway binary (verified dead,
  stale endpoint evicted on restart): task recovered at rev 1 with same
  objective/status and continued to rev 2. (First attempt mistakenly
  killed the wrapper shell, leaving the gateway alive and the restart
  correctly refused with AlreadyRunning; redone against the binary.)

- 2026-09-20 Milestone 0 — Foundation: `tachyon-types` (UUIDv7 ids,
  RFC 3339 timestamps), `tachyon-protocol` skeleton (versioned
  request/event envelopes, LE length-prefixed JSON framing, 15-command set),
  config loading with precedence defaults < file < `TACHYON_*` env < CLI,
  tracing bootstrap (stderr, `--json`-safe stdout), `tachyon --version`,
  `tachyon doctor` (6 checks), `tachyon config`. Gate: `cargo fmt --check`,
  `cargo check`, `cargo test` (15 passed, 0 failed), `cargo clippy -D
  warnings`, `tachyon doctor` exit 0 — all pass on Rust 1.98.1.

- 2026-09-20 scaffold baseline: `cargo fmt --check`, `cargo check --workspace`,
  `cargo test --workspace` (35 suites, 0 tests — stubs), and
  `cargo clippy --workspace --all-targets -- -D warnings` all pass on Rust 1.98.1.
  One scaffold fix required: workspace `clippy::all`/`pedantic` lints needed
  explicit `priority = -1` for Rust 1.98 `lint_groups_priority`. Package
  SHA-256 manifest verified 61/61 files OK; zip sha256 matched.

## Measurements

The frozen MVP matrix is in `MVP_REPORT.md`. Live-model fixture measurements,
including failed and interrupted candidates, are in
`docs/milestones/LIVE_RELIABILITY_REPORT.md`. Gateway latency measurements
are in `docs/milestones/POST_MVP_TTFR_REPORT.md`.

## Blockers

None recorded.

## Architecture deviations

None.
