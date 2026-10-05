# Changelog

## Unreleased

- Milestone 16 frozen held-out baseline: three new bounded tasks with
  independent edge oracles, public API consumers, protected-file checks,
  committed input hashes, and complete live call records. The unchanged
  prompt verified 29/30 runs with 30 calls. The rejected integer-overflow
  patch and its test failure remain published. The exploratory reference
  line was met; this small baseline supports no speed or general reliability
  claim. See `docs/milestones/HELDOUT_REPORT.md`.

- Milestone 15 bounded live-model reliability: one malformed-output retry
  before mutation, a complete typed response example, safe failure classes,
  and actual model-call/usage accounting. The matched `auth-refresh` batch
  verified 19/20 full and 20/20 serial runs. Strict parsing and acceptance
  remain required. Failed candidates, interrupted trials, pilots, and the
  rejected compile-error patch remain published as eval evidence. See
  `docs/milestones/LIVE_RELIABILITY_REPORT.md`. Unequal verified success
  prohibits a speed comparison; the result covers one fixture and model.

- Provider adapter: HTTPS and incremental streaming behind the existing
  `HttpTransport` boundary (handoff priority 3). `https://` targets are
  now accepted and verified against the platform trust store plus any
  operator-supplied roots (`TcpHttpTransport::with_extra_root_pem`, for a
  private CA), while plaintext to a non-loopback host stays refused — the
  scheme is never changed in either direction. Responses are read
  incrementally: headers as soon as they arrive, server-sent events
  decoded event by event with each assistant fragment published to the
  `ModelEventSink` as it arrives (the first token no longer waits for the
  connection to close), and a whole-response reply still lands as one
  Delta so every sink consumer sees the same shape. Requests ask for
  `stream: true` with `stream_options.include_usage`, so usage rides the
  provider's final chunk and unavailable counts stay `Unknown` rather
  than being reported as zeroes; `provider.stream = false` in the config
  is the escape hatch for a server that rejects `stream_options`. Framing
  covers Content-Length, close-delimited and SSE bodies under one bound —
  a response past `MAX_RESPONSE_BYTES` is now a typed `Transport`
  refusal naming the bound instead of a silent truncation. New deps:
  `rustls` (ring only), `tokio-rustls`, `rustls-native-certs` — no
  server-framework crate enters the lock, so G6 stays green, and every
  license is already on `deny.toml`'s allow list. Real-socket tests in
  `crates/tachyon-models/tests/http_bounds.rs` cover the round trip, the
  bound, a first Delta arriving before the stream ends, `https://`
  routing through TLS, and a full handshake against a private CA.
- ADR-0006 supervisor-owned evidence execution: the production `fs.read`
  stage now runs as a Supervisor-owned generation instead of a driver-side
  pathname read. Typed requests are fenced by run and revision, every
  target is resolved, opened and proven beneath the pinned root *before*
  the graph is allocated (canonical re-proof after the open, handle/path
  identity match, regular-file proof), authorization runs against that
  exact opened object through a non-cloneable one-shot permit, and the
  trusted compiler mints the validated-graph proof only after structural
  validation plus the capability contract checks (read-only, pure,
  immediate cancellation, no retry, declared output, `contract_version`).
  Execution goes through `tachyon-scheduler` on Supervisor-owned workers
  bound to a child of the host cancellation token, under one shared
  linearizable stage byte budget whose exhaustion cancels siblings and
  returns no partial batch. Success is journalled with a durable
  content-addressed receipt in the same transaction; retrieval is
  receipt-scoped and BLAKE3-verified before bytes reach the model. A crash
  mid-read journals a generation-interrupted event, cancels that
  generation's unfinished nodes, clears the active pointer and never
  reuses the generation number. Gates: `evidence_generation.rs`,
  `evidence_crash.rs` and the `evidence.rs` unit tests, plus
  `cargo verify full` at 150/150 verified with concurrent cells measuring
  4–6 overlapping evidence nodes on this path.
- Repository intelligence and deterministic routing on a real user
  request: new `Command::Query` + `tachyon query "<question>"`. The
  gateway routes with `tachyon-router`, indexes the workspace fresh with
  `tachyon-repo` (inventory scan → `SymbolIndex::build`, content hashes
  authoritative), and returns definitions and references as source
  locations with `model_calls: 0`. Zero model calls is structural: the
  handler never touches a provider, and a route that would need one (or a
  judge) is refused with `requires_model` instead of being degraded into a
  lookup — proven by running it on a gateway configured with no provider
  at all. Adds `requested_symbol` to the router so a plain-word question
  ("where is serve defined") binds a symbol through the question's cue
  when the classifier finds no `CamelCase`/`snake_case` candidate. CLI
  renders a readable answer (bounded); `--json` keeps the raw payload.
- Moved the bounded HTTP read test out of `openai_compat`'s inline test
  module into `crates/tachyon-models/tests/http_bounds.rs`, exposing
  `MAX_RESPONSE_BYTES` and `round_trip` for it: G6 forbids a TCP listener
  anywhere in shipped `crates/*/src`, and that gate was red at `9bdf97f`.
- Repo-roast honesty/gates batch: `GATES.md` → `GATES.json` (structured
  ledger, bytes verified against the original, host paths scrubbed) with
  G11 gaining a real check (`scripts/m14_reconcile_check.mjs`, which
  reconciles report figures the G8 checker misses); fixed a vacuous
  no-TCP gate whose Cargo.lock regex matched a key that never occurs
  there, plus `M14_SAMPLES` input validation in `scripts/m14_matrix.sh`;
  README/CHANGELOG/invariant wording aligned with what is actually
  measured; 12 declared-but-unused crate dependencies removed.
- Milestone 14: MVP freeze — full spec §44 benchmark matrix (3 fixtures ×
  5 modes × n=10 = 150 driver runs plus Class A/B composed legs) through
  the descriptor-driven `bench_matrix` example; 150/150 verified success
  at equal full/reference rates — pinned scripted provider, so this is
  harness/path correctness, not live-model performance; median TTFR full
  p50 1/1/1 ms (at the 1 ms clock's granularity, so cross-mode reads are
  ties; the superseded 2026-09-25 artifact read 32/108/115 ms on warmer
  page cache);
  explicit §42 security + recovery suite gate with a structural no-TCP
  exposure check; new Class D (`multi-file-migration`) and Class E
  (`architecture-plan`) fixtures with broken-first self-check gates;
  content-addressed text projection closes the M13 corpus-re-read watch
  item (warm queries read 0 bytes; T5 14.43 → 10.11 ms p50). §45
  dispositions 10 MET / 1 PARTIAL; kill criteria, docs/11 decisions
  (#2 TUI-first, #11 pinned scripted provider) and deferred work recorded
  in `MVP_REPORT.md`; aggregate artifact `docs/milestones/M14_MATRIX.json`.
- Milestone 13: performance campaign — release-mode harness for all
  five spec §43 targets (router path, scheduler dispatch, gateway
  command, first visible task event, warm symbol/reference) plus
  component baselines for the eight critical-path areas; all five
  targets pass with headroom. Fixed two measured 10 ms poll latencies
  on the process/verification critical path (`wait_for_exit` fast
  window: empty child 11.88 ms → 1.91–2.85 ms p50; `wait_finished`
  fast window: verification run 22.47 ms → 13.04 ms p50). Numbers,
  strace attribution and findings in `M13_REPORT.md`.
- Milestone 12: recovery hardening (env-gated fault points, effect
  fixture with §19 reconcile, driver re-entry / fresh-id re-ask,
  gateway SIGKILL restart test, six-domain seam gates) with the §42
  seven-point coverage matrix in `M12_REPORT.md`.
- Bare `tachyon` (no subcommand) opens the TUI (`attach`) instead of
  printing help, matching spec §38's first-class `tachyon` command.
- Milestone 11: TUI + live gateway events + run path (protocol v2
  streaming subscriptions, Ratatui client with all nine panes, `attach` +
  run aliases, operator provider config with redaction, supervisor-owned
  `StartRun` on the shared driver, five new journal kinds, approval wait
  with one-shot grants, run-held workspace lease with `workspace_busy`
  refusal, Cargo acceptance detection) with the disconnect/reconnect gate;
  plan board r4 unanimous BUILD, code board pending.
- Parent fix: stale supervisor handles after run completion no longer
  surface transient `supervisor_gone` (recover-once + regression test).

- Milestone 10: full debugging task (provider-neutral core runtime over
  evidence/model/mutation/verification, supervisor ownership with
  ack-after-drain steering, shared workspace lease through durable
  completion, authorized mutation with scoped recovery, measured
  auth-refresh benchmark) with R1/R2 code boards unanimous BUILD.

- Milestone 9: verification-gated completion (typed acceptance contracts,
  authorized source snapshots, affected-first Rust planning with
  reverse-dependent closure, validated verification IR through the
  scheduler, policy-bound commands on the resolved canonical cwd,
  process-wide per-workspace execution lease, supervisor-owned durable
  completion with atomic journal projection) with the wrong-patch/fixed-patch
  gate end to end.

- Milestone 8: mutation engine (hash-guarded patch specs, durable
  batch journal, preimage retention, per-file atomic commits,
  finish-or-compensate recovery, changed-file events) with Slice C
  fixing the incorrect implementation end to end.

- Milestone 7: judgment layer (provider-neutral boolean/choice/score
  batches, certainty policies, outage fallback, fake provider,
  feature-gated OpenJEV adapter, opt-in router bridge) with a synthetic
  A/B showing 14 avoided model calls at equal verified success.

- Milestone 6: model layer (provider-neutral requests/decisions,
  capability negotiation, role mapping, trusted context assembly,
  fake provider, OpenAI-compatible local adapter) and evidence
  structures with deterministic merge; Vertical Slice B answered in
  one reasoning call.

- Milestone 5: predictive router (deterministic classification, EWMA
  estimates, 75 ms evidence grace window, serial mode) and route telemetry.
- Milestone 4: repository intelligence (BLAKE3 inventory, heuristic
  symbol/reference index, lexical search, watcher invalidation) with
  Vertical Slice A answered zero-LLM.
- Milestone 3: capability policy (scope globs, trusted defaults,
  hash-bound approvals, path containment) and native tools (contained fs,
  process runner, read-only git, artifact spool, credential broker).
- Milestone 2: validated execution IR and conflict-aware DAG scheduler
  (readiness, atomic grants, critical-path priority, retries, timeouts,
  cancellation) with property tests; docs-freshness tripwires in CI.
- Milestone 1: durable task kernel (SQLite journal + snapshots, supervisor
  actor, gateway lifecycle, CLI) with live kill-9 recovery gate.
- Milestone 0: foundation types, protocol framing, config precedence,
  `tachyon doctor`.
- Initial Tachyon architecture and implementation handoff scaffold.
