# Tachyon improvement recommendations — September 30, 2026

Focus the next milestone on becoming a dependable daily driver. Tachyon's
strongest differentiator is verified repository work with safe recovery. The
largest gaps are connecting the existing components and proving their value on
real tasks.

## Recommended priority order

1. **Implement ADR-0006 first.** The design accepted on September 29 addresses
   the production evidence path: Supervisor-owned execution, authorization bound
   to the opened file, shared byte budgets, cancellation drain, and durable
   output receipts. Make crash, cancellation, and stale-result tests the release
   gate. See [ADR-0006](../adr/0006-supervisor-owned-evidence-execution.md).

2. **Connect repository intelligence and deterministic routing to actual user
   requests.** The README says these components are built but unused by the
   production run path. Prove that a CLI/TUI request such as “Where is this
   symbol defined and used?” returns fresh source locations with **zero model
   calls**. Leave judgment routing optional until measured results justify it.

3. **Improve the provider adapter.** It currently supports plaintext HTTP and
   delivers the answer after the full response arrives. Add HTTPS and
   incremental streaming behind the existing provider boundary, with bounded
   responses, cancellation, and accurate usage reporting. This expands practical
   model access and improves responsiveness.

4. **Add a realistic live-model evaluation suite.** The MVP's 150/150 successful
   runs validate scripted execution; they do not establish reasoning quality or
   an end-to-end speed advantage. Start with roughly 20 real repository tasks
   covering lookup, diagnosis, repair, and multi-file changes. Grade resulting
   files, required checks, and constraint preservation. Compare harness modes
   using the same model and environment, recording success, interventions,
   latency, and cost. Recent primary-source guidance supports outcome-based
   evaluation and controlling infrastructure confounders:
   [agent evaluations](https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents)
   and [infrastructure effects](https://www.anthropic.com/engineering/infrastructure-noise).

5. **Deliver ACP through the existing gateway.** ADR-0005 already establishes
   the direction. Prioritize durable session replay, exact-operation approvals,
   cancellation acknowledgement after worker drain, and reconnect without
   duplicate execution. Keep unsupported capabilities unadvertised. See
   [ADR-0005](../adr/0005-acp-v1-gateway-distribution.md).

## Dogfooding and scope

Alongside this, dogfood Tachyon on your own maintenance tasks and record every
manual rescue. Use those observations to choose UX improvements—especially
clearer evidence, failed-check explanations, and recovery controls.

Keep browser use, swarms, workflow compilation, and marketplace work deferred.
The next milestone should demonstrate useful work through the real client, with
a real model, and recover correctly when interrupted.

## Supporting research and implementation status

The dated primary-source research is saved in
[the research note](../research/2026-09-30-improvement-evidence.md).
These are recommendations, not completed implementation work. No implementation
code was changed as part of this assessment.

## Status — end of 2026-09-30 session

Priority 1 is **done and verified**: ADR-0006 (Supervisor-owned evidence
execution) is implemented in the working tree.

- `crates/tachyon-core/src/evidence.rs` — target preparation and proof,
  the trusted `fs.read` compiler, the one-shot permit, the shared
  linearizable stage byte budget, the scheduler-backed executor, receipt
  assembly and receipt-scoped retrieval, plus the `Loop` handlers.
- `crates/tachyon-core/src/lib.rs` — `execution_generation`,
  `next_execution_generation` and `evidence_receipts` on `TaskState`, and
  the three generation events (`ExecutionGenerationAccepted`,
  `EvidenceGenerationCommitted`, `GenerationInterrupted`), one durable
  transaction each. `ValidatedExecutionGraph::try_mint` is now the only
  production proof constructor.
- `crates/tachyon-core/src/driver.rs` — supervisor hosts collect through
  `SupervisorHandle::collect_evidence`; reference hosts keep the direct
  reader.
- `Invocation::contract_version` (tachyon-ir) and `outputs` on
  `tachyon_scheduler::TaskRunSnapshot`.

Release gate (crash / cancellation / stale, as asked for in ADR-0006):
`crates/tachyon-core/tests/evidence_generation.rs` (9 tests),
`crates/tachyon-core/tests/evidence_crash.rs` (SIGKILL at the
`evidence.read` seam, recover, re-enter under a new generation), and the
unit tests at the bottom of `evidence.rs`.

**Verification:** `cargo verify` EXIT=0 and `cargo verify full` EXIT=0 —
150/150 verified M14 samples, concurrent cells measuring 4–6 overlapping
evidence nodes on the new path, all report gates green.

**Also fixed (pre-existing, not part of ADR-0006):** G6's no-TCP source
gate was red at `9bdf97f` because `openai_compat`'s inline test bound a
loopback listener in shipped `src/`. Moved to
`crates/tachyon-models/tests/http_bounds.rs`.

**Not committed** — the working tree holds all of it; see the diff.

## Status — priority 2 also done

**Priority 2 is done and verified**: repository intelligence and
deterministic routing now answer a real user request.

- `Command::Query` (tachyon-protocol, additive inside protocol v2) →
  `crates/tachyon-gateway/src/query.rs` → `tachyon query "<question>"`.
- The gateway routes with `tachyon-router`, binds the symbol
  (`tachyon_router::requested_symbol`, new — cue fallback for plain-word
  symbols the classifier does not candidate), indexes the workspace fresh
  with `tachyon-repo`, and returns definitions/references with
  `model_calls: 0`.
- Zero model calls is **structural**: the handler never touches a
  provider, and `requires_model` refuses any route that would need one or
  a judge. `crates/tachyon-gateway/tests/query.rs` runs it on a gateway
  configured with *no provider at all*; `cli_surface.rs` does the same
  end to end through the CLI.
- Judgment routing (M7) left optional, as asked.

Priorities 3–5 above are untouched.

## Status — priority 3 done (same day)

**Priority 3 is done and verified**: the provider adapter speaks HTTPS
and streams.

- `https://` is a first-class scheme (`parse_url` → connect, plain or
  TLS); rustls pinned to the `ring` provider (no C toolchain, and every
  new crate is already on `deny.toml`'s allow list — G6's no-TCP closure
  gate stays green). `TcpHttpTransport::with_extra_root_pem` trusts an
  operator-supplied root for a private CA.
- One incremental reader replaces read-to-EOF: headers as they arrive,
  then Content-Length / close-delimited / SSE framing under a single
  `MAX_RESPONSE_BYTES` cap counted over head+body. Past the bound is a
  typed refusal naming the bound, not a silent truncation.
- SSE chunks decode as they arrive and each assistant fragment goes
  straight to the `ModelEventSink` — the first token no longer waits for
  the connection to close. A whole response still publishes one Delta, so
  the sink shape is identical to the fake provider.
- Requests ask for `stream: true` + `stream_options.include_usage`, so
  usage rides the final chunk and absent usage stays `Unknown` rather
  than a reported zero. `provider.stream = false` is the escape hatch.
- Evidence is real sockets: `tests/http_bounds.rs` proves the round
  trip, the bound, a first Delta arriving before the server's 300 ms
  pause, `https://` routing through TLS, and a full handshake against a
  private CA the test supplied.

**Also fixed today (pre-existing, not part of the priorities):** Windows
CI had been red since `9bdf97f`. Three commits, and **all three platforms
are green as of `1db5586`** (run 36660359612):

- `ccd19ac` gave nested `cargo` the toolchain homes
  (`CARGO_HOME`/`RUSTUP_*`) plus the Windows temp/user locations at the
  verification-check call site. **Confirmed**: Windows now reports
  `happy_path_repairs_via_production_supervisor ... ok`, which is the
  test that was dying on `link: missing operand after '\377\376'`.
- `f009af1` widened `INHERITED_ENV_KEYS` with platform *locations*
  (`TEMP`, `TMP`, `USERPROFILE`, `APPDATA`, `LOCALAPPDATA`,
  `SystemDrive`, `ComSpec`, `windir`) — defensible on its own (a Windows
  child without `TEMP` falls back to the Windows directory, which a
  non-admin cannot write) and the build-tool opt-in shrank to the three
  toolchain homes. **Not** what unblocked the remaining test: its commit
  message claims a causal link to `responsive_actor` that was never
  established.
- `1db5586` widened the readiness window in
  `responsive_actor::cancel_acknowledges_after_real_reap...`
  (20 s → 60 s for a PowerShell sleeper that writes its PID *then* sleeps
  60 s; command timeout 30 s → 120 s). **This is what turned the lane
  green.** It measures runner speed, not the code under test.

Lesson worth keeping: `rustup target add x86_64-pc-windows-msvc` lets you
*compile* cfg(windows) code locally, but the C dependencies (blake3,
ring, zstd, sqlite) need MSVC to build, so it stops at the build scripts.
A windows repro is the only way to settle Issue #31-style failures.

Priorities 4–5 above are untouched: 4 needs real model credentials I do
not have, 5 is ADR-0005's gateway surface.
