# Handoff — 2026-09-29 session: roast playbook (T0 landed, T1 paused at §2.3)

**Verification status at pause:** `cargo verify` green (EXIT=0). `cargo verify
full` has NOT been run against the T1 batch — see **FIRST ACTION NEXT SESSION**
below before trusting any gate result.

Current session state for a fresh agent. The 2026-09-26 session snapshot is archived at
[`docs/archive/HANDOFF-2026-09-26.md`](docs/archive/HANDOFF-2026-09-26.md). Current
development rules: [`AGENTS.md`](AGENTS.md) + [`docs/DEVELOPMENT_WORKFLOW.md`](docs/DEVELOPMENT_WORKFLOW.md).

## Mission

Execute the fix playbook from the full-coverage repo roast, in queue order:
**T0** honesty/gates batch (this session) → **T1** security (§2.1–§2.7, failing test first each)
→ **T2** correctness (§3.1–§3.9, one failing test per row) → **T3** architecture
(capability-enum unification → dead-crate ADR-0008 → ADR-0006 slice → benchmark instrumentation →
god-file splits). The playbook text lives in this session's conversation; the condensed queue is
below. Verification discipline after every batch: `cargo verify` green, then `cargo verify full`
for gate/script changes, plus one targeted mutation per new check (break it on purpose, confirm red).

## T0 state — DONE, COMMITTED AND PUSHED

T0 landed as `ee1953c` on `feat/supervisor-evidence-ir` (pushed to
`1deat0r/Tachyon-Agent`). Verification evidence: `cargo verify` green and
`cargo verify full` green (EXIT=0) — the FULL run regenerated
`target/m14/M14_MATRIX.json` at 150/150 verified success, and the new G11
check plus its mutation (break a report figure → red, restore → green)
were confirmed. The commit contains:

1. `PACKAGE_MANIFEST_SHA256.txt` deleted (zero references).
2. `HANDOFF.md` (2026-09-26 snapshot) archived → `docs/archive/HANDOFF-2026-09-26.md`.
3. `GATES.md` → `GATES.json`: structured ledger, every sha/evidence byte-verified against the
   original, host paths scrubbed (`cwd=.`), **G11 now has a real CHECK** →
   `node scripts/m14_reconcile_check.mjs`.
4. New `scripts/m14_reconcile_check.mjs` — reconciles report figures the G8 checker misses
   (first-evidence/task-wall pairs, 12 serial/reference p50s, verified-success rows, model_ms
   range, leg pairs), each scoped to its report section. Mutation-tested 4/4 red, restores green.
5. `scripts/m14_suites.sh` — **found and fixed a vacuous gate**: the Cargo.lock no-TCP regex
   required a quoted `"name"` key that never occurs in Cargo.lock, so it always passed. Now
   matches real `name = "..."` lines with exact server-framework names (socket2 excluded — tokio
   legitimately ships it), plus synthetic match-controls for both source and lock patterns.
   Source-scan mutation-tested red/green.
6. `scripts/m14_matrix.sh` — `M14_SAMPLES` guard: rejects non-numeric, empty (unset-only
   default), and zero; validated before any output truncation. All three rejections tested.
7. `SECURITY.md` — `TACHYON_FAULT_POINT` scope note (never in service units/CI without
   fault-injection intent). Optional `tracing::warn!` at arm time is deferred to T1 §2.7.
8. `.gitignore` — added `.unlazy/`.

Also in the same commit — the five "Remaining T0" items, all done:

- `CHANGELOG.md`: M14 TTFR corrected 32/108/115 → 1/1/1 ms, "150/150"
  qualified as scripted-provider harness overhead, Unreleased entry added.
- `README.md`: benchmark numbers framed as harness overhead; M4/M5/M7
  noted as built but not on the run path.
- `AGENTS.md`: Execution IR invariant marked **Target**, flip to Enforced
  when the ADR-0006 slice lands.
- 12 dead deps removed across 9 crates (core/tracing, mutation/tokio,
  repo/{serde_json,tachyon-types}, router/{serde,thiserror}, scheduler/serde,
  store/{serde_json,tracing}, telemetry/serde_json, tools/walkdir,
  tui/tachyon-store); `cargo check --workspace --all-targets` clean.
- Two-axis review of the commit ran; wording findings (CHANGELOG clock
  granularity, README ADR phrasing, SECURITY.md "seam" → CONTEXT.md "hold")
  were fixed in the follow-up below.

## After T0 (queue, from the playbook)

Done this session after T0:

- [x] Wire G7–G11 into `cargo verify full` (`67773b4`): `run_gate` now asserts
  each gate's own `expect` string, so a checker exiting 0 without checking
  fails the run. Mutation-tested: G7 (assert flipped) / G8 (section renamed) /
  G9 (README status regressed) / G10 (M14 line renamed) / G11 (figure changed)
  each red then green on restore, plus one end-to-end red run that stopped at
  G11 with G6→G3→G4→G5→G7→G8→G9→G10 green before it.
- [x] xtask runtime root discovery (`62731aa`): root is found at runtime from
  the executable, then cwd, then the compile-time path (last resort), and is
  printed as `root: …` on every run. Proven against a relocated checkout
  skeleton (`root: /tmp/moved-root`); reverting to compile-time-only printed
  the stale original path — the brick this fixes.

## T1 security — PAUSED MID-BATCH (§2.1–§2.3 done, §2.4/§2.5 open)

The verbatim playbook lives in the previous session's checkpoint:
`/home/ideator/.local/share/mimocode/memory/sessions/ses_ffe5f137805aeffe8oN63XlBNK/checkpoint-playbook.md`
(T1 §2.1–§2.7, T2 §3.1–§3.9 table, T3 steps 1–5). Read it before resuming —
this file only carries the condensed queue.

Landed in the working tree (commit this batch, see below):

- **§2.1(a)** — `tachyon-tools/src/process.rs` now inherits a fixed allowlist
  (`PATH HOME TMPDIR LANG SYSTEMROOT PATHEXT`) instead of `vars_os()`; test
  `crates/tachyon-tools/tests/secret_env_allowlist.rs` pads past `INLINE_CAP`
  so it fails on the *spool*, not just the inline body. Red → green verified.
- **§2.1(b)** — `ToolsContext::with_credentials` builder; the gateway's
  context is built by an extracted `run_tools_context(...)` in
  `tachyon-gateway/src/server.rs` that chains
  `.with_credentials(state.runtime.redactor.clone())`. Test
  `run_tools_context_carries_runtime_redactor`; removing the chain turns it red.
- **§2.2** — `file_hash` → `Result<Option<String>, MutationError>` (fail-closed
  on anything but `NotFound`) plus the same split inlined in
  `engine.rs::prepare_with_id` (it cannot call `file_hash` without reopening
  the verify-then-reread window). All 11 call sites updated.
  Tests: `crates/tachyon-mutation/tests/unreadable_preimage.rs` (2 tests).
- **§2.3** — `allow_insecure_remote` on `OpenAiCompatConfig`, loopback guard in
  `parse_http_url`, refusal raised at config **load** (`app/src/config.rs`);
  `stream.take(MAX_RESPONSE_BYTES)` bound, `Host: {host}:{port}`, error-body
  truncation. Tests: `plaintext_remote_base_url_fails_at_startup`,
  `loopback_plaintext_base_url_is_accepted`, `huge_response_read_is_bounded`.

**Deviation worth remembering:** the playbook's `{path:?}` in `file_hash` is
rejected by clippy's `unnecessary_debug_formatting` under the workspace's
`-D warnings`; it is `path.display()` instead. Repo lint wins over the
verbatim snippet.

**FIRST ACTION NEXT SESSION — do this before anything else:**
`cargo verify full`. The normal tier is green, but this batch changed how
children are spawned (§2.1(a) env allowlist), and the M14 matrix/fixture gates
spawn `cargo test` through that runner — if a cargo child needs `CARGO_HOME`,
`RUSTUP_*` or similar, those gates will now fail. If they do, the fix is to
carry the needed vars through `spec.env` at the call site, not to widen the
allowlist silently. Also mutation-test any gate that goes red.

Still open in T1:

- **§2.4** TUI frame caps — share `read_frame` in `tachyon-protocol` so
  `tui/src/reader.rs:526` and `command.rs:185` use one cap; test feeds a
  `0xFFFF_FFFF` prefix and asserts an error without a 4 GiB allocation.
- **§2.5** endpoint file — atomic publish (`endpoint.rs:113`), quarantine
  corrupt files and rebind unless the recorded pid is alive, `chmod 0700`
  *before* writing, four tests (file missing / truncated / two concurrent
  claimers / stale pid), optional `SO_PEERCRED` + one sentence in `docs/06`.
- **§2.7** optional — `tracing::warn!` once at fault-point arm time
  (`SECURITY.md` note already landed in T0).
- Deferred per the playbook: merging the two hand-rolled HTTP/1.1 clients
  into `tachyon-models/src/http.rs` (separate task).

Remaining queue:

- [1 day] **T2** rows §3.1–§3.9, one failing test each (verify_manifest_freshness wiring,
  model deadline timeout, release_parked Result, shutdown ordering, approval id compare,
  fault-kill marker handshake, shared token budget, spawn_blocking in accept loop,
  scheduler Reconcile + proptest budgets).
- [This week] ADR-0007 (§45 PARTIAL disposition) + ADR-0008 (dead-crate wire-vs-delete:
  router/repo/judgment/telemetry form a dead subgraph rooted at `tachyon-judgment` — only
  consumers are each other's tests).
- [Next] T3 Step 1 capability enum in `tachyon-ir` + conformance test → Step 2 → ADR-0006
  slice → benchmark instrumentation → god-file splits.

## Skills (installed this session)

All **38** `mattpocock/skills` installed project-level (`./.agents/skills/`, symlinked into
`.claude/skills/` etc.; both gitignored) and verified byte-identical to upstream `main` on
2026-09-29. `skills-lock.json` is tracked and was already at latest (unchanged). Refresh with
`npx skills@latest update -p -y`; inspect with `npx skills@latest list`.

## Suggested skills (invoke via Skill tool)

- `tdd` — T1/T2 are explicitly failing-test-first.
- `diagnosing-bugs` — for §3.x regressions and gate mutations that go red unexpectedly.
- `code-review` — review T0 commit and each batch since.
- `writing-for-agents` — if playbook items get promoted into AGENTS.md/skills.
- `handoff` — to refresh this file at session end.

## Environment notes

- Host: Omarchy (Arch) + Hyprland, user `ideator`. Repo: `/run/media/ideator/Projects/AI Agents/Tachyon Agent`.
- No secrets in this document. Never edit `/usr/share/omarchy/`; never commit/push unless asked.
