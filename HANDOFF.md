# Handoff — 2026-09-29 session: roast playbook execution (T0 landed)

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

- [30m] Wire G8/G9/G10 + G7 runner into `cargo verify full` (xtask), mutation-test each.
- [1h] xtask runtime root discovery (unbricks gates on moved checkouts).
- [3–4h] **T1 security**: write the redaction regression test first (TACHYON_TEST_SECRET=hunter2
  must not reach receipt/inline body/artifact spool) → env allowlist in
  `crates/tachyon-tools/src/process.rs:92` + pass the populated `CredentialBroker` from
  `tachyon-app/src/config.rs::gateway_runtime()` into ToolsContext instead of `default()`
  (`tools/src/lib.rs:93/116`); `file_hash` → `Result<Option<String>>` (fail-open shape of
  `scoped.rs:281`); TUI frame caps via shared `read_frame` in `tachyon-protocol`; loopback guard
  in `openai_compat.rs::parse_http_url`.
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
