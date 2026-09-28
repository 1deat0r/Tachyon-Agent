# MVP report — Tachyon M14 freeze

**Date:** 2026-09-25 · **Issue:** #10 · **Branch:** `feat/m14-mvp-freeze` · **Base:** `84d02e3`
**Artifact:** `docs/milestones/M14_MATRIX.json` (aggregate; raw transcript in `target/m14/`, not committed)

## Method

- **Matrix:** 3 checked-in fixtures × 5 spec §44 modes (`full`, `no-speculation`,
  `no-judgment`, `serial`, `reference`) × **n=10** samples per cell = 150 driver runs,
  plus the two composed legs (Class A n=50, Class B n=20), all in release mode
  (`scripts/m14_matrix.sh`, ledger gate G4; validated by `scripts/m14_matrix_check.mjs`, G5).
- **Fixtures:** `auth-refresh` (Class C, the M10 canonical fixture), `multi-file-migration`
  (Class D, new — two drifted tax implementations), `architecture-plan` (Class E, new —
  extraction under an architecture constraint). Each ships `bench.json` + a solution under
  `fixtures/solutions/`; gate G3 proves broken-first fails, the solution repairs, protected
  paths stay byte-identical, and exactly `change_paths` changes — in a scratch copy, never
  the checked-in tree.
- **Host:** `crates/tachyon-core/examples/bench_matrix.rs` (descriptor-driven generalization
  of the M10 `auth_refresh` example, which it replaces) driving the ONE production driver.
  Scripted proposals prove runtime integration and verification, never model reasoning.
- **Configuration (docs/11 #11):** pinned scripted provider `bench-script-<fixture>` /
  model `scripted-replay-1` — identical model across every mode by construction
  (AD-015 same-model rule). No live model IDs in the MVP matrix. Verification risk `Affected`.
- **Percentiles:** nearest-rank, `p50 = sorted[n*50/100]`, `p95 = sorted[min(n*95/100, n-1)]`.
  At n=10 the p95 is the maximum sample — tails below are honest maxima, not smooth quantiles.
- **Machine:** AMD Ryzen 7 5800X (8C/16T), 30 GB RAM, Linux 7.0.0-31, ext4, Rust 1.98.1,
  release profile, local single-user machine (same host as M13).
- **Definitions:** *TTFR* = time from driver origin to first committed edit (the first useful
  artifact of a repair task). *Completion* = final verification (supervisor runs) or the end
  of the host-owned verification tail (reference). *First visible progress* = first evidence
  completion (`first_evidence_ms`); client-visible frame latency is M13's T4, measured on the
  gateway transport separately.

## Verified-success comparison

| Fixture | Class | full | no-spec | no-judg | serial | reference |
|---|---|---|---|---|---|---|
| auth-refresh | C | 10/10 | 10/10 | 10/10 | 10/10 | 10/10 |
| multi-file-migration | D | 10/10 | 10/10 | 10/10 | 10/10 | 10/10 |
| architecture-plan | E | 10/10 | 10/10 | 10/10 | 10/10 | 10/10 |

- **150/150** driver runs verified successfully (every cell rate 1.0), plus both legs.
- Every run additionally proved: broken-first regression failed before the repair,
  protected paths byte-identical after, observed change set equal to `change_paths`,
  and (supervisor runs) durable `completed` recovered as `recovered_completed`.
- **full vs reference: verified success equal** (1.0 vs 1.0) — no regression, satisfying the
  first clause of §45's exit condition. The speed clause is **PARTIAL** — see below.
- Negative controls (wrong patch, denied write, unknown capability) stay with the M9/M10
  gates and are re-run in gate G6; the matrix only measures successful repair paths.

## Median and p95 TTFR and completion

`full` mode (p50/p95 ms, n=10 per cell):

150/150 driver runs verified successfully.

| Fixture | completion | TTFR (first edit) | first visible progress | task wall |
|---|---|---|---|---|
| auth-refresh | 318/374 | 1/1 | 0/0 | 319/375 |
| multi-file-migration | 287/311 | 1/2 | 0/1 | 287/311 |
| architecture-plan | 360/431 | 1/2 | 0/0 | 361/432 |

Controls, p50 (completion / TTFR):

| Fixture | serial (p50) | reference (p50) | full vs serial | full vs reference |
|---|---|---|---|---|
| auth-refresh | 296 / 1 | 155 / 0 | completion within noise, TTFR equal | slower completion, **equal TTFR at 1 ms granularity** |
| multi-file-migration | 308 / 1 | 144 / 0 | faster completion, TTFR equal | slower completion, TTFR equal |
| architecture-plan | 337 / 1 | 148 / 0 | completion within noise, TTFR equal | slower completion, TTFR equal |

- The alias modes track `full` within noise (all alias cells coincide with
  `full` by construction, and every alias sample carries `coincides_with: full`)
  and report `coincides_with: full` in every sample — there is no speculation or judgment
  stage in the MVP driver to disable.
- **Honest reading:** with a *scripted* model (call cost ≈ 0.01 ms), the pipeline's overlap
  advantage has nothing to amortize, so the supervisor path's journal + verification-tail
  overhead shows up in raw wall time. Every mode verifies 150/150 at equal verified
  success, but on wall-clock completion the serial control is fastest on all three
  fixtures — **PARTIAL**, not a general speed claim (see Known limitations; §45 row 11).
  TTFR at 1 ms granularity cannot separate the modes on these tiny fixtures (see below):
  every full-vs-reference TTFR cell reads 1 ms vs 0 ms — a single clock tick, not a win.
- Completion tails (e.g. architecture-plan p95 431 ms) are real measured maxima at n=10,
  not sustained latencies.

## Critical-path breakdown

p50 stage shares of `full` completion (from the same samples):

| Stage | auth-refresh | multi-file-migration | architecture-plan |
|---|---|---|---|
| startup → first evidence | 0 ms | 0 ms | 0 ms |
| model (scripted invoke, measured) | 0.009 ms | 0.007 ms | 0.009 ms |
| evidence → first committed edit | 1 ms | 1 ms | 1 ms |
| edit → final verification (cargo test) | 317 ms | 286 ms | 359 ms |
| **completion p50** | **318 ms** | **287 ms** | **360 ms** |

- **Verification dominates** (≈ 99 % of completion, derived as
  completion p50 − first-edit p50 per fixture): one `cargo test --offline
  --locked` through the policy-bound process runner. Mutation (fsync'd
  journal + rename) plus the truncated sub-millisecond evidence/model
  prefix make up the remaining ~1 ms.
- **TTFR granularity caveat (new artifact):** `first_evidence_ms` and
  `first_edit_ms` both read 0–1 ms on every cell of the new run — the ms
  clock cannot resolve evidence/model/mutation apart on fixtures this
  small, so TTFR comparisons between modes are ties, not wins. The
  previous artifact resolved 5–20 ms / 32–124 ms on the same fixtures
  (identical driver code, warmer page cache); both runs agree that these
  stages are ≤ 1 % of completion and that cargo-test verification is the
  critical path. A microsecond-resolution stage breakdown is deferred
  work, not a freeze requirement.
- Evidence runs **concurrently** in `full` (max overlap 4/5/3 across fixtures) and
  serially in `serial`/`reference` (measured 1) — the mode switch behaves as specified.
- Composed legs (route → repo, fixture corpus of 15 files, n=50/20):
  Class A 31/40, Class B 34/39 µs p50/p95 (one scripted call for B).
  M13's micro legs still frame the
  transport/router edges: T1 route p95 1.65 µs, T3 gateway command p95 16.2 µs, T4 first
  frame p95 ~202 µs.
- **Post-projection re-measure** of M13's T5 (warm symbol/reference over this workspace,
  478 files): p50 10.11 ms / p95 10.59 ms PASS — improved from M13's 14.43/15.15 ms now
  that warm queries no longer re-read the corpus (G7). Remaining cost is the in-memory
  scan itself, not I/O.
- A single end-to-end client-visible composition (task creation → first replayed journal
  frame, M13's T4 t0 ambiguity) is still not built — deferred, see Known limitations.

## Model, Jev and tool calls

- **Model calls:** exactly 1 per run in every cell (p50 = p95 = 1), scripted
  `bench-script-<fixture>` / `scripted-replay-1`, measured invoke duration
  p50 0.005–0.010 ms across cells. Leg A: 0 calls (asserted
  `plan.requires_model() == false` on all 50 samples; no provider
  constructed). Leg B: exactly 1 call (asserted `request_count() == 1`),
  answer cited both source paths every sample.
- **Judgment/Jev calls: 0** in every sample — the MVP driver has no judgment stage;
  `no-judgment` is a measured alias of `full`. Judgment is reachable through the M7
  registry but is not on these paths.
- **Tool calls:** evidence reads + mutation prepare/commit per run — 7 (auth-refresh),
  8 (multi-file-migration), 6 (architecture-plan) — plus 1 verification subprocess per run
  (counted separately). Legs: 2 repo operations (definition_use + lexical_search) for A,
  2 file reads + 1 definition_use + 1 assemble for B.
- **Tokens:** usage provenance `scripted`, counters 0/0 — the fake does not fabricate
  token counts. Monetary cost: none (no live provider).
- **Speculative work:** started/used/discarded = 0/0/0 — speculation policy is
  `Forbidden` in the MVP runtime (spec §14: no speculative mutation).
- **Retries 0, provider failures 0, verification failures 0, user interventions 0** across
  all 150 runs (trusted-workspace policy auto-allows every fixture operation; a parked
  approval would surface as a driver error and none occurred).

## Known limitations

1. **Scripted model, no live latency.** The official MVP configuration (docs/11 #11)
   measures the harness, not model quality or provider wait. The concurrency advantage of
   `tachyon-full` cannot materialize when the model call costs ~0.01 ms; a live-model leg
   (pinned model ID + date) is deferred and would change these comparisons.
2. **n=10 per cell.** Nearest-rank p95 at n=10 is the maximum sample; tails above are
   maxima. p95-grade conclusions need n ≥ 20 on a quiet machine.
3. **Small synthetic fixtures.** Three checked-in fixtures (15–20 files each) plus legs on
   one 15-file corpus. Representative of Classes C/D/E shapes, not of large real repositories.
   T5's 478-file workspace corpus is the only large corpus measured (single repository —
   disclosed in M13 as F8, unchanged here).
4. **No end-to-end client-visible composition in the frozen matrix.** Driver-level
   `first_evidence_ms` stands in for first visible progress; gateway transport legs come from
   M13's separate T3/T4 measurements. The task-creation→first-replayed-entry composition was
   measured post-MVP on 2026-09-28; see
   [`docs/milestones/POST_MVP_TTFR_REPORT.md`](docs/milestones/POST_MVP_TTFR_REPORT.md).
   That follow-up does not alter the frozen M14 matrix or establish a `tachyon-full` speed
   advantage. At 1 ms granularity the driver stage markers additionally cannot resolve
   evidence/model/mutation apart on fixtures this small (see the TTFR caveat above);
   cross-mode TTFR reads are ties, and the completion comparison is the load-bearing one.
5. **Alias modes are structural.** `no-speculation`/`no-judgment` prove the modes are
   accepted and equivalent; they cannot show a difference until speculation/judgment stages
   exist on these paths.
6. **Projection memory cost.** Warm-query I/O was bought with an in-memory text projection:
   the index holds indexed file text for the corpus lifetime (bounded by inventory size).
7. **Class A phrasing.** The leg asks “Where is complete_refresh defined and used?” because
   that symbol exists in `fixtures/auth-refresh` (the M4 synthetic fixture, not checked in
   here, holds `refreshToken`). Same class, same zero-LLM contract.
8. **M13 cold-scan outlier unreproduced.** The one-off 97.7 s `comp[index_cold]` from M13
   did not recur (first-touch disk artifact); still recorded, never explained.

## Deferred work

- **After this gate only (docs/04 M14):** workflow compilation, browser/computer-use,
  distributed workers, multi-agent specialization — all still out of scope per spec §46.
- **Issue #26** (general effect-barrier journal protocol + node-level `UnknownAfterCrash`)
  — `ready-for-agent`, deliberately behind the M14 milestone order.
- **ACP distribution** — decided docs/11 #2: TUI-first for MVP, ACP adapter post-MVP.
- **Live-model benchmark leg** with pinned model IDs + dates (docs/11 #11 future runs) and
  an external-harness comparison under identical model/environment (spec §44, “where practical”).
- **End-to-end TTFR composition** — measured after the M14 freeze on 2026-09-28; see
  [`docs/milestones/POST_MVP_TTFR_REPORT.md`](docs/milestones/POST_MVP_TTFR_REPORT.md). This
  adds no latency target and does not change the §45 disposition.
- **n ≥ 20 tail measurement** for p95-grade latency claims.
- Launch-calendar / marketing items (docs/11 #21) remain post-MVP.

## Kill criteria

Numeric stop/pivot gates adopted at freeze (docs/11 #1; owner review before public launch):

1. **Distribution proof, 90 days.** From public launch, if there are not ≥ 10 independent
   active users or ≥ 3 non-trivial external contributions by day 90, stop the current
   distribution vehicle and pivot (ACP surface or a vertical wedge) — do not extend the
   window without a written reason.
2. **Post-release regression, 90 days.** After any major lab model release, re-run the
   pinned matrix within 90 days. If `tachyon-full` verified success falls below the serial
   reference on any representative task, freeze feature work until it is fixed.
3. **Red CI is stop-the-line.** Any red CI without a fix PR within 24 h halts feature work
   (AGENTS.md rule, reaffirmed at freeze).
4. **Performance budget.** A > 20 % p95 regression against `M14_MATRIX.json` or any §43
   target MISS on the ledger re-run reverts the causing change before merge.

## Spec §45 MVP exit dispositions

| # | Exit condition (spec §45) | Disposition | Evidence |
|---|---|---|---|
| 1 | CLI and TUI are usable gateway clients | MET | M11 gate: nine-pane TUI, `attach` + run/ps/pause/resume/cancel aliases, approval wait; M11 report |
| 2 | local gateway persists across client disconnects | MET | M11 gate: disconnect/close leaves tasks untouched; reconnect gapless replay |
| 3 | simple repository questions commonly use zero LLM calls | MET | Leg A: `direct_native`, 0 model calls on all 50 samples, 31/40 µs; M4/M5 gates |
| 4 | complex tasks start evidence work in parallel | MET | `full` cells measured evidence overlap 4/5/3; serial cells measured 1 |
| 5 | model and judgment providers are replaceable | MET | Matrix driven through the `ModelProvider` trait by a fake; OpenAI-compat adapter (M6), judgment registry + fakes + feature-gated OpenJEV (M7) |
| 6 | tasks recover after process restart | MET | G6: kill_restart, runtime_recovery, restart_approval, reentry; every supervisor sample recovered `completed` |
| 7 | local mutation batches recover safely | MET | G6: M8 crash-injection gates, recovery_scoped, effect_fixture §19 reconcile |
| 8 | workspace containment survives traversal/symlink tests | MET | G6: tools_gate traversal/symlink, mutation authorized alias/symlink refusal, runtime_repair zero-writes |
| 9 | verification gates completion | MET | Every matrix run completed only through the acceptance contract; M9 wrong-patch gate re-run in G6 |
| 10 | benchmarks report p50/p95 and verified success | MET | `M14_MATRIX.json` (n=10/cell, nearest-rank p50/p95, verified-success rates) + this report |
| 11 | tachyon-full beats the in-tree serial reference | PARTIAL | Verified success equal (150/150, both rates 1.0); median completion: serial control fastest on all 3 fixtures (full pays journal+verify-tail overhead against a ~0.01 ms scripted model); TTFR ties at 1 ms granularity. Scripted-model caveat above — no general speed claim is made |

**MVP status:** 10 of 11 exit conditions MET; the performance claim is PARTIAL and stated
as such. Tachyon is frozen for MVP on this basis.
