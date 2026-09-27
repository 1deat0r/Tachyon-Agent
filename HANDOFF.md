# Handoff — 2026-09-27 session: pipe flake → ack-reap flake → Intent substrate slices → paqet skill

Fresh-session entry point. Nothing below is in-progress mid-edit; everything landed is committed/green, everything open is listed under **Open work**. Main is clean at `24aca56`, equal to origin.

## What this session delivered (condensed)

1. **Issue #35 Windows pipe fix (PR #36 → `9a689af`)**: `transport::connect` (cfg windows) retries `ERROR_PIPE_BUSY` bounded (~1 s); `start_with` publishes the endpoint only after recovery + accept-loop spawn. Regression probe `pipe_staging.rs` + `kill_restart` both proven green on `windows-latest`.
2. **Issue #37 ack-reap flake (PR #38 → `a3171f8`)**: unrelated `tachyon-core` flake surfaced in #36's PR-run (same commit green in push run) → stop-the-line issue filed, `wait_pid_dead` bounded wait (200×25 ms), re-run green, merged.
3. **ChatGPT self-improving pack reviewed**: `tachyon-self-improving-pack.zip` evaluated against MVP freeze + `AGENTS.md` deferrals. Verdict: design valuable, code stubs only, Phases B–F stay deferred. Outcome: **ADR 0004** (PR #39 → `65b870c`) scoping Phase A (Intent substrate) as a verification-gate extension.
4. **Slice 1 — IntentSpec (PR #45 → `bc4cdbc`, closes #40)**: new `tachyon-intent` crate (not inside `tachyon-ir` — belief-with-confidence kept out of the validated-graph contract). `Provenance`, mandatory-provenance `AttributedText`, `validate()`, quarantine accessors. 11 tests, TDD red→green.
5. **Slice 2 — criteria compiler (PR #46 → `8e05b98`, closes #41)**: `tachyon-verify` `compile` module (`compile_criterion/criteria/spec`), narrow mini-syntax, `HardConstraint` bindings with BLAKE3 ids, fail-closed `Unresolved`. 14 tests. Review-driven strictness fix-ups (dot/empty-segment rejection, trim normalization).
6. **Slice 3 — conformance report (PR #47 → `8593833`, closes #42)**: `check_conformance(spec, contract, report, baseline, current)` — pure, advisory-only; constraints Satisfied only on text-match with an evaluated `HardConstraint` plus passing report. 6 integration tests over real runs (`#![cfg(unix)]` precedent). Compatibility deferred to #48 (needs a nonexistent `IntentSpec` field).
7. **Process throughout**: every PR through independent two-axis review subagents (standards + spec, verbatim reporting) — 0 hard violations total; full local gates + 3-OS CI green on every merge; stop-the-line issue discipline (#37).
8. **New global skill `paqet`** (`~/.prime/agent/skills/paqet/`): P.A.Q.E.T session audit — Performance, Accuracy, Quality, Efficiency, Token efficiency; per-lens /100, weights, calibration anchors, finding-ownership, panel mode (5 experts) + quick mode. First panel scored this session 88; skill repaired from its own review (bands, split-or-lump, spread-to-range).

## What the follow-up session delivered (2026-09-27, continued)

9. **Slice 4 — correction lifecycle (PR #50 → `24aca56`, closes #43)**: last Phase-A slice with new code. `tachyon-intent` gains `correction.rs` (6 classes, middle-four `may_persist` gate, typed validation, `propose_durable`), `knowledge.rs` (confirm/contradict/retire/revalidate lifecycle, `apply_bad_evidence` penalty that creates nothing, `storage_key`), `clarify.rs` (ask/skip `decide` with fail-closed non-finite handling, `JudgmentMapping` mirror with no new dep edge). 22 tests TDD red→green (34/34 in crate); `uuid` workspace edge with `Cargo.lock` checked in first time. CONTEXT.md gains Intent-and-learning glossary (IntentSpec, intent conformance, correction classes, durable item, ask/skip). Two-axis review: standards clean, spec NEEDS_REPRO adjudicated REJECTED-with-reason (string refs cite rather than snapshot; slice-1 `Vec<String>` precedent; rationale in-tree). Full local gates + 3-OS CI green, squash-merged, branch deleted.

## Open work (priority order)

1. **Issue #48** — compatibility coverage in conformance (needs `IntentSpec` compatibility field first). Next up; all Phase-A code slices done.
2. **Issue #26** — effect-barrier journal follow-up (`ready-for-agent`, predates this session).
3. **Issue #44** — deferred roadmap tracker, Phases B–F (blocked on Phase A + exit gate/ADR).
4. Optional: paqet per-PR quick runs; next panel at milestone boundary.

## Key decisions — do not re-litigate

- New `tachyon-intent` crate over extending `tachyon-ir` (belief vs validated graph).
- Compiler lives in `tachyon-verify` (owns `Clause`); `verify → intent` via `path` dep, no cycle.
- Conformance is advisory-only; `Unresolved` never fails `conforms`, only `Violated` does.
- Free text provably never compiles to `CommandPasses`/`HardConstraint` (battery test); `CommandPasses` omission is deliberate fail-closed design.
- Mini-syntax owned by `compile.rs` rustdoc until a follow-up moves it into spec/ADR.
- `Cargo.lock` must be checked into every dep-edit commit — missed twice, amended pre-PR both times.
- Paqet scoring: per-lens /100, weighted total, one finding in exactly one home lens, debt-halving mandatory, spread >15 or lens <70 → reported range.

## Artifacts (reference, not duplicated)

- Repo: `github.com/1deat0r/tachyon`, main `24aca56` (post-merge push CI green 2026-09-27). ADRs 0001–0004 in `docs/adr/`.
- PRs: #36, #38, #39, #45, #46, #47, #50 (all squash-merged, all 3-OS green; feature branches deleted). Issues: closed #35, #37, #40, #41, #42, #43; open #26, #44, #48.
- Handoff PR #49 (this branch) stays open per convention; Codex pickup: `git checkout main`, tree clean, next work is issue #48.
- Initiative memory: "Intent substrate (Phase A)" tracks slices (Hindsight page `kp-ebad1b2c1c00428aa2d06a31c6bede7e`).
- Skill: `~/.prime/agent/skills/paqet/SKILL.md` (global, loads in new sessions).
- This file: repo-root `HANDOFF.md`, tracked in git.
