# Handoff — 2026-09-26 session: scaffold research → Skeleton v2 → Tachyon delivery

Historical session snapshot. The open-work list below reflects 2026-09-26 and may be stale; verify tracker state before resuming it. Current development follows [`AGENTS.md`](AGENTS.md) and [`docs/DEVELOPMENT_WORKFLOW.md`](docs/DEVELOPMENT_WORKFLOW.md), which supersede delivery instructions recorded in this snapshot.

## What this session established (condensed)

1. **Muse-spark scaffold audit**: re-fetched all 15 of its sources — 13/15 clean, one 404 (real Codex doc: `https://developers.openai.com/codex/agent-configuration/agents-md`), one miscite, vendor-family independence problem, two unsourced tree elements.
2. **Method decision (user-confirmed direction)**: `project-skeleton` (canonical, `VERSION` 0.1.0, methodology 2026-09-24) **stays the method**; the 2026-09-26 sources pass is its scheduled re-research. Method delta for the offered-but-not-yet-started **v0.2.0 merge**: (a) live-URL + claim-in-text check, (b) 3 sources must span ≥2 vendor families, (c) every tree element sourced (local convention counts locally), (d) scarcity ≠ irrelevance (evals + permissions re-admitted with 3 vendors each).
3. **Tachyon audit** (`/run/media/its1deat0r/Projects/AI Agents/Tachyon Agent`): full skeleton v1 compliance-plus; gaps were permission layer, acceptance-not-in-CI, skills-lock ignored, and GitHub required-review absent.
4. **Delivered Skeleton v2** (7 paths, one squash PR, all gates green):
   - PR https://github.com/1deat0r/tachyon/pull/30 merged → `e1c1384` on main (tree clean, == origin).
   - `.claude/settings.json` (env-read deny), `opencode.json` (git-push ask, env deny), `.github/workflows/acceptance.yml` (nightly `17 3 * * *` + dispatch), `skills-lock.json` tracked, `.gitignore` `.claude/*` + `!.claude/settings.json`.
   - Governance: `docs/adr/0003-skeleton-v2-permission-and-acceptance-gates.md` + `AGENTS.md` stamped `Skeleton: v2 — 2026-09-26`.
5. **Verification**: 3 unlazy ledgers, **16/16 gates met** (`/tmp/opencode/tachyon-v020/GATES*.md`; approvals under `~/.unlazy/approved`). Pre-push local gate suite (fmt/check/test/clippy `-D warnings`) green; PR 3-OS CI green; acceptance first live run green (all 5 steps).
6. **Flake caught post-merge**: Windows `cancel_acknowledges_after_real_reap_while_the_mailbox_serves` (responsive_actor.rs:351) failed on main, same tree passed Windows ×2 on the PR and on rerun → pre-existing timing race, not the change. Stop-the-line issue filed: https://github.com/1deat0r/tachyon/issues/31.

## Open work (priority order)

1. **Issue #31 cancellation race** — stabilize the reap-vs-durable-cancel race at `crates/tachyon-core/tests/responsive_actor.rs:351` (tolerate the interleaving or sync on the durable-cancel receipt). The Issue is optional tracking for this multi-session fix. Work locally, run `cargo verify`, review the diff, and commit atomically; use a branch or PR only if isolation, review value, or repository protection calls for it.
2. **project-skeleton v0.2.0 merge** (offered, not yet approved) — edit canonical source at `/run/media/its1deat0r/Projects/Skills/canonical/project-skeleton/` (never the opencode view), bump `VERSION` 0.1.0 → 0.2.0 + CHANGELOG, add: `.claude/skills/` frozen path, permission-layer element, lazy `evals/` element, the four method rules above, refreshed source citations. Then re-export views (`export.py`) and run `scripts/validate-all.sh` per home `AGENTS.md`. **Input**: Hindsight document titled `Skeleton v2 sources pass (2026-09-26) — verified scaffold findings` (retrieve via `hindsight_search_knowledge_pages`).
3. **Review-gate revisit trigger** — when a second identity/bot exists, enable `required_approving_review_count` on main (solo self-approval deadlock recorded in ADR 0003).
4. Optional: first *scheduled* acceptance run fires 03:17 UTC nightly — glance at Actions after the first one.

## Key decisions — do not re-litigate

- Branch protection stays **checks-only** (user's explicit choice; rationale in ADR 0003).
- Frozen paths untouched; v2 was additive only (project-skeleton §4 honored: ADR + stamp).
- 3-source standard for this work = 3 sources **and ≥2 vendor families** (or labeled GitHub-authoritative for platform mechanics).

## Artifacts (reference, not duplicated)

- Repo: ADR 0003, `AGENTS.md` (delivery rules + stamp), `acceptance.yml`, permission configs — all at `github.com/1deat0r/tachyon`, commit `e1c1384`.
- Ledgers/evidence: `/tmp/opencode/tachyon-v020/` (GATES.md, GATES-skeleton-v2.md, GATES-delivery.md + pr-body.md + flake-issue.md). **`/tmp` may be wiped on reboot** — ledgers are evidence only; the work itself is landed in git/GitHub.
- Research findings: Hindsight doc `Skeleton v2 sources pass (2026-09-26) — verified scaffold findings`.
- This file: repo-root `HANDOFF.md` at project root, tracked in git — matching the convention in pi-rust, Hermes-Agent-Rust, TIDE (and Research's lowercase `handoff.md`). A transient copy also sits at `/tmp/opencode/handoff.md`.

## Suggested skills (Skill tool)

- `unlazy` — write gates before any non-trivial continuation (this session's 3-ledger pattern).
- `project-skeleton` — needed for item 2 (and its §4 versioning rules).
- `diagnosing-bugs` — for item 1 (issue #31 race).

## Environment notes

- Host: opencode/T3 Code; run gates from `/home/its1deat0r/.config/opencode/skills/unlazy/scripts/` with `--cwd "<tachyon project path>"`; ledgers outside the repo need an explicit path argument.
- No secrets in this document; git author PII intentionally omitted.
