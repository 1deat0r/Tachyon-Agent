# 0003 — Skeleton v2: committed permission layer, nightly acceptance gates

**Status:** accepted · 2026-09-26

## Context

Skeleton v1 (stamp `Skeleton: v1 — 2026-09-24` in `AGENTS.md`) shipped frozen paths, the delivery loop, and CI on every push. The 2026-09-26 re-research pass (project-skeleton's scheduled sources pass; every external claim below was fetched that day) found three gaps:

1. **No committed permission layer.** Hard rules in `AGENTS.md` (gates-before-push, environment hygiene) rested on prose alone. Claude Code reads `.env` files by default inside the working directory, and opencode's bash permission defaults to `allow`. Three independent vendors document allow/ask/deny policy as configuration: Codex sandbox/approvals (`sandbox_mode`, `approval_policy`), Claude Code permissions (explicitly "check permission settings into version control"), and opencode's `permission` config.
2. **Acceptance verification was manual-only.** `AGENTS.md` states "Completion is gated by acceptance/verification, never model self-report," yet the named security/recovery suites, the fixture gate, and the spec §44 matrix ran only by hand. `ci.yml` gates fmt/check/test/clippy on three OSes per push but never the acceptance set.
3. **`skills-lock.json` was gitignored** while `Cargo.lock` is tracked — clones could not reinstall pinned skill hashes. The lock is portable (zero absolute paths).

Alternatives considered:

1. **Stay prose-only** — rejected: untracked policy decays silently, and policy text without a home is exactly what the skill source-citation policy exists to prevent.
2. **Require 1 approving review on `main`** — rejected for now: this is a sole-account repository (every PR authored by `1deat0r`), GitHub forbids self-approval, and `enforce_admins=true` blocks admin bypass, so a required approval would deadlock the merge flow. Revisit when a second identity or bot account exists.
3. **Run the matrix on every PR** — rejected: cost/fidelity mismatch. `ci.yml` already gates every push; the 150-cell release matrix is nightly material.

## Decision

Adopt the Skeleton v2 content layer — five paths, no frozen path moved:

1. `.claude/settings.json` — deny `Read(.env)` and `Read(.env.*)` with a `Read(!.env.example)` carve-out, mirroring `.gitignore`.
2. `opencode.json` — `git push` and `git push *` → `ask`; `read`/`edit` deny `*.env` and `*.env.*`, allow `*.env.example`.
3. `.github/workflows/acceptance.yml` — `workflow_dispatch` plus nightly schedule, `permissions: contents: read`, running `scripts/m14_suites.sh` → `scripts/m14_fixture_gate.sh` → `scripts/m14_matrix.sh` (`M14_SAMPLES=10`) → `scripts/m14_matrix_check.mjs`, off the PR path.
4. Track `skills-lock.json`; `.gitignore` switches `.claude/` to `.claude/*` + `!.claude/settings.json` (a directory exclusion cannot re-include children).
5. Branch protection stays checks-only (alternative 2 above). The `/code-review` sentence records the delivery workflow in force on 2026-09-26; current review policy is local diff review on every change and independent expert review when it adds value, as defined in [`docs/DEVELOPMENT_WORKFLOW.md`](../DEVELOPMENT_WORKFLOW.md).

## Evidence

- Sources fetched 2026-09-26: Claude Code permissions and skills docs; Codex sandbox and AGENTS.md docs; opencode permissions, skills, and rules docs; GitHub status-checks, protected-branches, PR-review, and Actions docs; OpenAI evaluation best practices; Anthropic skill best practices; agentskills.io specification.
- Local unlazy ledger, 7/7 met and re-verified: config parses with expected policy, `acceptance.yml` parses with all four runners wired, lock un-ignored/staged/portable, exact five-path changeset, protection confirmed `checks-only-confirmed` via the GitHub API.

## Consequences

- `AGENTS.md` stamped `Skeleton: v2 — 2026-09-26`.
- Fresh clones carry the permission policy and can reinstall pinned skills; the nightly acceptance run fails loudly if the suites, fixture gate, or matrix contracts break. First live run is a manual `workflow_dispatch` after this lands.
- The required-review gate stays absent on GitHub until a second identity exists; that revisit trigger lives in this record.
- Unchanged from v1 at decision time: three-OS CI, frozen paths, delivery loop, lazy skeleton rule. The current local-first workflow and CI triggers are defined by [`docs/DEVELOPMENT_WORKFLOW.md`](../DEVELOPMENT_WORKFLOW.md).

## Migration/rollback plan

- Migration: none — the change is additive; existing clones pick up the policy on pull, and the `.gitignore` edit only widens tracking.
- Rollback: delete the two config files and `acceptance.yml`, restore the `.claude/` and `skills-lock.json` ignore lines, revert the stamp to v1, and mark this ADR `superseded` rather than deleting it (reversals keep the record).
