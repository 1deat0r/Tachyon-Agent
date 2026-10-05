# Development workflow

Tachyon's normal development loop is local and Git-based:

```text
task and relevant context → inspect → implement → verify locally → review diff → atomic commit
```

GitHub backs up and synchronizes the work and hosts long-lived tracking when useful. Pull requests are retired: every verified change lands as a direct commit to `main`, gated only by local `cargo verify`. A read-only CI mirror (`.github/workflows/ci-mirror.yml`) repeats the VERIFY tier plus platform tests and `cargo deny` for external visibility; it never gates, publishes, or pushes.

## Choose the smallest useful process

- Start with `AGENTS.md`, the relevant domain context, and the specific architecture or ADR references touched by the change. Do not read unrelated documentation just because it exists.
- Keep routine work in the current working tree and commit directly to `main`. Branches exist only for genuinely isolated experiments; land them back into `main` with a fast-forward or merge and delete them.
- GitHub Issues are optional. Use one when it materially helps with deferred backlog, multi-session work, dependencies, coordination, architectural decisions, or an externally reported problem. A small task that can be completed now needs no Issue.
- Do not open pull requests. The read-only CI mirror (`.github/workflows/ci-mirror.yml`) runs `cargo verify` on Linux plus platform tests and `cargo deny` for external visibility; it never gates, publishes, or pushes. Local `cargo verify` remains the only commit gate.
- Skills are tools for planning, implementation, tests, debugging, and review. This repository policy overrides a skill's default request to publish an Issue, create a branch, or open a PR when that step adds no value. Apply the task and relevant spec to choose routine ticket breakdowns and test seams; proceed without approval checkpoints for those choices. Ask only when a material requirement, safety/security boundary, architecture decision, or external authorization is unresolved.

## Skills and hooks

Treat the current task and conversation as sufficient context for routine work. Do not add a `/to-spec`, `/to-tickets`, `/wayfinder`, Issue, branch, or PR stage unless it materially improves durable planning, coordination, isolation, or review. Skills may still provide useful decomposition, TDD, debugging, and review; adapt their output to the smallest useful local artifact. Use `/to-spec` and `/to-tickets` when a durable plan or dependency graph helps across sessions; use `/wayfinder` for large, uncertain efforts that benefit from a decision map. Keep prototypes in the current working tree unless preserving a separate artifact adds value. `/implement` and `/implement-spec` must finish with `cargo verify`, a reviewed diff, and an atomic commit directly on `main`. `/setup-pre-commit` must follow the fast-hook rule below and must not install a task runner or put the full test suite in a blocking hook.

Keep blocking Git hooks fast: formatting, lightweight lint, and secret checks are suitable. Do not put the full test suite in a hook; `cargo verify` is the required agent gate before committing.

## Local verification

The canonical entry point is `cargo verify`; it uses only the Rust toolchain and repository scripts, with no added task-runner dependency.

| Tier | Command | Checks |
| --- | --- | --- |
| FAST | `cargo verify fast` | Formatting and workspace compile check |
| VERIFY | `cargo verify` | FAST, workspace tests, and strict Clippy for the workspace and verifier |
| FULL | `cargo verify full` | VERIFY plus security/recovery suites, fixture gate, benchmark matrix, and release performance gate |

Verifier Cargo checks and the FULL helper scripts use `--locked`. Run FAST during implementation as useful. Run VERIFY before each commit unless a documented environmental limitation prevents it; explain any skipped check in the commit or task summary. Run FULL when changing those acceptance gates or when the task calls for exhaustive validation. FULL requires a POSIX shell and Node.js and defaults to ten benchmark samples per cell (`M14_SAMPLES` can override this). It writes generated outputs under ignored `target/m14/`; it does not refresh tracked reports. To deliberately refresh the published matrix artifact, run `node scripts/m14_matrix_check.mjs` and review its diff.

The first run may need to download the pinned Rust toolchain or uncached Cargo dependencies. The project toolchain is pinned in `rust-toolchain.toml`.

## Commits and review

Before committing, inspect `git status`, `git diff --check`, and the staged diff. Keep each commit small, coherent, and independently understandable; use a Conventional Commit subject. Do not stage secrets, generated reports, build output, or unrelated user/agent changes. Preserve the repository's architecture, security, recovery, and effect-boundary invariants in `AGENTS.md`.

Local VERIFY is the normal pre-commit gate. Review the diff yourself on every change; use an independent expert review for architecture, security, recovery, protocol, or broad workflow changes where it can catch meaningful errors. Commit directly to `main`; the `post-commit` hook publishes each commit to GitHub immediately.

## Releases

There is no hosted CI. Local VERIFY is the only routine gate, and FULL is the release gate: run `cargo verify full` when changing the security, recovery, fixture, matrix, or performance gates, or when a task calls for exhaustive validation. Do not remove security, release, or platform checks without evidence that they are redundant and no longer protect a supported path.
