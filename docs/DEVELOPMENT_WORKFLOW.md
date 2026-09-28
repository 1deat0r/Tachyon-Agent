# Development workflow

Tachyon's normal development loop is local and Git-based:

```text
task and relevant context → inspect → implement → verify locally → review diff → atomic commit
```

GitHub backs up and synchronizes the work, hosts long-lived tracking when useful, and checks clean builds on supported platforms. It is not a required planning or review hop for every change.

## Choose the smallest useful process

- Start with `AGENTS.md`, the relevant domain context, and the specific architecture or ADR references touched by the change. Do not read unrelated documentation just because it exists.
- Keep routine work in the current working tree. Create a branch when isolation, parallel work, or repository protection calls for one.
- GitHub Issues are optional. Use one when it materially helps with deferred backlog, multi-session work, dependencies, coordination, architectural decisions, or an externally reported problem. A small task that can be completed now needs no Issue.
- Pull requests are optional. Use one for risky or substantial changes that benefit from remote review, public contributions, parallel work, or a repository rule that requires one. Do not create a PR solely to satisfy convention.
- Skills are tools for planning, implementation, tests, debugging, and review. This repository policy overrides a skill's default request to publish an Issue, create a branch, or open a PR when that step adds no value.

## Local verification

The canonical entry point is `cargo verify`; it uses only the Rust toolchain and repository scripts, with no added task-runner dependency.

| Tier | Command | Checks |
| --- | --- | --- |
| FAST | `cargo verify fast` | Formatting and workspace compile check |
| VERIFY | `cargo verify` | FAST, workspace tests, and strict Clippy for the workspace and verifier |
| FULL | `cargo verify full` | VERIFY plus security/recovery suites, fixture gate, benchmark matrix, and release performance gate |

Verifier Cargo checks and the FULL helper scripts use `--locked`. Run FAST during implementation as useful. Run VERIFY before each commit unless a documented environmental limitation prevents it; explain any skipped check in the commit or task summary. Run FULL when changing those acceptance gates or when the task calls for exhaustive validation. FULL requires a POSIX shell and Node.js and defaults to ten benchmark samples per cell (`M14_SAMPLES` can override this). It writes generated outputs under ignored `target/m14/`; it does not refresh tracked reports. To deliberately refresh the published matrix artifact, run `node scripts/m14_matrix_check.mjs` and review its diff.

The first run may need to download the pinned Rust toolchain or uncached Cargo dependencies. CI caches downloaded Cargo sources; each runner builds its own output. The project toolchain is pinned in `rust-toolchain.toml`.

## Commits and review

Before committing, inspect `git status`, `git diff --check`, and the staged diff. Keep each commit small, coherent, and independently understandable; use a Conventional Commit subject. Do not stage secrets, generated reports, build output, or unrelated user/agent changes. Preserve the repository's architecture, security, recovery, and effect-boundary invariants in `AGENTS.md`.

Local VERIFY is the normal pre-commit gate. Review the diff yourself on every change; use an independent expert review for architecture, security, recovery, protocol, or broad workflow changes where it can catch meaningful errors. Commit directly to the normal development branch when permitted. Never weaken or bypass GitHub branch protections to avoid a required check or review.

## CI and releases

Main-branch pushes and pull requests run the same `cargo verify` entry point on Ubuntu, Windows, and macOS. Feature-branch pushes are checked by the PR run, avoiding a duplicate matrix before a pull request exists. Those checks provide independent platform coverage and remain part of repository branch protection. Continue useful local work while remote checks run; a protected merge or release must still satisfy the applicable repository rules.

The scheduled/manual acceptance workflow runs `cargo verify full`. Keep expensive or platform-specific checks there when they add confidence without slowing routine commits. Do not remove security, release, or platform checks without evidence that they are redundant and no longer protect a supported path.
