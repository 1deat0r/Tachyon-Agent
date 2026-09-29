# 09 — Initial Hermes Handoff Checklist (Historical)

This checklist records the original project scaffold handoff. It is not a
current agent startup or delivery gate. For current work, follow
[`AGENTS.md`](../AGENTS.md) and the [development workflow](DEVELOPMENT_WORKFLOW.md),
consulting only the architecture, specification, and domain references relevant
to the task. The Milestone 0 setup steps below are retained for historical
context and do not apply to routine development.

Before implementation starts:

- [ ] Read `AGENTS.md`.
- [ ] Read docs 00–08.
- [ ] Confirm Rust 1.98.1 toolchain.
- [ ] Run scaffold formatting/check/tests.
- [ ] Initialize Git if this folder is not already a repository.
- [ ] Create/update `PROGRESS.md`.
- [ ] Begin Milestone 0 only.

Before each milestone is marked complete:

- [ ] Workspace formats.
- [ ] Workspace checks.
- [ ] Unit/integration tests pass.
- [ ] Clippy passes with warnings denied.
- [ ] Milestone-specific gate passes.
- [ ] New capability metadata is complete.
- [ ] Security/recovery implications are tested.
- [ ] Performance measurements are recorded when the milestone has a latency gate.
- [ ] `PROGRESS.md` updated.
- [ ] Coherent Git commit created.

Before MVP is declared complete:

- [ ] Recovery fault-injection suite passes.
- [ ] Security escape suite passes.
- [ ] DirectNative repository queries commonly avoid LLMs.
- [ ] Same-model serial-reference benchmark performed.
- [ ] p50 and p95 TTFR/completion reported.
- [ ] Verified success did not regress for speed.
- [ ] Provider/Judgment abstractions demonstrably replaceable.
- [ ] TUI detach/reattach works without killing task.
- [ ] Remote gateway remains off by default.
- [ ] `MVP_REPORT.md` written.
