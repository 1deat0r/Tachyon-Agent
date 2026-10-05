# Tachyon

A high-performance AI agent harness. Routine work runs through deterministic
code, repository indexes, and bounded judgment — LLM reasoning pays only for
genuine unresolved uncertainty.

> LLMs are reasoning accelerators, not Tachyon's operating system.

**Status (Oct 2026):** Milestone 20 stateful evaluation complete.
The baseline verified 16/20 runs. The registered reference line was not met.
Four failures are retained: one partial repair, one transport error, and two
timeouts. Protected files and completed/failed recovery passed. See the
[stateful report](docs/milestones/STATEFUL_REPORT.md).
Production behavior remains unchanged. The M18 candidate remains unselected.
The M16 [live baseline](docs/milestones/HELDOUT_REPORT.md) remains 29/30.
The MVP remains frozen at Milestone 14; see
[`PROGRESS.md`](PROGRESS.md) and [`MVP_REPORT.md`](MVP_REPORT.md).
These small samples do not establish general coding-agent reliability.

Read the frozen MVP matrix numbers as **harness overhead**: every cell runs a pinned
scripted provider (no live-model leg), so they show path and verification
cost, not end-to-end model latency.

## Quickstart

Requires Rust 1.98.1 (`rustup` installs it from `rust-toolchain.toml`).

```bash
cargo verify
cargo run -p tachyon-app -- doctor
```

In one shell, start the runtime; in another, drive it:

```bash
tachyon gateway
tachyon                # bare invocation opens the TUI (attach)

# Deterministic lookup: routed, indexed fresh, answered with source
# locations and ZERO model calls. A question that would need a model or
# a judge is refused with `requires_model`, never degraded into a lookup.
tachyon query "Where is refreshToken defined and used?"

tachyon session create
tachyon task create --session <SESSION_ID> "Fix the refresh race"
tachyon task list
```

## Architecture

- **Rust-first core** — scheduler, persistence, policy, repo intelligence.
- **Task Supervisor** — single logical writer of canonical task state.
- **Execution IR** — every scheduled operation is validated before it runs.
- **Predictive routing** — cheapest sufficient path first, not a model loop.
- **Verification-gated completion** — done means proven, not self-reported.

Repository intelligence (M4) and predictive routing (M5) now sit on a
user-facing path: `tachyon query` routes a question with the classifier,
indexes the workspace fresh (content hashes are authoritative), and
returns definitions and references with `model_calls: 0`. The `StartRun`
run path still does not call them, and judgment (M7) is unused, so its
wire-vs-delete disposition stays open until an ADR decides it.

Start with [`AGENTS.md`](AGENTS.md), then read the domain and architecture
references relevant to your task. The frozen architecture and implementation
contract remain authoritative where applicable.

## Contributing

Use the local-first loop in
[`docs/DEVELOPMENT_WORKFLOW.md`](docs/DEVELOPMENT_WORKFLOW.md): implement,
run `cargo verify`, inspect the diff, and make an atomic commit directly on
`main`. Pull requests are not used; Issues are optional for durable tracking.
Keep tests for core invariants; frozen architecture changes need an ADR in
`docs/adr/` first.

## Security

See [`SECURITY.md`](SECURITY.md) for reporting and scope.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at
your option — the Rust ecosystem standard.
