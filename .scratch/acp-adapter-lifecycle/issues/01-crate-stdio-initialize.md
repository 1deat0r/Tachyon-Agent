# 01: tachyon-acp crate + stdio lifecycle + initialize

**What to build:** A new `crates/tachyon-acp` binary crate: newline-delimited JSON-RPC 2.0 codec generic over `AsyncRead`/`AsyncWrite` (request/response correlation by id, notifications without id skipped, malformed-line typed protocol error, stdout = ACP frames only / stderr = logs), a minimal gateway client in-crate over `tachyon_gateway::transport` + `tachyon-protocol` frames (mirroring the private CLI loop — no `tachyon-tui` dependency), and the `initialize` handshake: negotiate `protocolVersion: 1`, probe gateway liveness (Ping/endpoint), advertise ONLY what is implemented (`loadSession: false`, Text-only prompt capabilities, nothing else), answer `notifications/initialized`, and fail every request with one clear typed error when the gateway is down — never auto-starting it. Unknown methods ⇒ standard JSON-RPC method-not-found. Workspace wiring: root `Cargo.toml` members, `docs/02_IMPLEMENTATION_SPEC.md` §1 crate bullet, `CONTEXT.md` crate shorthand (all three tripwires in `docs_freshness.rs`), plus the new-capability checklist doc `docs/agents/acp-adapter-capability.md`.

**Blocked by:** None (can start immediately)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-02)

- [x] `cargo verify` passes with the new crate: members list, spec §1 bullet, and CONTEXT shorthand all updated (docs_freshness tripwires green) and `[lints] workspace` + workspace package fields honored
- [x] Codec unit tests: framed request → correlated response; out-of-order responses; notification (no id) skipped without reply; malformed JSON line ⇒ typed protocol error, loop survives; request to unknown method ⇒ standard method-not-found error object
- [x] `initialize` negotiates `protocolVersion: 1` and the response advertisement equals exactly the implemented set (`loadSession: false`, Text-only prompt content; no audio/image/embedded, no fs/terminal claims) — asserted byte-exact against a golden shape in a test
- [x] Gateway-down test: with no gateway endpoint/socket, `initialize` (and any request) returns ONE clear actionable typed error and no gateway process is ever spawned (assert via endpoint file absence / no listener)
- [x] Live-gateway test: `initialize` succeeds against a test gateway and the adapter's stderr carries logs while stdout carries only valid ACP JSON-RPC frames
- [x] `docs/agents/acp-adapter-capability.md` contains the full AGENTS.md checklist (why-deterministic, input/output schema, access set, effect class, idempotency, resource claim, cancellation, retry, verification, crash-recovery, latency class) and matches actual behavior
- [x] Tests live in `crates/tachyon-acp/tests/` (round-trip against gateway fixture prior art `run_path.rs`/`g5_e2e.rs`) + codec units; neighbor suites `tachyon-app --test docs_freshness` green
