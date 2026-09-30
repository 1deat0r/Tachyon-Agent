# 0001 — Env-gated fault points in production code

## Status

accepted · 2026-09-24

## Context

M12 recovery tests must crash processes at exact commit seams (journal commit, effect `prepared` barrier, model-call enter/return, verifier wait, approval park, mutation file commit) and at the real gateway binary. `cfg(test)` hold-points (e.g. `verification.rs`) do not exist in integration-test builds of the library or in the shipped `tachyon` binary. Timed/random kills are flaky. Natural park points only cover approval wait.

Alternatives considered:

1. **Env-gated fault points in production code** — named hold compiled in always; no-op unless `TACHYON_FAULT_POINT` matches; blocks until the test releases a file/pipe.
2. `cfg(test)` holds only — cannot arm a killed child process or the gateway binary.
3. Natural parks only — cannot hit `§42` mid-commit seams.
4. Timed kill windows — non-deterministic; rejected (M12 grill Q2/Q7).

## Decision

Use **env-gated fault points** as the single shared M12 kill mechanism: arm → process reaches seam → parent `Child::kill()` → restart → assert reconcile. Portable `std` APIs only (`Child::kill`, `process::exit`) so ubuntu/windows/macos CI run identical tests. Off by default (cached env read via `OnceLock`); controlling the gateway env already implies controlling the process.

## Consequences

- One pattern across all six fault domains and the checked-in gateway SIGKILL test.
- Production binaries contain inert hold sites; document in security notes.
- `verification.rs` `cfg(test)` hold can stay or migrate later; not a blocker.
