# 0002 — Driver re-entry is user-triggered

## Status

accepted · 2026-09-24

## Context

Spec §41 step 6: recovery must "resume safe work or request user intervention." After restart, interrupted runs are not respawned (`recover_incomplete` only rebuilds supervisor handles). `Resume` on `Recovering` currently fails `IllegalTransition` (only `Paused` is resumable). M11 explicitly deferred driver re-entry and the fresh-id approval re-ask to M12 (`restart_approval.rs:153-168`, `approval_wait.rs:496-500`).

Alternatives considered:

1. **User-triggered `Resume` on `Recovering`** — in-flight run → respawn `drive()`; ask path issues a fresh approval id (old id stays dead). No run to re-enter → `Recovering→Paused`.
2. Auto-respawn drivers at gateway boot — surprise runs; may re-enter approval waits without a human watching.
3. New protocol command (`ResumeRun`) — splits `Resume` semantics across statuses; grows the protocol surface.

## Decision

**`Resume` on `Recovering` is the only re-entry path.** With a durable in-flight run, it respawns the shared driver; the continuation approval always gets a **fresh id** (pre-restart id expired/consumed — never reused, never silent-grant). With no run, transition `Recovering→Paused` so the existing `Paused→Created` path applies. Gateway start never auto-spawns runs.

## Consequences

- Matches `§41` "request user intervention" default; safe-work rerun happens inside the re-entered driver (pure reads rerun by construction).
- M11 test `restart_approval.rs` API-park leg flips from "refuses" to "lands `Paused`"; run-having leg gains re-entry + fresh-id assertions.
- Glossary: **driver re-entry**, **fresh-id re-ask** (`CONTEXT.md`).
