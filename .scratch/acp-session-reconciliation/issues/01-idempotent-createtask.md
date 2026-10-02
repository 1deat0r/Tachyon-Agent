# 01: Idempotent CreateTask with client idempotency keys

**What to build:** A gateway client that sends `CreateTask` with an optional idempotency key and loses the response can retry the identical command and receive the byte-identical original success response — same task, same turn, no duplicate — including after a gateway restart between commit and response. Reusing the key with a different objective fails with a typed `idempotency_key_conflict`. Clients that send no key behave exactly as today. The success response includes the minted `turn_seq` so keyless clients can still correlate their turn via `GetSession`. Ships the AGENTS.md capability checklist doc for the new capability.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] `CreateTask` accepts an optional `idempotency_key`: non-empty, ≤128 bytes; empty/oversized → typed validation error; absent → legacy behavior unchanged
- [ ] Key row (`UNIQUE(session_id, key)`, request fingerprint, stored success-response JSON) commits in the same transaction as the task + journal event; crash between commit and response → retry after restart replays the stored response verbatim
- [ ] Duplicate `(session_id, key)` with identical fingerprint → replay stored response byte-identically (one task, one turn only)
- [ ] Same key with different `objective` → typed `idempotency_key_conflict`; no state change
- [ ] Concurrent same-key sends → exactly one task (store uniqueness enforced under race)
- [ ] `CreateTask` success response includes correct `turn_seq`, matching what `GetSession` reports for that turn
- [ ] No-key duplicate sends still create two tasks (legacy semantics documented in tests)
- [ ] Tests at both seams: gateway round-trip (duplicate, conflict, restart-retry, race, legacy, turn_seq, validation) and store (same-transaction record/replay, fingerprint mismatch, unique race)
- [ ] Capability checklist doc `docs/agents/task-creation-reconciliation-capability.md` with all 11 AGENTS.md fields, factually matching the shipped code
