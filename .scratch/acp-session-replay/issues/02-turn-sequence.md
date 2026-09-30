# 02: Monotonic per-session turn sequence

**What to build:** Every task created in a session is stamped with a per-session monotonic turn sequence at creation, durable before any run can start, unique per session, never reused for terminal tasks. `GetSession` returns the session's turns as an ordered skeleton (sequence, task id, canonical status) so a client can see exactly which task was which turn — identically after a restart.

**Blocked by:** 01: Durable Session root through the gateway

**Status:** ready-for-agent

- [ ] Task creation assigns the next turn sequence atomically within its session (uniqueness enforced by the store; concurrent creation cannot double-assign)
- [ ] Turn→task association is persisted at creation, before `StartRun` can admit work
- [ ] Terminal tasks are never re-stamped or reused as new turns; sequences are strictly increasing per session
- [ ] `GetSession` lists turns in stable sequence order with task id and canonical `TaskStatus`
- [ ] Ordering is deterministic under same-instant creation (no wall-clock tie-breaking)
- [ ] Sequence assignment and ordering survive a gateway/store restart
- [ ] Existing databases migrate additively with legacy tasks sequenced deterministically by creation order
- [ ] Tests at both seams: store uniqueness/ordering/migration, gateway round-trip incl. restart
