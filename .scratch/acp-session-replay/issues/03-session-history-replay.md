# 03: Ordered Session history replay

**What to build:** `GetSession` returns the complete Session history: each turn with its canonical conversation content, derived read-only from canonical durable task state in stable turn order. A client can fetch the same history before and after a gateway restart and get identical results, with zero writes on the read path — the durable session lookup and ordered replay release blocker of ADR-0005, closed with evidence.

**Blocked by:** 02: Monotonic per-session turn sequence

**Status:** ready-for-agent

- [ ] `GetSession` includes each turn's conversation content sourced from canonical task state (snapshots + append-only journal), with no second message store
- [ ] Full history is byte-identical across a gateway/store restart (create session + turns → restart → fetch equality)
- [ ] Fetch path performs no writes: task/session/journal state unchanged before and after `GetSession`
- [ ] Read results never create work, start runs, re-enter drivers, or replay effects (read-only projection)
- [ ] Works for legacy sessions (no root) and sessions with a root
- [ ] Tests at both seams: gateway round-trip restart-equality + read-only assertion, store projection ordering
