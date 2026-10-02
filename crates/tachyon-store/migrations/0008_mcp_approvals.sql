-- Session-scoped MCP approval records (issue #57, ACP MCP-stdio slice
-- review fix round; ADR-0005 blocker 3). One row per parked approval:
-- the session-scoped approval id, owning session, kind (`launch` for a
-- register call's server set, `call` for one parked `CallMCPTool`), the
-- BLAKE3 hash (hex) of the exact authorized operation JSON the grant
-- binds to, the outcome (`parked` at park time, then `granted` /
-- `denied` / `consumed-missing` at decide time — one-shot, never
-- rewritten after the decide), and micros-since-epoch park/decide
-- timestamps (0 while parked). Additive: new table only, existing rows
-- and columns untouched. Crash semantics stay fail-closed — the live
-- park is still the in-memory map, so a restarted gateway answers
-- pre-restart ids with `approval_missing`; these rows are the durable
-- audit trail, not the grant.
CREATE TABLE mcp_approvals (
    approval_id TEXT    NOT NULL PRIMARY KEY,
    session_id  TEXT    NOT NULL REFERENCES sessions (id),
    kind        TEXT    NOT NULL,
    op_hash     TEXT    NOT NULL,
    outcome     TEXT    NOT NULL,
    created_at  INTEGER NOT NULL,
    decided_at  INTEGER NOT NULL DEFAULT 0
) STRICT;
