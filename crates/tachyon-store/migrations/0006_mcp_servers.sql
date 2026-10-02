-- Pinned MCP server descriptors (issue #57, ACP MCP-stdio slice ticket 01;
-- ADR-0005 blocker 3). One row per (session_id, server_id): the validated
-- command, args JSON, env JSON (`secret: true` env values persist as
-- CredentialBroker handles only, never raw), and lifecycle status
-- (`awaiting_approval` at pin; `live` / `refused` / `stopped` via launch
-- decisions). Additive: new table only, existing rows and columns untouched.
-- Re-register upserts the named rows; no expiry/GC this slice.
CREATE TABLE mcp_servers (
    session_id TEXT    NOT NULL REFERENCES sessions (id),
    server_id  TEXT    NOT NULL,
    command    TEXT    NOT NULL,
    args_json  TEXT    NOT NULL,
    env_json   TEXT    NOT NULL,
    status     TEXT    NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (session_id, server_id)
) STRICT;
