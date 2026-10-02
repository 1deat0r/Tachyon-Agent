-- MCP server liveness (issue #57, ACP MCP-stdio slice ticket 02;
-- ADR-0005 blocker 3). Two additive columns on the ticket-01 table:
-- `version` is the `initialize`-negotiated protocol version (empty
-- unless the server is `live`) and `tools_json` is the recorded
-- `tools/list` inventory JSON (`[]` unless live). Existing rows and
-- columns are untouched; rows pinned by ticket 01 keep their values.
ALTER TABLE mcp_servers ADD COLUMN version TEXT NOT NULL DEFAULT '';
ALTER TABLE mcp_servers ADD COLUMN tools_json TEXT NOT NULL DEFAULT '[]';
