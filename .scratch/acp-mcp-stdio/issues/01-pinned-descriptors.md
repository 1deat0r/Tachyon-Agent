# 01: Pinned MCP server descriptors (validate + durably pin, no launch)

**What to build:** A gateway client can register client-supplied MCP server descriptors against a session and list them back. Every descriptor is validated as untrusted input — relative commands, oversized args/env, bad env names, and dangerous variables fail with a typed error and pin nothing. Valid descriptors persist in a new store table keyed `UNIQUE(session_id, server_id)` and `ListMCPServers` reports them with secret values shown as broker handles only. Nothing spawns in this ticket: servers record status `awaiting_approval` (parked-by-default per the spec Approval section; the earlier `registered` wording was superseded in ticket 02 and no `registered` rows were ever shipped — the slice lands as one uncommitted diff), and launch/call commands do not exist yet.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [x] `RegisterMCPServers { session_id, servers }` validates each descriptor: `server_id` opaque 1..=64 bytes, NUL-free, no `/` (the authorize scope `<server-id>/launch` splits on `/`); `command` absolute path; `args` ≤32 entries each ≤4KiB NUL-free; env names `[A-Za-z_][A-Za-z0-9_]*`, values ≤16KiB; `LD_PRELOAD` / `LD_LIBRARY_PATH` / `DYLD_*` rejected; unknown session → `unknown_session`
- [x] Malicious descriptors fail with typed `invalid_mcp_descriptor` and pin nothing (no partial rows)
- [x] Valid set pins durably (new table, additive migration, `UNIQUE(session_id, server_id)`); re-registering a live set replaces only via explicit contract (document the chosen semantics in the ticket)
- [x] `secret: true` env values persist as broker handles only — raw values appear nowhere in store rows, logs, or list output
- [x] `ListMCPServers { session_id }` returns descriptors with handles-only secrets and status `awaiting_approval`
- [x] Tests at both seams: gateway round-trip (validation rejections, pin-then-list, handle-only listing, unknown session) and store (uniqueness under race, handles-not-raw after reopen)

**Re-register semantics (chosen contract):** `RegisterMCPServers` upserts
the named `server_id` rows only — each named row is inserted as
`awaiting_approval`, or replaced in place when `(session_id, server_id)`
already exists (whole-row replace: command, args, env-with-handles,
status reset to `awaiting_approval`, version/tools cleared; any
still-running child from a superseded pin is reaped). Unnamed rows are
left untouched; there is no delete and no whole-set replace, so a
re-register can never silently drop a pinned server. A duplicate
`server_id` inside one call is rejected as
`invalid_mcp_descriptor` with nothing pinned.
