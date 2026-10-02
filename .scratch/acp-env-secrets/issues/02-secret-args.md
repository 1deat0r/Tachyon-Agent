# 02: Secret-marked MCP args end-to-end

**What to build:** `args` gain `secret: true` marking symmetric with env entries: the wire shape becomes `{value, secret}` entries (legacy plain-string JSON rows parse as all-`secret: false`, no migration — `args_json` is free TEXT JSON), secret values register with the credential vault in the same pin transaction as env secrets and persist as credential handles only, launch resolves handles via `use_handle` into the child argv (missing handle after restart ⇒ typed `mcp_spawn_failed`, zero process), and list output, call/launch receipts, and durable files show handles — never raw arg bytes. Bounds (≤32 args, ≤4 KiB/arg, NUL-free) apply to entries unchanged. `command` stays raw: an absolute path is a location, not a credential.

**Blocked by:** 01 (shares the descriptor validation/launch core that must re-validate args entries at the use site too)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-01)

- [x] Wire: `RegisterMCPServers.servers[].args` accepts `{value, secret}` entries; legacy array-of-strings rows and requests still parse (secret defaults false)
- [x] Register-time validation applies existing arg bounds/NUL rules to entries and registers `secret: true` values with the vault in the same transaction as env secrets (raw value never written to `args_json`)
- [x] `ListMCPServers` returns credential handles for secret args (and for the redacted marker shape consistent with env), raw bytes for non-secret args, never a raw secret arg
- [x] Launch re-validates args entries (bounds, NUL, secret-handle presence) immediately before spawn; resolves secret handles from the vault into argv; missing handle ⇒ typed `mcp_spawn_failed`, no process started
- [x] Restart fail-closed: after gateway restart with a fresh vault, a pinned secret arg refuses launch exactly like a secret env does today
- [x] Raw secret arg bytes absent from `state.db`, `state.db-wal`, list frames, and receipts (grep-the-bytes assertions)
- [x] Child process actually receives the raw secret in its argv (fake stdio child echoes a redacted receipt proving delivery)
- [x] Tests at the gateway seam (pin → list → restart → launch → call lifecycle) + store row-shape assertions + unit tests for legacy parse
