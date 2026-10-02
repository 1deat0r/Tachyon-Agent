# 01: Fail-closed descriptor enforcement at launch

**What to build:** Stored MCP descriptors are untrusted at the use site, not just at register. The denylist becomes one deterministic name/prefix rule set shared by register and launch (loader hijack: `LD_PRELOAD`, `LD_LIBRARY_PATH`, `LD_AUDIT`, `GCONV_PATH`, `DYLD_*`; interpreter/startup: `BASH_ENV`, `ENV`, `SHELLOPTS`, `PS4`, `IFS`, `PYTHONPATH`, `PYTHONSTARTUP`, `NODE_OPTIONS`, `PERL5OPT`, `OPENSSL_CONF`, `SSL_CERT_FILE`, `SSL_CERT_DIR`, `GIT_SSH_COMMAND`, `GIT_CONFIG_*` prefix, plus same-class siblings `GIT_SSH`, `GIT_EXEC_PATH`, `GIT_TEMPLATE_DIR`, `JAVA_TOOL_OPTIONS`, `_JAVA_OPTIONS`, `RUBYOPT`), explicit entries may not override inherited allowlist location keys (`PATH`, `HOME`, `TMPDIR`, `LANG` + platform set — name-only rejection), and the full `check_mcp_descriptor` core re-runs on each stored row immediately before spawn: failure refuses with typed `invalid_mcp_descriptor`, zero processes started, existing launch-failure lifecycle semantics reused. Include the env-isolation regression pin: MCP child env contains the allowlist + pinned entries only (provider key absent), and `process.spawn` receipts never contain MCP secrets.

**Blocked by:** None (can start immediately)

**Status:** complete (`cargo verify` exit 0; boxes checked 2026-10-01)

- [x] Denylist is a single shared rule set consulted by both register-time validation and launch-time re-validation (no duplicated name lists)
- [x] Register rejects each denylisted name and each allowlist-override attempt by name, echoing only the name (never a value)
- [x] A row mutated directly in `state.db` to carry `LD_PRELOAD`/`BASH_ENV`/`PATH`-override (or any legacy-invalid env) refuses launch with typed `invalid_mcp_descriptor` **before any process starts**
- [x] Launch-time re-validation covers env name shape, denylist, value-size bounds, and allowlist-override rules — the full register core, not a subset
- [x] Valid legacy rows still launch unchanged (no regression on shipped mcp_gated_launch / mcp_pinned / mcp_mediated_call suites)
- [x] Env-isolation test: spawned MCP child env = inherited allowlist + pinned entries only (provider API key absent from child env); MCP secrets absent from `process.spawn` inline receipt and artifact spool
- [x] Tests at the gateway seam (mutated-row launch refusal with zero process; isolation pin) + unit tests for the shared denylist/override rules
