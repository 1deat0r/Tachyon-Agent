# TASK 55: Live-trial infra-failure rollback rule
**Status:** ready
**Blocked by:** 03
**What to build:** rule for provider outage / unknown-usage abort: retain evidence, mark trial void, no adopt; documented in M22 report template.
**Verify:** rule doc committed; report template references it.
## Small tasks
- [ ] S1 Rule doc · Verify: committed
- [ ] S2 Template reference · Verify: grep green
- [ ] S3 Commit · Verify: `cargo verify` exit 0
