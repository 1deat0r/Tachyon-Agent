# TASK 53: Security/release gate (deny, platform, freshness)
**Status:** ready
**Blocked by:** 03
**What to build:** `cargo deny`, platform tests, docs-freshness green on release tree; security seat decisive for marketplace/browser items.
**Verify:** deny + freshness green; report committed.
## Small tasks
- [ ] S1 Deny + platform run · Verify: exit 0
- [ ] S2 Freshness green · Verify: test green
- [ ] S3 Commit · Verify: `cargo verify` exit 0
