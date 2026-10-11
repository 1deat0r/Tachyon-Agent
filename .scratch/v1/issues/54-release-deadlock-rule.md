# TASK 54: Release deadlock rule (14-day approve-to-decline timeout)
**Status:** ready
**Blocked by:** 03
**What to build:** admission timeout recorded in V1_DEFINITION; task 47 unblocks if approved-but-stalled items decline by timeout.
**Verify:** rule in definition; task 47 edge updated.
## Small tasks
- [ ] S1 Record timeout rule · Verify: definition updated
- [ ] S2 Update task 47 edges · Verify: edge script passes
- [ ] S3 Commit · Verify: `cargo verify` exit 0
