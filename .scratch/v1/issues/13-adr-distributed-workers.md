# TASK 13: ADR — distributed workers for v1
**Status:** ready
**Blocked by:** 03
**What to build:** ADR deciding whether distributed workers enter v1 (touches single-writer + no-conflicting-access invariants): approve (spawns 26–28) or decline with rationale.
**Verify:** ADR file with Status + verdict; approval activates 26–28, decline activates nothing.

## Small tasks (each = one commit, in order)
- [ ] S1 Draft ADR (all required sections) · Verify: section check green
- [ ] S2 Review vs core invariants (single writer, access sets) · Verify: impact stated
- [ ] S3 Verdict: approve (spawn 26–28) or decline with rationale · Verify: committed
