# TASK 47: Release mechanics (1.0.0)
**Status:** ready
**Blocked by:** 07, 10, 50, 51, 52, 53, 54, 55, and each activated *-report-gate ticket (22, 25, 28, 31, 34, 37, 40, 43, 46); declined or timeout-declined items activate nothing and block nothing
**What to build:** version 1.0.0 consistent across the workspace, CHANGELOG release section folded, release commit, docs-freshness green.
**Verify:** version grep consistent; `cargo test -p tachyon-app --test docs_freshness` green.
## Small tasks (each = one commit, in order)
- [ ] S1 Version 1.0.0 + CHANGELOG section · Verify: freshness tests green
- [ ] S2 Release commit · Verify: `cargo verify` exit 0

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.

Timeout: approved-but-stalled items decline after 14 days (E18).
