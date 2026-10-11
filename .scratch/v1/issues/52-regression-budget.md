# TASK 52: Regression budget (nightly/perf trend, no noisy gates)
**Status:** ready
**Blocked by:** 03
**What to build:** `docs/milestones/REGRESSION_BUDGET.md` + nightly trend wiring; no wall-clock microbenchmarks blocking on noisy shared CI.
**Verify:** doc committed; CI config tracks trends.
## Small tasks
- [ ] S1 Doc + thresholds · Verify: doc committed
- [ ] S2 Trend wiring · Verify: config present
- [ ] S3 Commit · Verify: `cargo verify` exit 0

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.

Numeric budget: p95 delta threshold + sample count + trend alert; controlled host required; no blocking wall-clock microbenchmarks on shared CI.
