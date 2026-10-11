# TASK 08: 11th MVP exit — measure or waive
**Status:** ready
**Blocked by:** 03
**What to build:** the PARTIAL speed exit condition closed — same-model harness measurement proving the claim, or a formal evidence waiver recorded in the exit ledger. No cross-model speed claims (repo performance discipline).
**Verify:** 11/11 ledger or waiver recorded with measurements.

## Small tasks (each = one commit, in order)
- [ ] S1 Same-model harness measurement · Verify: instrumentation + benchmark committed
- [ ] S2 Close or waive with evidence · Verify: exit ledger updated

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
