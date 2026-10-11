# TASK 36: Plugin marketplace — implement
**Status:** ready
**Blocked by:** 35
**What to build:** the approved marketplace with untrusted-code containment; every checklist field filled, security review explicit.
**Verify:** `cargo verify` exit 0; security review recorded; checklist complete.

## Small tasks (each = one commit, in order)
- [ ] S1 Implement with containment · Verify: unit + integration tests green
- [ ] S2 Security review (untrusted code) · Verify: review recorded
- [ ] S3 Default tier · Verify: `cargo verify` exit 0

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
