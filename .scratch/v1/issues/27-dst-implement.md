# TASK 27: Distributed workers — implement
**Status:** ready
**Blocked by:** 26
**What to build:** approved distributed workers preserving one logical writer and conflict-free access sets, every checklist field filled.
**Verify:** `cargo verify` exit 0; checklist complete, no blank fields.

## Small tasks (each = one commit, in order)
- [ ] S1 Implement per checklist · Verify: unit + integration tests green
- [ ] S2 Invariant proof (single writer, access sets) · Verify: dedicated tests green
- [ ] S3 Default tier · Verify: `cargo verify` exit 0

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
