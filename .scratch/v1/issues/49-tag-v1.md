# TASK 49: Tag v1.0.0, push, record
**Status:** ready
**Blocked by:** 48
**What to build:** annotated tag v1.0.0 on the release commit, pushed to the canonical remote, recorded in PROGRESS.md (the v0.0.1 ritual).
**Verify:** `git ls-remote --tags origin` shows v1.0.0 on the release commit.

## Small tasks (each = one commit, in order)
- [ ] S1 Annotated tag + push · Verify: ls-remote confirms
- [ ] S2 PROGRESS record · Verify: freshness tests green

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
