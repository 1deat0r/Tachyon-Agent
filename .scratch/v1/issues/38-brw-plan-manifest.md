# TASK 38: Browser/computer use — plan + manifest freeze
**Status:** ready
**Blocked by:** 17 (activates only on extraordinary ADR approval; decline-by-default)
**What to build:** frozen milestone plan + input manifest for browser/computer use, following the M16–M21 protocol. Plan must contain the effect surface (unbounded outside-world effects) explicitly.
**Verify:** manifest script validates; plan committed before any implementation.

## Small tasks (each = one commit, in order)
- [ ] S1 Milestone plan (effect surface contained) · Verify: scope + frozen inputs listed
- [ ] S2 Manifest + freeze · Verify: manifest validates

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
