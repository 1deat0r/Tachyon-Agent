# TASK 41: Agent swarms — plan + manifest freeze
**Status:** ready
**Blocked by:** 18 (activates only on extraordinary ADR approval; decline-by-default)
**What to build:** frozen milestone plan + input manifest for agent swarms, following the M16–M21 protocol. Plan must preserve the single-writer invariant across every agent or prove why it still holds.
**Verify:** manifest script validates; plan committed before any implementation.

## Small tasks (each = one commit, in order)
- [ ] S1 Milestone plan (single writer preserved) · Verify: scope + frozen inputs listed
- [ ] S2 Manifest + freeze · Verify: manifest validates

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
