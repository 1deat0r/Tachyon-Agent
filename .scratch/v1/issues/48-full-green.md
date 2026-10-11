# TASK 48: FULL green on the 1.0.0 release tree
**Status:** ready
**Blocked by:** 47
**What to build:** `cargo verify full` exit 0 on the exact release commit, with every GATES.json marker and the perf-gate markers recorded (the v0.0.1 ritual).
**Verify:** exit 0; all gate markers present; zero FAILED.

## Small tasks (each = one commit, in order)
- [ ] S1 FULL run · Verify: exit 0 with markers (rerun on transient load flakes per M21 precedent, disclosed)

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
