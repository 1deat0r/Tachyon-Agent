# TASK 34: Telegram client — report + board gate
**Status:** ready
**Blocked by:** 33
**What to build:** milestone report with measurements and a recorded board verdict; FULL tier green.
**Verify:** `cargo verify full` exit 0; verdict in the report.

## Small tasks (each = one commit, in order)
- [ ] S1 Report with measurements · Verify: report committed
- [ ] S2 Board gate + FULL · Verify: verdict recorded, full exit 0

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
