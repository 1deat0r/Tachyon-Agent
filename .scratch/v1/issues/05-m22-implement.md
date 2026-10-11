# TASK 05: Implement M22 candidate; M21 regressions stay green
**Status:** ready
**Blocked by:** 04
**What to build:** candidate code behind the flag; the four deterministic failure-class replays still prove typed refusal/timeout, one attempt, unknown usage, zero writes, durable failed recovery.
**Verify:** `cargo test -p tachyon-core --test m21_replay` 4/4; `cargo verify` exit 0.

## Small tasks (each = one commit, in order)
- [ ] S1 Implement candidate behind flag · Verify: unit tests green
- [ ] S2 M21 regression run · Verify: m21_replay 4/4
- [ ] S3 Full default tier · Verify: `cargo verify` exit 0

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
