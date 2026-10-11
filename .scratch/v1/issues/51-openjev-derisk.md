# TASK 51: OpenJEV de-risk (fallback on default path)
**Status:** ready
**Blocked by:** 03
**What to build:** JudgmentProvider fake/evidence fallback exercised on default CI path; openjev.sh never required in docs.
**Verify:** fallback test green on default `cargo verify`; docs state optional.
## Small tasks
- [ ] S1 Fallback test on default path · Verify: test green
- [ ] S2 Docs state optional · Verify: grep green
- [ ] S3 Commit · Verify: `cargo verify` exit 0

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
