# TASK 50: Distribution wedge (ACP compat + launch story)
**Status:** ready
**Blocked by:** 03
**What to build:** ACP adapter FULL green + `docs/milestones/DISTRIBUTION_WEDGE.md` (OSS launch story or kernel-embed path).
**Verify:** ACP tests green; doc committed.
## Small tasks
- [ ] S1 ACP compat proof · Verify: tests green
- [ ] S2 Launch story doc · Verify: doc committed
- [ ] S3 Commit · Verify: `cargo verify` exit 0

**Hardening (E13-E18, task 56):** validated-IR lowering proof; access-set conflict-freedom + one-logical-writer proof; M9 wrong-patch-refuses-Completed re-proof; per-effect idempotency type+key+reconcile+retry rule + UnknownAfterCrash fixture; consequential via prepare/commit+receipt; same-model MUST (waiver 4/5 board + expiry); median+p95+N+env+hardware pin; manifest schema+hash+validator (`node scripts/m22_manifest.mjs` pattern); report requires adopt/reject verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
