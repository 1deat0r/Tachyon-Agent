# TASK 56: Hardening amendment (board G0 round-1 blocks)
**Status:** ready
**Blocked by:** 03
**What to build:** amend V1_DEFINITION (E13-E18) + affected tasks:
- E13 IR+single-writer: every deferred implement proves validated-IR lowering, access-set conflict-freedom, one-logical-writer; report gates add IR regression test.
- E14 Verification re-proof: M9 wrong-patch-refuses-Completed re-proof for wfc/dst/brw/swm/smr/ACP; M22 task 05 adds M9/M12 re-proof.
- E15 No-blind-replay: per-effect idempotency type+key+reconcile+retry rule; UnknownAfterCrash fixture for mkt/brw/swm; consequential effects via prepare/commit+receipt.
- E16 Security containment: mkt sandbox+signing+grant-scope rule; brw effect taxonomy+allow/deny+approval binding+untrusted-input isolation; credential broker-only + redaction tests; swarm principal isolation.
- E17 Measurement discipline: same-model MUST (waiver needs 4/5 board + expiry); median+p95+N+env+hardware pin mandatory; numeric regression budget (p95 delta + sample count + trend alert); controlled-host + ban on blocking wall-clock microbenchmarks on shared CI; frozen manifest schema+hash+validator; report gates require adopt/reject rule verbatim + AcceptanceContract/verifier results (no self-report); implement S3 = FULL tier.
- E18 Scope mapping: charter 8 non-goals mapped to ADRs 11-19 (mobile/telegram = new clients, require ADR); per-item gates mandatory; 14-day timeout enforced in task 47 edges.
**Verify:** V1_DEFINITION E13-E18 committed; tasks 05,08,20-46,47,50-52 amended; `cargo verify` exit 0.
