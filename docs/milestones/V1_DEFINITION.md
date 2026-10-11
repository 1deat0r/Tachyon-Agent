# V1 Definition (frozen for G0)

## Exit criteria (numbered, measurable)

- E1 M22 candidate enabled or dropped with evidence (tasks 04-07).
- E2 MVP exit 11/11 closed or formally waived with same-model measurements (task 08).
- E3 Preflight perf follow-up proven with before/after data (task 09).
- E4 G2 board verdict recorded (task 10).
- E5 Deferred-scope ADRs 11-19 each approved or declined with rationale.
- E6 Approved deferred items pass plan/manifest, implement, report+FULL gates.
- E7 Distribution wedge shipped: ACP compat green + OSS launch story doc (task 50).
- E8 OpenJEV de-risk: fake/evidence fallback exercised on default CI path (task 51).
- E9 Regression budget: nightly/perf trend tracking documented, no noisy blocking microbenchmarks (task 52).
- E10 Security/release gate: `cargo deny`, platform tests, docs-freshness green on release tree (task 53).
- E11 Release mechanics 1.0.0 consistent, FULL green, tag pushed (tasks 47-49, 54).
- E12 Live-trial infra-failure rollback rule applied if trials abort (task 55).

## Versioning

0.x series until E1-E12 pass. 1.0.0 only via tasks 47-49.

## Board ritual

G0 (definition), G1 (M22 enable/drop), G2 (exit closure). Five seats:
correctness, security/effects, verification, latency, scope.
Same frozen snapshot for all seats. Blocks become numbered gap tickets.

## Admission rule

Deferred item enters v1 only via ADR approve. Decline activates nothing
and blocks nothing. Approved-but-stalled items time out in 14 days to
decline (task 54) to prevent release deadlock.

## Hardening (G0 round-1, task 56)

- E13 IR + single-writer proof on every deferred implement + IR regression in report gates.
- E14 M9 verification re-proof on wfc/dst/brw/swm/smr/ACP; M22 task 05 adds M9/M12 re-proof.
- E15 Per-effect idempotency + UnknownAfterCrash fixture; consequential via prepare/commit+receipt.
- E16 Marketplace sandbox+signing+grant scope; browser taxonomy+allow/deny+approval; credential broker-only + redaction; swarm isolation.
- E17 Same-model MUST (waiver = 4/5 board + expiry); median+p95+N+env+hardware mandatory; numeric budget + controlled host + no noisy blocking gates; manifest schema+hash+validator; adopt/reject verbatim; FULL tier for implement.
- E18 Charter non-goal → ADR map; per-item gates; 14-day timeout in task 47.

Charter 8 non-goals map: swarm->18, marketplace->16, browser->17, distributed->13, self-modifying->19, provider-CLI/MCP/Jev-wrapper excluded by design; mobile(14)/telegram(15) = new gateway clients requiring ADR; workflow-compilation(11)/custom-vdb(12) = deferred extensions requiring ADR.
