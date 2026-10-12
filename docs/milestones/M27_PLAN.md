# M27 Plan: Live ResourceLink Turns Through the Adapter (frozen)

## Purpose

First live measurement of the ResourceLink path implemented under the
current goal (ADR-0018, resolve+embed in `turn.rs`): prompt turns that
carry a `resource_link` block through the full ACP boundary. M25 proved
text-only turns verify; this batch proves the link resolve+embed path
works live end to end. EVALUATION, not a candidate: no production
change, no adopt/reject — pre-registered reporting rules only.

## Cells (3 rows, single arm)

3 sequential turns, fresh gateway + adapter + fixture copy each, on
`duplicate-range` (class C, independent holdout grader):

- Prompt: the frozen M25 objective text PLUS one `resource_link`
  block pointing at `subject/tests/visible.rs` inside the pinned
  workspace copy (143 bytes, relevant evidence: the visible contract
  for the defect).
- Model `meta/muse-spark-1.3-contributor` via local OMP proxy (keyless),
  temperature 0 (harness wire default).

Recorded per row (same schema as M25 plus `links: 1`): adapter
`end_turn`, task status via gateway `GetTask` (`Completed` ⟺
verified), model calls, input/output tokens (`provider_reported`),
wall ms. Resolution failures would surface as `error` rows with the
confined taxonomy (`unresolvable`, `out_of_root`, …) — reported
as-observed.

## Reporting rules

- Per-row outcomes + pooled verified count; no cross-task or
  cross-client generalization (one fixture, one link target, one
  model, one night).
- Failure taxonomy counts every failed attempt/turn class; retry
  behavior as-observed under the shared malformed-only policy.
- Latency as median + range with host-load disclaimer; no speed claim.
- Cost UNKNOWN (pricing unpinned): token counts only, never dollars.

## Rollback (task 55)

Proxy outage, 402s, unknown-usage abort, or adapter-harness failure:
retain evidence, mark void, no conclusions. Rows + manifest + report
published regardless.

## Frozen inputs

This plan, `crates/tachyon-acp/tests/session_prompt_live_reslink.rs`,
`scripts/m27_manifest.mjs`, `scripts/m27_run.mjs`,
`docs/milestones/M27_MANIFEST.json`, `fixtures/duplicate-range` (+
`fixtures/solutions/duplicate-range` for the self-check reference),
`crates/tachyon-acp/src/server.rs`, `crates/tachyon-acp/src/turn.rs`,
`crates/tachyon-acp/tests/common/mod.rs`.
No production change for this measurement.

## Hardening

New test file only (`#[ignore]`, env-gated, never runs in
`cargo verify`); VERIFY green required (compile + clippy). The live
run goes through `cargo test --release -- --ignored`, never the
default suite. Same-model continuity with M25 (no arms to mix).
