# M28 Plan: Live Failure-Path Turns (frozen)

## Purpose

M27 exercised only the happy path. This batch proves the fail-closed
taxonomy live: two turns with bad `resource_link` blocks through the
full ACP boundary, each expected to refuse typed with zero tasks
created and zero model calls (resolution precedes CreateTask and any
model contact). EVALUATION, not a candidate: no production change, no
adopt/reject — pre-registered reporting rules only.

## Cells (2 rows, single arm)

Fresh gateway + adapter + fixture copy per turn, same model/proxy/
temperature as M27:

- s1 `sibling-escape`: link to an existing directory sibling of the
  workspace copy (`file://<dir>/outside/secret.txt`) — canonical path
  outside the pinned root → expected `out_of_root`.
- s2 `oversized-in-ws`: link to a 64KiB+1 file written into the
  workspace copy before prompting → expected `oversized`.

Recorded per row: adapter error code + `data` taxonomy marker, task
absence (no turn in session history), model-call count (expected 0),
wall ms. Success criterion is per-row exact: the expected taxonomy
marker, no task, no calls. Any verified turn, any task creation, or
any model call on these rows is a FAIL of the confinement claim.

## Reporting rules

- Per-row outcomes + pooled exact-match rate; no generalization.
- Failure taxonomy reported as-observed; any deviation from the
  expected markers is a finding, not noise.
- Latency with host-load disclaimer; no speed claim. Cost UNKNOWN.

## Rollback (task 55)

Proxy outage, 402s, harness failure: retain, void, publish regardless.

## Frozen inputs

This plan, `crates/tachyon-acp/tests/session_prompt_live_reslink_fail.rs`,
`scripts/m28_manifest.mjs`, `scripts/m28_run.mjs`,
`docs/milestones/M28_MANIFEST.json`, `fixtures/duplicate-range`,
`crates/tachyon-acp/src/server.rs`, `crates/tachyon-acp/src/turn.rs`,
`crates/tachyon-acp/tests/common/mod.rs`.
No production change for this measurement.

## Hardening

New test file only (`#[ignore]`, env-gated); VERIFY green required.
Same-model continuity (no arms to mix).
