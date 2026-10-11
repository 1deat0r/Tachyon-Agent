# Milestone 21 report: deterministic production-driver replay of M20 failure classes

Date: 2026-10-11.
Plan: `docs/milestones/M21_PLAN.md` (frozen in
`78ef4e1` before any implementation).
Frozen inputs: `docs/milestones/M21_MANIFEST.json`
(26 files, SHA-256; validated by
`node scripts/m21_manifest.mjs`).
Regressions: `crates/tachyon-core/tests/m21_replay.rs`.

**All four retained M20 failure classes replay
deterministically through the ONE production driver
(`tachyon_core::driver::drive`) with no live model calls.
Production behavior, prompt, acceptance, model policy, and
retry policy are unchanged. The M18 guidance stays
disabled.**

## Method

Each regression builds a fresh scratch copy of the
checked-in `reservation-ledger` fixture, creates a real
task on the production supervisor path, and drives it
through the production driver with a scripted provider.
The retained failure bytes are pinned twice: SHA-256 in
the manifest protocol and BLAKE3 inside the test
(`RETAINED_PATCH_BLAKE3`, `RETAINED_RELEASE_BLAKE3`,
`RETAINED_LOG_BLAKE3`, `BROKEN_RESERVE_BLAKE3`), so any
drift in the frozen evidence fails the regression loudly.
The checked-in tree is never patched.

The deadline regressions use a 250 ms model-stage
deadline (`REPLAY_DEADLINE_MS`); the frozen M20 protocol
used 120000 ms. The production `timeout_at` path, typed
error, usage accounting, and retry policy are
value-independent, so the mechanism is the same; only the
wall-clock duration differs. This deviation is declared in
the plan, not discovered after the fact.

## Results

| Cell | Failure class | Result |
| --- | --- | --- |
| `reservation-ledger/full/3` | partial repair | replayed and rejected |
| `reservation-ledger/serial/3` | transport error | replayed, zero writes |
| `reservation-ledger/full/4` | model-stage deadline | replayed, zero writes |
| `reservation-ledger/serial/4` | model-stage deadline | replayed, zero writes |

`cargo test -p tachyon-core --test m21_replay`:
**4 passed, 0 failed** (≈ 0.9–2.6 s total).
`cargo verify` (default tier): exit 0.
`cargo verify full`: exit 0 — every `GATES.json` gate
(fixture, 150-sample matrix, validation, security and
recovery suites, projection, report, progress, changelog,
reconciliation), the perf gate with all required markers,
and `m21_replay` 4/4; strict Clippy clean.

### Partial repair (`full/3`)

The provider returns the exact retained `reserve.rs`
patch bound to the broken fixture's base hash. The
regression proves, end to end:

- The mutation applies: exactly
  `subject/src/reserve.rs` changes, within the
  descriptor's authorized `change_paths`; every protected
  path stays byte-identical.
- Broken-first holds: the fixture fails its own
  acceptance before any patch.
- The acceptance gate rejects the repair:
  `drive` fails with `DriveError::Core(CoreError::VerificationBlocked)`
  carrying `required acceptance failed`; completion never
  comes from model self-report.
- The rejection is the retained failure class: in the
  patched scratch workspace, `visible_reserve_is_atomic`
  passes, `visible_release_is_atomic` fails, and all
  three hidden holdout tests
  (`integer_limits_and_error_precedence_preserve_state`,
  `smallest_sku_precedence_does_not_follow_batch_order`,
  `sequences_match_independent_state_oracle`) fail —
  the same failing tests as the retained replay log.
- One model call, one `model_attempt` record, no retry,
  provider-reported usage.
- The production host failure path (`mark_failed`,
  shutdown, `recover_task`) replays `recovered_failed`.

### Transport error (`serial/3`)

The provider fails with `ModelError::Transport` and
default (unknown) usage. The regression proves: exactly
one attempt (only malformed output retries — a transport
failure is not retried), a durable `model_attempt`
record with error class `transport` and
`UsageProvenance::Unknown` with no counters, no mutation
directory, zero workspace changes, protected paths
byte-identical, and the host failure path replaying
`recovered_failed`.

### Model-stage deadline (`full/4` and `serial/4`)

The provider parks for 30 s past the configured deadline.
The production `timeout_at` path fires
`ModelError::Timeout { timeout_ms }` carrying the
configured deadline value. Both evidence modes prove: one
attempt, a durable `model_attempt` record with error
class `timeout` and unknown usage (the driver synthesizes
the timed-out invocation; it never invents counters), no
mutation directory, zero workspace changes, protected
paths byte-identical, and the host failure path
replaying `recovered_failed`. If the deadline failed to
fire, the provider would return `ModelError::Internal`
after 30 s — a loud failure, never a silent pass.

## Preflight performance failure

The intermittent verification performance failure is
root-caused as an environmental, load-sensitive
wall-clock measurement: the test measures real
`python3 -c pass` spawn latency against a budget that
scales with the `/proc` process count (drain-scan cost)
but not with machine load. At the M20 failure, spawn
latency roughly doubled (39.99 ms vs ≈ 20.8 ms isolated)
and crossed the then-current 31.512 ms budget. The same
binary passed in isolation, on rerun, and in two
full-gate plus three isolated runs during this diagnosis.
No production code, threshold, feature selection, CPU
affinity, or timeout was changed. Full evidence, limits,
and candidate follow-ups:
`docs/milestones/PREFLIGHT_PERF_DIAGNOSIS.md`.

## Limits

- During this milestone's FULL runs, the workspace-test
  step saw three transient failures under parallel load on
  the shared development host (load average 20–35 from other
  tenants), all in spawn/kill-heavy tests untouched by this
  milestone, which changes no production source: one SIGKILL
  of a `tachyon-acp` test binary, one 20 s seam-wait expiry
  in `tachyon-core --test evidence_crash` (passes alone in
  0.03 s), and three gateway-endpoint waits expiring in
  `tachyon-app --test cli_surface` (passes alone in 0.20 s).
  Each passes alone and in lighter runs; a fourth FULL run
  at lower load passed end to end (exit 0: every
  `GATES.json` gate, the perf gate, and `m21_replay` 4/4).
  No code change was made in response — the same
  environmental class as the preflight issue above.
- The regressions prove mechanism and evidence handling,
  not provider behavior: a scripted provider replays the
  retained classes; the upstream transport failure and
  provider delay remain unexplained, as in M20.
- The shortened deadline is a declared deviation for test
  wall-clock; the frozen 120000 ms value is unchanged in
  the protocol and in `RuntimeBounds::default()`.
- The diagnosis did not reproduce the preflight failure
  on demand and captured no system metrics during the
  original failure; that limit is preserved in the
  diagnosis document.
- Five-sample M20 cells still support no true reliability
  probability; this milestone adds no product-score claim.

## Next step

The four failure classes are now deterministic development
data. A prompt, retry, or deadline candidate can be
selected and measured against these regressions plus new
frozen live trials — with acceptance, semantic repair
retries, and production behavior unchanged until such a
candidate is deliberately chosen and gated.
