# Milestone 21: deterministic production-driver replay of M20 failure classes

Replay the four retained M20 failures through the ONE production
driver (`tachyon_core::driver::drive`) with no live model calls.
This is development data for the next candidate selection;
production behavior, prompt, acceptance, model policy, and retry
policy stay unchanged. The unselected M18 guidance stays disabled.

Frozen inputs (SHA-256 in `M21_MANIFEST.json`, checked before any
regression runs; the regressions also pin the retained bytes by
BLAKE3 inside the test): the retained partial patch
(`STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.reserve.rs.txt`), its
unchanged release counterpart, the retained replay log, the
reservation-ledger fixture and solution, the production driver
sources, and the bench host.

Regressions, one per retained failure class:

1. Partial repair (`reservation-ledger/full/3`): the provider
   returns the exact retained reserve.rs patch (BLAKE3-pinned),
   bound to the broken fixture's base hash. Prove the mutation
   applies, the acceptance contract (`cargo test --offline
   --locked`, visible and holdout) rejects it with the retained
   failing tests, the run fails with `VerificationBlocked`, and
   the production host failure path (`mark_failed`, shutdown,
   `recover_task`) replays `recovered_failed`. Exactly
   `subject/src/reserve.rs` changes; every protected path stays
   byte-identical.

2. Transport error (`reservation-ledger/serial/3`): the provider
   fails with `ModelError::Transport` and unknown usage. Prove one
   attempt, no blind retry (only malformed output retries), a
   durable `model_attempt` record with unknown usage, zero
   workspace writes, no mutation directory, and the host failure
   path replaying `recovered_failed`.

3. Model-stage deadline, full mode (`reservation-ledger/full/4`):
   the provider parks past the configured model-stage deadline.
   Prove the deadline fires as `ModelError::Timeout` carrying the
   configured deadline value, one attempt, unknown usage, zero
   writes, and the durable `recovered_failed` replay.

4. Model-stage deadline, serial mode (`reservation-ledger/serial/4`):
   regression 3 through the serial evidence mode.

The deadline is shortened for test wall-clock; the mechanism is the
production `timeout_at` path with the same typed error, usage
accounting, and retry policy as the frozen 120000 ms protocol.
Each regression runs in a fresh scratch copy of the checked-in
fixture; the checked-in tree is never patched.

Also root-cause the intermittent preflight verification performance
failure (`STATEFUL_PREFLIGHT_PERF_FAILURE.log.txt`) as a bounded
context diagnosis: measure isolated and full-gate behavior, preserve
the failure evidence and its limits, and change no production code,
threshold, feature selection, CPU affinity, or timeout. Do not claim
the issue is fixed or that a larger budget explains it.

Run VERIFY and FULL. Publish the regressions, provenance, and a
bounded report. The failure classes become future development data;
acceptance, semantic repair retries, and prompt changes stay out of
scope until a candidate is selected against this evidence.
