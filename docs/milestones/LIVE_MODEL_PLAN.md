# Live-model benchmark leg — plan

## Goal

Answer one question: does `tachyon-full` beat the in-tree serial reference
on a representative task once the model call costs real time? The M14 matrix
(150/150 verified, serial fastest everywhere) cannot answer it because the
scripted provider costs ~0.01 ms per call. This leg re-runs one fixture cell
pair with a live model behind the existing `ModelProvider` trait.

## Scope (deliberately narrow)

- One fixture: `auth-refresh` (Class C, single `change_paths` entry,
  broken-first + solution self-check already proven by `fixture-check`).
- Two modes only: `full` vs `serial`. No alias modes, no legs, no other
  fixtures. If the pair shows no signal, stop — do not expand.
- n = 20 per mode (nearest-rank p95 needs n ≥ 20; MVP report limitation #2).
- One pinned model: record model ID + date in the report (docs/11 #11,
  AD-015 same-model rule). Same model, same environment, both modes —
  the only delta is the harness path.

## Method

1. Add a `live` provider mode to the `bench_matrix` host: construct
   `OpenAiCompatProvider` (existing OpenAI-compatible adapter,
   `crates/tachyon-models/src/openai_compat.rs`) against a local
   inference endpoint (default `http://localhost:11434`) instead of
   `FakeModelProvider`. Keep the `TimedProvider` wrapper so model-call
   durations stay measured, not assumed.
2. The live model must emit one JSON `AgentDecision`
   (`propose_execution` with `mutation.patch` ops carrying `base_hash`).
   The driver system prompt already specifies this shape; boundary repair
   handles fenced blocks, anything else is a provider failure and counts
   as `verification_failures`, never a silent pass. Expect prompt
   iteration: budget 2–3 shaping rounds on a single scratch sample before
   the measured run.
3. Run `M14_SAMPLES=20` over `auth-refresh × {full, serial}` on one quiet
   Linux host. Fresh scratch workspace per sample (existing behavior).
   Record provider endpoint, model ID, date, and per-sample cost.
4. Extend `m14_matrix_check.mjs` minimally (or a sibling `live_check.mjs`):
   same per-sample contract, plus `usage_provenance == provider_reported`
   and real token counts. Write the aggregate to
   `docs/milestones/LIVE_MODEL_MATRIX.json` — never touch the frozen
   `M14_MATRIX.json`.
5. Report in `docs/milestones/LIVE_MODEL_REPORT.md`: verified success,
   median/p95 completion and TTFR per mode, model-call durations, tokens,
   cost. State the comparison honestly: if serial still wins, say so —
   that finding rewrites the roadmap (see below).

## Cost and safety

- Live calls cost real money/latency: 40 measured samples + shaping
  rounds. Cap retries at the driver default; a failed sample is data.
- API key flows through the existing `api_key_env` / resolved-key path
  (fail-closed, redaction-registered). Never paste the key into the
  report or the plan — only the model ID, date, and totals.
- No spec or harness changes beyond the provider switch and the checker
  extension. No §46 capability work regardless of outcome.

## Decision rule (write it down before running)

- `full` beats serial on completion p50/p95 at equal verified success →
  proceed to one §46 wedge (recommendation: ACP adapter) or expand to a
  second fixture class.
- Serial still wins or ties → do not build new subsystems. Investigate
  where `full` pays (journal + verify-tail overhead is the prime
  suspect from M14) and fix the overhead first.
- Verified success drops in either mode → the live-model prompt contract
  is the problem, not the harness. Fix shaping, re-run, do not publish
  comparisons until both modes verify reliably.

## Out of scope

- n ≥ 20 re-measurement of scripted cells, microsecond TTFR breakdown,
  external-harness comparison, any §46 feature work.
