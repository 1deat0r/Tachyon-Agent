# M23 Candidate Plan (frozen)

## Candidate

Framing fix for the EXIT-11 / M22 output-framing failure mode: raise the
model output reserve and add one terminal JSON-only reminder. No retry,
deadline, verification, or access-set change.

- New flag `RuntimeBounds::m23_framing_fix: bool`, default `false`.
- When `false` (default): zero behavior change. Output budget stays 4096,
  system prompt byte-identical, no extra call, no retry, no deadline
  change, no new mutation, no prompt change.
- When `true` (opt-in trials only): output budget 4096 → 8192 (ContextSlice
  output reserve and wire `max_tokens` both 8192; still fits the 32768
  default context window with the same fit check), plus one appended
  system-prompt sentence: `Return only the JSON object. Do not emit
  reasoning, prose, or fences.` Retry stays malformed-only (max 2
  attempts), deadline stays 120 s, `response_format` handling unchanged
  (`json_object` already sent when the provider advertises
  StructuredOutput).

## Why this candidate

EXIT-11 (waived with expiry): full 0/3 vs serial 1/3, all failures
`malformed_output` (`invalid_json`) with `output_tokens` exactly 4096 —
truncation, not verification or scheduling. M22 Spark batch: dominant
failure mode is first-attempt empty/invalid JSON, second attempt usually
succeeds; error rows die on `interrupted` second attempts when the
malformed-output budget is spent. The reasoning model emits reasoning +
patch text that exceeds the 4096 reserve. Deterministic code cannot author
the patch; the harness can only fit the budget and frame the output, while
acceptance/verification still gates truth (JEV never verifies truth).

## New-capability checklist

- Why not deterministic: patch content needs model judgment; budget and
  framing are harness shaping, verification gates completion.
- Input/output schema: `ModelRequest` / `AgentDecision` unchanged; only
  `max_output_tokens` 4096 → 8192 and one system suffix sentence.
- Access set: unchanged. No new files, capabilities, or evidence paths.
- Effect class: unchanged (single bounded model inference per attempt;
  non-mutating, no workspace effect).
- Idempotency: unchanged. No replay semantics change; malformed-only retry
  (max 2) preserved; unknown effects never blindly replayed.
- Resource claim: at most +4096 output tokens per attempt, same 2-attempt
  bound; same 120 s deadline.
- Cancellation: unchanged (cancel token + deadline; `interrupted` calls
  stay counted via CallGuard; cancellation drain unchanged).
- Retry policy: unchanged (retry `MalformedOutput` once, nothing else).
- Verification: M21 replay 4/4, `m23_flag_default_off` + budget/prompt
  unit tests, VERIFY tier before commit, FULL tier for implement; frozen
  same-model live trials below.
- Crash recovery: no new durable state; `model.enter` fault point
  unchanged; interrupted rows reconcile as before.
- Latency class: Medium (provider unchanged).

## Flag

`m23_framing_fix`, disabled by default (`Default::default()` yields
`false`). Proven by `m23_flag_default_off` test plus a deterministic
budget/prompt selection test.

## Frozen inputs

M21 regressions (`m21_replay` 4/4), `docs/milestones/M21_MANIFEST.json`,
`docs/milestones/M21_PLAN.md`, `fixtures/reservation-ledger`, this plan,
`docs/milestones/M23_TRIAL_MANIFEST.md`, `scripts/m23_manifest.mjs`,
`scripts/m23_run.mjs`, `crates/tachyon-core/src/runtime.rs`,
`crates/tachyon-core/src/driver.rs`,
`crates/tachyon-core/src/driver/model.rs`,
`crates/tachyon-core/examples/bench_matrix.rs`.

## Adopt/reject rule (stated here so trials cannot reinterpret)

Adopt only if frozen live trials show strict improvement on verified
success with same-model harness discipline and M21 regressions stay 4/4.
Otherwise reject. On adopt, the EXIT-11 6-row speed cell re-runs as M23b
under the same freeze discipline; no speed claim is valid on unequal
verified success. Rule applied verbatim in the trial report.

## Hardening (E13-E18)

Validated-IR lowering unchanged; access sets unchanged; one-logical-writer
unchanged; M9/M12 re-proof required before live trials; no blind retry
added; same-model MUST for trials; implement S3 = FULL tier.
