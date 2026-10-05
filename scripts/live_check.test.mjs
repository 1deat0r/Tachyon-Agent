import test from "node:test";
import assert from "node:assert/strict";
import { aggregate, validateSamples, percentiles } from "./live_check.mjs";

function sample(mode, id) {
  return { fixture: "auth-refresh", mode, sample: id, provider: "bench-live", model: "pinned",
    outcome: "completed", verified: true, broken_first_failed: true, protected_unchanged: true,
    observed_changes: ["auth-session/src/session.rs"], expected_changes: ["auth-session/src/session.rs"], observed_matches_expected: true,
    task_wall_ms: 100, completion_ms: 100, model_ms: 80, first_evidence_ms: 1, first_edit_ms: 85,
    evidence_concurrency: mode === "full" ? 2 : 1, model_calls: 1, retries: 0, provider_failures: 0,
    verification_failures: 0, input_tokens: 17, output_tokens: 9, usage_provenance: "provider_reported",
    recovery: "recovered_completed", model_attempts: [{ attempt: 1, latency_ms: 80, error: null, output_failure: null,
      usage: { input_tokens: 17, output_tokens: 9, provenance: "provider_reported" } }] };
}
const corpus = () => ["full", "serial"].flatMap((m) => Array.from({ length: 20 }, (_, i) => sample(m, i + 1)));

test("complete matched corpus passes with nearest-rank percentiles", () => {
  const artifact = aggregate(corpus());
  assert.equal(artifact.reliable, true);
  assert.equal(artifact.modes.full.known_input_tokens, 340);
  assert.deepEqual(percentiles(Array.from({ length: 20 }, (_, i) => i + 1)), { p50: 10, p95: 19 });
});

test("all failure samples need complete safe accounting", () => {
  const rows = corpus();
  const s = rows[0];
  Object.assign(s, { outcome: "error", verified: false, observed_changes: [], error: "malformed output",
    error_code: "malformed_output", failure_durable: true, recovery: "failed", provider_failures: 1 });
  s.model_attempts[0].error = "malformed_output";
  s.model_attempts[0].output_failure = "invalid_json";
  assert.equal(aggregate(rows).modes.full.provider_failures, 1);
  // The first measured candidate emitted TaskStatus::name's enum spelling.
  // Both recognized failure spellings still prove a failed recovery state.
  s.recovery = "Failed";
  validateSamples(rows);
  s.recovery = "recovered_failed";
  validateSamples(rows);
  delete s.model_attempts;
  assert.throws(() => validateSamples(rows), /invalid attempts/);
});

test("retry usage includes rejected calls and unknown stays unknown", () => {
  const rows = corpus(); const s = rows[0];
  s.model_attempts.unshift({ attempt: 1, latency_ms: 10, error: "malformed_output", output_failure: "invalid_json",
    usage: { provenance: "unknown", input_tokens: null, output_tokens: null } });
  s.model_attempts[1].attempt = 2;
  Object.assign(s, { model_calls: 2, retries: 1, provider_failures: 1, model_ms: 90,
    input_tokens: null, output_tokens: null, usage_provenance: "unknown" });
  const a = aggregate(rows);
  assert.equal(a.modes.full.unavailable_usage_attempts, 1);
  assert.equal(a.modes.full.known_input_tokens, 340);
  assert.equal(a.modes.full.first_attempt_verified, 19);
});

test("incomplete, contradictory, unsafe, and mixed samples fail", () => {
  const mutations = [
    (r) => r.pop(), (r) => { r[1].sample = 1; }, (r) => { r[0].model = "other"; },
    (r) => { r[0].model_calls = 0; }, (r) => { r[0].input_tokens = 0; },
    (r) => { r[0].verified = false; }, (r) => { r[0].protected_unchanged = false; },
    (r) => { r[0].observed_changes = []; }, (r) => { r[0].recovery = null; },
    (r) => { r[0].task_wall_ms = 1; }, (r) => { r[0].model_attempts[0].latency_ms = -1; },
    (r) => { r[0].model_attempts[0].usage.input_tokens = "17"; },
    (r) => { r[0].model_attempts.push({ ...r[0].model_attempts[0], attempt: 2 }); },
  ];
  for (const mutate of mutations) { const rows = corpus(); mutate(rows); assert.throws(() => validateSamples(rows)); }
});

test("unreliable corpus cannot support comparison", () => {
  const rows = corpus();
  for (const s of rows.slice(0, 2)) {
    Object.assign(s, { outcome: "error", verified: false, observed_changes: [], error: "bad",
      error_code: "malformed_output", failure_durable: true, recovery: "failed", provider_failures: 1 });
    s.model_attempts[0].error = "malformed_output";
  s.model_attempts[0].output_failure = "invalid_json";
  }
  const a = aggregate(rows);
  assert.equal(a.reliable, false);
  assert.equal(a.comparison_allowed, false);
});

test("overflow totals remain unavailable and final error must match", () => {
  const rows = corpus(); const s = rows[0];
  s.model_attempts.unshift({ attempt: 1, latency_ms: 10, error: "malformed_output", output_failure: "invalid_json",
    usage: { provenance: "provider_reported", input_tokens: 0xffffffff, output_tokens: 0xffffffff } });
  s.model_attempts[1].attempt = 2;
  Object.assign(s, { model_calls: 2, retries: 1, provider_failures: 1, model_ms: 90,
    input_tokens: null, output_tokens: null });
  validateSamples(rows);
  Object.assign(s, { outcome: "error", verified: false, observed_changes: [], error: "timeout",
    error_code: "timeout", failure_durable: true, recovery: "failed", provider_failures: 2 });
  s.model_attempts[1].error = "unauthorized";
  assert.throws(() => validateSamples(rows), /contradictory final error/);
});

test("failed acceptance is distinct from failed provider output", () => {
  const rows = corpus(); const s = rows[0];
  Object.assign(s, { outcome: "error", verified: false, error: "drive failed: verification_failed",
    error_code: "verification_failed", failure_durable: true, recovery: "failed", verification_failures: 1 });
  assert.equal(aggregate(rows).modes.full.verification_failures, 1);
  s.verification_failures = 0;
  assert.throws(() => validateSamples(rows), /incorrect verification failure count/);
});

test("small pilot cannot establish the reliability gate", () => {
  const a = aggregate([sample("full", 1), sample("serial", 1)], 1);
  assert.equal(a.reliable, false);
  assert.equal(a.comparison_allowed, false);
});

test("failure codes cannot hide writes or completed recovery", () => {
  const rows = corpus(); const s = rows[0];
  Object.assign(s, { outcome: "error", verified: false, observed_changes: [], error: "bad",
    error_code: "malformed_output", failure_durable: true, recovery: "failed", provider_failures: 1 });
  s.model_attempts[0].error = "malformed_output";
  s.model_attempts[0].output_failure = "invalid_json";
  validateSamples(rows);
  delete s.error_code;
  assert.throws(() => validateSamples(rows), /invalid failure code/);
  s.error_code = "driver_failure";
  s.observed_changes = s.expected_changes;
  assert.throws(() => validateSamples(rows), /failed model call claimed post-model failure/);
  s.error_code = "malformed_output";
  assert.throws(() => validateSamples(rows), /mutated workspace/);
  const failed = corpus();
  Object.assign(failed[0], { outcome: "verification_failed", verified: false, verification_failures: 1 });
  assert.throws(() => validateSamples(failed), /claimed completion recovery/);
  failed[0].recovery = "recovered_executing";
  validateSamples(failed);
  failed[0].observed_changes.push("protected-test.rs");
  assert.throws(() => validateSamples(failed), /forbidden or duplicate changes/);
});
