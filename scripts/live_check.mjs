#!/usr/bin/env node
// Complete live-attempt accounting. Frozen M14 artifacts are never overwritten.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const MODES = ["full", "serial"];
const ERRORS = new Set(["invalid_request", "unauthorized", "rate_limited", "provider_unavailable",
  "timeout", "malformed_output", "context_overflow", "transport", "cancelled", "internal", "interrupted"]);
const assert = (condition, message) => { if (!condition) throw new Error(message); };
const finite = (n) => typeof n === "number" && Number.isFinite(n) && n >= 0;
const integer = (n) => Number.isSafeInteger(n) && n >= 0;
const counter = (n) => n === null || (integer(n) && n <= 0xffffffff);
const equal = (a, b) => JSON.stringify(a) === JSON.stringify(b);

export function percentiles(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const at = (p) => sorted.length ? sorted[Math.ceil(sorted.length * p) - 1] : null;
  return { p50: at(0.5), p95: at(0.95) };
}

function total(calls, field) {
  if (calls.some((c) => c.usage[field] === null)) return null;
  const sum = calls.reduce((sum, c) => sum + c.usage[field], 0);
  return sum <= 0xffffffff ? sum : null;
}

export function validateSamples(samples, expectedN = 20) {
  assert(integer(expectedN) && expectedN > 0, "sample count must be positive");
  assert(samples.length === 2 * expectedN, `expected ${2 * expectedN} samples`);
  const identities = new Set();
  const first = samples[0];
  for (const s of samples) {
    const where = `${s.fixture}/${s.mode}/${s.sample}`;
    assert(s.fixture === "auth-refresh" && MODES.includes(s.mode), `${where}: unexpected cell`);
    assert(integer(s.sample) && s.sample >= 1 && s.sample <= expectedN, `${where}: invalid sample id`);
    assert(!identities.has(`${s.mode}/${s.sample}`), `${where}: duplicate sample`);
    identities.add(`${s.mode}/${s.sample}`);
    assert(typeof s.provider === "string" && s.provider.length > 0 &&
      typeof s.model === "string" && s.model.length > 0, `${where}: missing provider/model`);
    assert(s.provider === "bench-live", `${where}: live provider required`);
    assert(s.provider === first.provider && s.model === first.model, `${where}: provider/model changed`);
    assert(["completed", "verification_failed", "error"].includes(s.outcome), `${where}: invalid outcome`);
    assert(s.verified === (s.outcome === "completed"), `${where}: contradictory verified flag`);
    assert(s.broken_first_failed === true && s.protected_unchanged === true, `${where}: fixture protection failed`);
    assert(Array.isArray(s.observed_changes) && equal(s.expected_changes, ["auth-session/src/session.rs"]), `${where}: missing or incorrect change sets`);
    assert(s.observed_changes.every((p) => s.expected_changes.includes(p)) && new Set(s.observed_changes).size === s.observed_changes.length, `${where}: forbidden or duplicate changes`);
    assert(finite(s.task_wall_ms) && finite(s.completion_ms) && finite(s.model_ms), `${where}: missing duration`);
    assert(s.task_wall_ms >= s.model_ms, `${where}: model time exceeds task time`);
    const calls = s.model_attempts;
    assert(Array.isArray(calls) && calls.length >= 1 && calls.length <= 2, `${where}: invalid attempts`);
    for (const [i, c] of calls.entries()) {
      assert(c.attempt === i + 1 && finite(c.latency_ms), `${where}: malformed attempt`);
      assert(c.error === null || ERRORS.has(c.error), `${where}: invalid error class`);
      assert(c.error === "malformed_output" ? ["empty_content", "invalid_json", "invalid_decision", "missing_decision", "missing_decision_field", "unknown_decision", "invalid_decision_type", "missing_content", "invalid_response", "invalid_stream", "unclassified"].includes(c.output_failure) : c.output_failure === null, `${where}: invalid output failure class`);
      assert(c.usage && ["provider_reported", "unknown"].includes(c.usage.provenance), `${where}: invalid usage provenance`);
      assert(counter(c.usage.input_tokens) && counter(c.usage.output_tokens), `${where}: invalid usage counters`);
      if (c.usage.provenance === "unknown") {
        assert(c.usage.input_tokens === null && c.usage.output_tokens === null, `${where}: unknown usage has counters`);
      }
    }
    if (calls.length === 2) assert(calls[0].error === "malformed_output", `${where}: unsafe retry`);
    assert(s.model_calls === calls.length && s.retries === calls.length - 1, `${where}: contradictory call count`);
    assert(s.provider_failures === calls.filter((c) => c.error !== null).length, `${where}: incorrect provider failures`);
    assert(Math.abs(s.model_ms - calls.reduce((sum, c) => sum + c.latency_ms, 0)) < 0.01, `${where}: incorrect model duration`);
    assert(s.input_tokens === total(calls, "input_tokens") && s.output_tokens === total(calls, "output_tokens"), `${where}: incorrect token totals`);
    const provenance = calls.every((c) => c.usage.provenance === calls[0].usage.provenance)
      ? calls[0].usage.provenance : "unknown";
    assert(s.usage_provenance === provenance, `${where}: incorrect total provenance`);
    const last = calls.at(-1);
    if (s.outcome === "error") {
      assert(typeof s.error === "string" && s.error.length > 0, `${where}: missing failure reason`);
      assert(ERRORS.has(s.error_code) || ["driver_failure", "verification_failed"].includes(s.error_code), `${where}: invalid failure code`);
      assert(s.failure_durable === true && ["recovered_failed", "failed", "Failed"].includes(s.recovery), `${where}: failure not recovered`);
      assert(s.verification_failures === Number(s.error_code === "verification_failed"), `${where}: incorrect verification failure count`);
      if (!ERRORS.has(s.error_code)) assert(last.error === null, `${where}: failed model call claimed post-model failure`);
      if (last.error !== null) assert(s.observed_changes.length === 0, `${where}: failed model call mutated workspace`);
      if (ERRORS.has(s.error_code)) {
        assert(last.error !== null && s.observed_changes.length === 0, `${where}: provider error mutated workspace`);
        assert(last.error === s.error_code || (["timeout", "cancelled"].includes(s.error_code) && last.error === "interrupted"), `${where}: contradictory final error`);
      }
    } else {
      assert(last.error === null, `${where}: failed final call claimed execution`);
      assert(s.verification_failures === Number(!s.verified), `${where}: incorrect verification failures`);
      assert(finite(s.first_evidence_ms) && finite(s.first_edit_ms), `${where}: missing stage timing`);
      assert(s.mode === "full" ? s.evidence_concurrency >= 2 : s.evidence_concurrency === 1, `${where}: wrong evidence mode`);
      if (s.verified) {
        assert(equal(s.observed_changes, s.expected_changes) && s.observed_matches_expected === true, `${where}: incorrect change set`);
        assert(s.recovery === "recovered_completed", `${where}: completion not recovered`);
      } else {
        assert(["recovered_failed", "recovered_executing"].includes(s.recovery), `${where}: failed verification claimed completion recovery`);
      }
    }
  }
  return samples;
}

export function aggregate(samples, expectedN = 20) {
  validateSamples(samples, expectedN);
  const modes = {};
  for (const mode of MODES) {
    const own = samples.filter((s) => s.mode === mode);
    const ok = own.filter((s) => s.verified);
    const calls = own.flatMap((s) => s.model_attempts);
    modes[mode] = {
      n: own.length, verified: ok.length, errors: own.filter((s) => s.outcome === "error").length,
      verification_failures: own.reduce((sum, s) => sum + s.verification_failures, 0),
      first_attempt_valid: own.filter((s) => s.model_attempts[0].error === null).length,
      first_attempt_verified: ok.filter((s) => s.model_calls === 1).length,
      model_calls: calls.length, retries: own.reduce((sum, s) => sum + s.retries, 0),
      provider_failures: own.reduce((sum, s) => sum + s.provider_failures, 0),
      all_task_wall_ms: percentiles(own.map((s) => s.task_wall_ms)),
      verified_task_wall_ms: percentiles(ok.map((s) => s.task_wall_ms)),
      verified_first_edit_ms: percentiles(ok.map((s) => s.first_edit_ms)),
      model_ms: percentiles(own.map((s) => s.model_ms)),
      known_input_tokens: calls.reduce((sum, c) => sum + (c.usage.input_tokens ?? 0), 0),
      known_output_tokens: calls.reduce((sum, c) => sum + (c.usage.output_tokens ?? 0), 0),
      unavailable_usage_attempts: calls.filter((c) => c.usage.input_tokens === null || c.usage.output_tokens === null).length,
      cost_usd: null,
      output_failures: Object.fromEntries([...new Set(calls.map((c) => c.output_failure).filter(Boolean))]
        .map((code) => [code, calls.filter((c) => c.output_failure === code).length])),
      error_classes: Object.fromEntries([...new Set(calls.map((c) => c.error).filter(Boolean))]
        .map((code) => [code, calls.filter((c) => c.error === code).length])),
    };
  }
  const reliable = expectedN >= 20 && MODES.every((m) => modes[m].verified / expectedN >= 0.95);
  return { generated: new Date().toISOString(), provider: samples[0].provider, model: samples[0].model,
    fixture: "auth-refresh", modes, reliable, cost_note: "No verified price supplied; token counts are measured, monetary cost is unavailable.",
    comparison_allowed: reliable && modes.full.verified === modes.serial.verified,
    decision_rule_arm: reliable ? "reliability gate met; compare latency only at equal verified success" :
      "third: reliability gate unmet; continue shaping diagnosis; no speed comparison" };
}

function main() {
  const args = process.argv.slice(2);
  const rawFile = path.resolve(args[0] ?? path.join(root, "target/live-reliability/raw.jsonl"));
  const nFlag = args.indexOf("--samples");
  const n = nFlag >= 0 ? Number(args[nFlag + 1]) : 20;
  const outPath = path.resolve(root, process.env.LIVE_MATRIX_OUT ?? "docs/milestones/LIVE_RELIABILITY_MATRIX.json");
  assert(outPath.startsWith(`${root}${path.sep}`) && path.basename(outPath) !== "M14_MATRIX.json", "unsafe output path");
  const samples = fs.readFileSync(rawFile, "utf8").split("\n").filter((l) => l.trim()).map((l) => JSON.parse(l));
  const artifact = aggregate(samples, n);
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify(artifact, null, 2)}\n`);
  console.log(`live matrix ok: full ${artifact.modes.full.verified}/${n}, serial ${artifact.modes.serial.verified}/${n}`);
  if (args.includes("--require-reliable")) {
    assert(n >= 20 && artifact.reliable, "live reliability gate unmet");
    console.log("live reliability passed");
  }
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(); } catch (error) { console.error(error.message); process.exitCode = 1; }
}
