#!/usr/bin/env node
// Live-model matrix checker (LIVE_MODEL_PLAN.md step 4).
//
// Sibling of m14_matrix_check.mjs for the live leg: reads raw JSONL
// samples (one object per line, error samples included), enforces the
// same per-sample contract plus `usage_provenance == provider_reported`
// with real provider token counts, then writes the aggregate
// docs/milestones/LIVE_MODEL_MATRIX.json. Never touches M14_MATRIX.json.
//
// Unlike the scripted gate, error samples are data, not gate failures:
// the plan counts malformed provider output as verification_failures and
// applies its decision rule (third arm at low verified success: fix the
// prompt contract, do not publish comparisons).
"use strict";

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const rawFile = process.argv[2]
  ? path.resolve(process.cwd(), process.argv[2])
  : path.join(root, ".scratch", "live_raw.jsonl");
const outPath = process.env.LIVE_MATRIX_OUT
  ? path.resolve(root, process.env.LIVE_MATRIX_OUT)
  : path.join(root, "docs", "milestones", "LIVE_MODEL_MATRIX.json");
if (!outPath.startsWith(`${root}${path.sep}`)) {
  fail("LIVE_MATRIX_OUT must point inside the repository");
}
if (outPath.endsWith("M14_MATRIX.json")) {
  fail("refusing to touch the frozen M14_MATRIX.json");
}

const MODES = ["full", "serial"];

function fail(message) {
  console.error(message);
  process.exit(1);
}

if (!fs.existsSync(rawFile)) fail(`missing ${rawFile}`);
const samples = fs
  .readFileSync(rawFile, "utf8")
  .split("\n")
  .filter((line) => line.trim().length > 0)
  .map((line, index) => {
    try {
      return JSON.parse(line);
    } catch (error) {
      fail(`${rawFile}:${index + 1}: invalid JSON: ${error.message}`);
    }
  });

const REQUIRED_NUMERIC = [
  "completion_ms",
  "first_evidence_ms",
  "first_edit_ms",
  "model_ms",
  "tool_calls",
  "verification_failures",
  "user_interventions",
  "retries",
  "provider_failures",
  "evidence_concurrency",
  "input_tokens",
  "output_tokens",
];
for (const [i, s] of samples.entries()) {
  const where = `sample[${i}] ${s.fixture}/${s.mode}/${s.sample}`;
  if (s.outcome === "error") continue; // failed samples are data (counted below)
  for (const field of ["fixture", "mode", "provider", "model", "outcome"]) {
    if (typeof s[field] !== "string" || s[field].length === 0) fail(`${where}: missing ${field}`);
  }
  for (const field of REQUIRED_NUMERIC) {
    if (typeof s[field] !== "number" || !Number.isFinite(s[field])) {
      fail(`${where}: ${field} is not a finite number: ${s[field]}`);
    }
  }
  if (s.usage_provenance !== "provider_reported") {
    fail(`${where}: usage_provenance=${s.usage_provenance}, want provider_reported`);
  }
  if (s.input_tokens < 1 || s.output_tokens < 1) {
    fail(`${where}: token counts must be real provider numbers`);
  }
  if (s.model_calls !== null && s.model_calls !== undefined) {
    fail(`${where}: live model_calls must be None (no scripted queue), got ${s.model_calls}`);
  }
  if (s.mode === "full" && s.evidence_concurrency < 2) {
    fail(`${where}: full mode measured evidence_concurrency=${s.evidence_concurrency}`);
  }
  if (s.mode === "serial" && s.evidence_concurrency !== 1) {
    fail(`${where}: serial mode measured concurrency=${s.evidence_concurrency}`);
  }
}

function percentiles(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  if (n === 0) return { p50: null, p95: null };
  return {
    p50: sorted[Math.min(Math.floor((n * 50) / 100), n - 1)],
    p95: sorted[Math.min(Math.floor((n * 95) / 100), n - 1)],
  };
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

const first = samples.find((s) => s.outcome !== "error") ?? samples[0];
const cells = {};
for (const mode of MODES) {
  const own = samples.filter((s) => s.fixture === "auth-refresh" && s.mode === mode);
  const ok = own.filter((s) => s.outcome === "completed" && s.verified === true);
  const walls = ok.map((s) => s.task_wall_ms).filter((v) => typeof v === "number");
  cells[mode] = {
    n: own.length,
    verified: ok.length,
    errors: own.length - ok.length,
    wall_ms: { ...percentiles(walls), min: walls.length ? Math.min(...walls) : null, max: walls.length ? Math.max(...walls) : null },
    model_ms_med: median(ok.map((s) => s.model_ms).filter((v) => typeof v === "number")),
    input_tokens: ok.reduce((a, s) => a + s.input_tokens, 0),
    output_tokens: ok.reduce((a, s) => a + s.output_tokens, 0),
  };
}

const verifiedRates = MODES.map((m) => (cells[m].n ? cells[m].verified / cells[m].n : 0));
const unreliable = verifiedRates.some((r) => r < 0.9);
const artifact = {
  generated: new Date().toISOString(),
  provider: first.provider,
  model: first.model,
  fixture: "auth-refresh",
  usage_provenance: "provider_reported",
  modes: cells,
  decision_rule_arm: unreliable
    ? "third: verified success unreliable in at least one mode; prompt/model contract is the problem; do not publish comparisons"
    : "first/second: both modes verify reliably; full-vs-serial comparison applies",
};

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, `${JSON.stringify(artifact, null, 2)}\n`);
console.log(
  `live matrix ok: full ${cells.full.verified}/${cells.full.n}, serial ${cells.serial.verified}/${cells.serial.n} — ${artifact.decision_rule_arm.split(":")[0]}`,
);
