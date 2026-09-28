#!/usr/bin/env node
// M14 matrix checker (gate G5).
//
// Validates the raw §44 matrix transcript: every fixture × mode cell has
// the expected sample count, every sample carries the docs/05 primary
// metrics, every run verified successfully with an unchanged fixture and
// the expected change set, the alias modes disclose their coincidence,
// and both composed legs hold their model-call contracts. Then computes
// nearest-rank p50/p95 per cell and writes the checked-in aggregate
// docs/milestones/M14_MATRIX.json that MVP_REPORT.md quotes.
"use strict";

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const rawDir = path.join(root, "target", "m14");
const outPath = process.env.M14_MATRIX_OUT
  ? path.resolve(root, process.env.M14_MATRIX_OUT)
  : path.join(root, "docs", "milestones", "M14_MATRIX.json");
if (!outPath.startsWith(`${root}${path.sep}`)) {
  fail("M14_MATRIX_OUT must point inside the repository");
}

const FIXTURES = ["auth-refresh", "multi-file-migration", "architecture-plan"];
const MODES = ["full", "no-speculation", "no-judgment", "serial", "reference"];
const ALIAS_MODES = new Set(["no-speculation", "no-judgment"]);
const CONCURRENT_MODES = new Set(["full", "no-speculation", "no-judgment"]);

function fail(message) {
  console.error(message);
  process.exit(1);
}

function readJsonLines(file) {
  if (!fs.existsSync(file)) fail(`missing ${file}`);
  return fs
    .readFileSync(file, "utf8")
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line, index) => {
      try {
        return JSON.parse(line);
      } catch (error) {
        fail(`${file}:${index + 1}: invalid JSON: ${error.message}`);
      }
    });
}

const metaPath = path.join(rawDir, "run_meta.json");
if (!fs.existsSync(metaPath)) fail("missing target/m14/run_meta.json — run scripts/m14_matrix.sh first");
const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
const perCell = meta.samples;
if (!Number.isInteger(perCell) || perCell < 1) fail(`bad run_meta samples: ${perCell}`);

const samples = readJsonLines(path.join(rawDir, "raw.jsonl"));
const legs = readJsonLines(path.join(rawDir, "legs.jsonl"));

// ---- per-sample contract -------------------------------------------------
const REQUIRED_NUMERIC = [
  "completion_ms",
  "first_evidence_ms",
  "first_edit_ms",
  "model_calls",
  "judgment_calls",
  "jev_calls",
  "tool_calls",
  "verification_failures",
  "user_interventions",
  "retries",
  "provider_failures",
  "evidence_concurrency",
];
for (const [i, s] of samples.entries()) {
  const where = `sample[${i}] ${s.fixture}/${s.mode}/${s.sample}`;
  for (const field of ["fixture", "mode", "class", "provider", "model", "outcome"]) {
    if (typeof s[field] !== "string" || s[field].length === 0) fail(`${where}: missing ${field}`);
  }
  for (const field of REQUIRED_NUMERIC) {
    if (typeof s[field] !== "number" || !Number.isFinite(s[field])) {
      fail(`${where}: ${field} is not a finite number: ${s[field]}`);
    }
  }
  if (s.verified !== true) fail(`${where}: verified=${s.verified}`);
  if (s.broken_first_failed !== true) fail(`${where}: broken-first proof missing`);
  if (s.protected_unchanged !== true) fail(`${where}: protected paths changed`);
  if (s.observed_matches_expected !== true) fail(`${where}: observed changes != expected`);
  if (s.verification_failures !== 0) fail(`${where}: verification_failures=${s.verification_failures}`);
  if (s.user_interventions !== 0) fail(`${where}: user_interventions=${s.user_interventions}`);
  if (!s.provider.startsWith("bench-script-")) fail(`${where}: provider ${s.provider} not pinned`);
  if (s.model !== "scripted-replay-1") fail(`${where}: model ${s.model} not pinned`);
  if (ALIAS_MODES.has(s.mode)) {
    if (s.coincides_with !== "full") fail(`${where}: alias must declare coincidence with full`);
    if (!s.mode_note) fail(`${where}: alias missing mode_note`);
  } else if (s.coincides_with !== null) {
    fail(`${where}: non-alias mode declares coincidence`);
  }
  if (s.mode === "reference" && s.outcome !== "completed_reference") {
    fail(`${where}: reference outcome ${s.outcome}`);
  }
  if (s.mode !== "reference" && s.outcome !== "completed") {
    fail(`${where}: supervisor outcome ${s.outcome}`);
  }
  if (CONCURRENT_MODES.has(s.mode) && s.evidence_concurrency < 2) {
    fail(`${where}: concurrent mode measured evidence_concurrency=${s.evidence_concurrency}`);
  }
  if (s.mode === "serial" && s.evidence_concurrency !== 1) {
    fail(`${where}: serial mode measured concurrency=${s.evidence_concurrency}`);
  }
  if (s.mode === "reference" && s.evidence_concurrency !== 1) {
    fail(`${where}: reference (serial control) concurrency=${s.evidence_concurrency}`);
  }
}

// ---- cell completeness ---------------------------------------------------
const cells = new Map();
for (const fixture of FIXTURES) {
  for (const mode of MODES) {
    const key = `${fixture}/${mode}`;
    const own = samples.filter((s) => s.fixture === fixture && s.mode === mode);
    if (own.length !== perCell) {
      fail(`cell ${key}: expected ${perCell} samples, found ${own.length}`);
    }
    const indices = own.map((s) => s.sample).sort((a, b) => a - b);
    // Sample indices start at 1: the shell loop runs i=1..SAMPLES and the
    // host echoes the index it was given (0 only for ad-hoc manual runs).
    for (let i = 0; i < perCell; i += 1) {
      if (indices[i] !== i + 1) fail(`cell ${key}: sample indices ${JSON.stringify(indices)}`);
    }
    cells.set(key, own);
  }
}
const strays = samples.filter((s) => !FIXTURES.includes(s.fixture) || !MODES.includes(s.mode));
if (strays.length > 0) fail(`unexpected samples: ${JSON.stringify(strays[0])}`);

// ---- legs ---------------------------------------------------------------
if (legs.length !== 2) fail(`expected 2 legs, got ${legs.length}`);
const legA = legs.find((l) => l.leg === "A");
const legB = legs.find((l) => l.leg === "B");
if (!legA || !legB) fail("legs must be A and B");
if (legA.model_calls !== 0) fail(`leg A model_calls=${legA.model_calls}`);
if (legA.route_class !== "direct_native") fail(`leg A route_class=${legA.route_class}`);
if (legA.verified !== true || legA.definitions < 1 || legA.references < 2) {
  fail("leg A answer correctness missing");
}
if (typeof legA.p50_us !== "number" || typeof legA.p95_us !== "number" || legA.n < 20) {
  fail("leg A percentiles/n missing");
}
if (legB.model_calls !== 1) fail(`leg B model_calls=${legB.model_calls}`);
if (legB.evidence_first !== true || legB.answer_cites_both !== true || legB.verified !== true) {
  fail("leg B evidence/citation contract missing");
}
if (typeof legB.p50_us !== "number" || typeof legB.p95_us !== "number" || legB.n < 10) {
  fail("leg B percentiles/n missing");
}

// ---- aggregate -----------------------------------------------------------
function percentiles(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  return {
    p50: sorted[Math.min(Math.floor((n * 50) / 100), n - 1)],
    p95: sorted[Math.min(Math.floor((n * 95) / 100), n - 1)],
  };
}

const METRIC_FIELDS = [
  "completion_ms",
  "task_wall_ms",
  "harness_ms",
  "first_evidence_ms",
  "first_edit_ms",
  "model_ms",
];

const cellReports = [];
for (const fixture of FIXTURES) {
  for (const mode of MODES) {
    const own = cells.get(`${fixture}/${mode}`);
    const first = own[0];
    const metrics = {};
    for (const field of METRIC_FIELDS) {
      const values = own.map((s) => s[field]).filter((v) => typeof v === "number");
      if (values.length === own.length) metrics[field] = percentiles(values);
    }
    cellReports.push({
      fixture,
      class: first.class,
      mode,
      coincides_with: first.coincides_with,
      n: own.length,
      verified_success_rate: own.filter((s) => s.verified).length / own.length,
      provider: first.provider,
      model: first.model,
      model_calls: percentiles(own.map((s) => s.model_calls)),
      judgment_calls: 0,
      jev_calls: 0,
      tool_calls: first.tool_calls,
      max_evidence_concurrency: Math.max(...own.map((s) => s.evidence_concurrency)),
      check_broadening: own.some((s) => s.check_broadening),
      selected_checks: first.selected_checks,
      check_note: first.check_note,
      metrics,
    });
  }
}

function cellFor(fixture, mode) {
  return cellReports.find((c) => c.fixture === fixture && c.mode === mode);
}

const comparisons = {};
for (const fixture of FIXTURES) {
  const full = cellFor(fixture, "full");
  const serial = cellFor(fixture, "serial");
  const reference = cellFor(fixture, "reference");
  comparisons[fixture] = {
    full_vs_serial: {
      completion_p50_full: full.metrics.completion_ms.p50,
      completion_p50_serial: serial.metrics.completion_ms.p50,
      first_evidence_p50_full: full.metrics.first_evidence_ms.p50,
      first_evidence_p50_serial: serial.metrics.first_evidence_ms.p50,
    },
    full_vs_reference: {
      completion_p50_full: full.metrics.completion_ms.p50,
      completion_p50_reference: reference.metrics.completion_ms.p50,
      ttfr_p50_full: full.metrics.first_edit_ms.p50,
      ttfr_p50_reference: reference.metrics.first_edit_ms.p50,
      verified_full: full.verified_success_rate,
      verified_reference: reference.verified_success_rate,
    },
  };
}

const artifact = {
  generated: new Date().toISOString(),
  sha: meta.sha,
  samples_per_cell: perCell,
  percentiles: "nearest-rank (p50 = sorted[n*50/100], p95 = sorted[min(n*95/100, n-1)])",
  configuration: {
    provider: "bench-script-<fixture> (FakeModelProvider, scripted queue)",
    model: "scripted-replay-1",
    note: "pinned scripted provider per docs/11 #11: identical model across every mode by construction (AD-015 same-model rule); no live model IDs in the MVP matrix",
    modes: MODES,
    alias_modes: "no-speculation and no-judgment are measured aliases of full (no speculation/judgment stage exists in the MVP driver)",
    risk: "Affected",
  },
  cells: cellReports,
  legs: [legA, legB],
  comparisons,
};

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, `${JSON.stringify(artifact, null, 2)}\n`);
console.log("m14 matrix ok");
