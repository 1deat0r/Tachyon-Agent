#!/usr/bin/env node
// M14 G11: MVP_REPORT.md figures reconcile against docs/milestones/M14_MATRIX.json.
//
// G8 (m14_report_check.mjs) covers the verified-success total, the full-cell
// completion/TTFR pairs and the leg pairs. This checker closes the classes
// the G11 reconciliation note lists but G8 does not: the full-cell
// first-evidence and task-wall pairs, the twelve serial/reference control
// p50s, the verified-success table rows, and the model_ms p50 range — each
// scoped to its own report section so a number appearing elsewhere (or a
// stale copy) fails here.
"use strict";

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const reportPath = path.join(root, "MVP_REPORT.md");
const matrixPath = path.join(root, "docs", "milestones", "M14_MATRIX.json");

function fail(message) {
  console.error(message);
  process.exit(1);
}

if (!fs.existsSync(reportPath)) fail("missing MVP_REPORT.md");
if (!fs.existsSync(matrixPath)) fail("missing docs/milestones/M14_MATRIX.json (run G5 first)");
const report = fs.readFileSync(reportPath, "utf8");
const matrix = JSON.parse(fs.readFileSync(matrixPath, "utf8"));

function section(from, to) {
  const rest = report.split(from)[1];
  if (!rest) fail(`report section unreadable: ${from}`);
  return to ? rest.split(to)[0] : rest;
}

const verifiedSection = section(
  "## Verified-success comparison",
  "## Median and p95 TTFR and completion"
);
const medianSection = section(
  "## Median and p95 TTFR and completion",
  "## Critical-path breakdown"
);
const modelSection = section("## Model, Jev and tool calls", "## Known limitations");

function row(sectionText, firstCell) {
  const line = sectionText
    .split("\n")
    .find((l) => l.startsWith(`| ${firstCell} |`));
  if (!line) fail(`table row missing: "${firstCell}"`);
  return line.split("|").slice(1, -1).map((c) => c.trim());
}

// The median section holds two fixture tables; scope each lookup to its own.
const fullTable = medianSection.split("`full` mode (p50/p95 ms, n=10 per cell):")[1];
if (!fullTable) fail("median section lacks the full-mode table header");
const controlsTable = medianSection.split("Controls, p50 (completion / TTFR):")[1];
if (!controlsTable) fail("median section lacks the controls table header");

// ---- verified-success table rows -----------------------------------------
const order = matrix.configuration.modes;
for (const fixture of [...new Set(matrix.cells.map((c) => c.fixture))]) {
  const cells = row(verifiedSection, fixture);
  const modes = cells.slice(2); // fixture, class, then one cell per mode
  if (modes.length !== order.length) {
    fail(`${fixture}: verified-success table has ${modes.length} modes, artifact has ${order.length}`);
  }
  order.forEach((mode, i) => {
    const cell = matrix.cells.find((c) => c.fixture === fixture && c.mode === mode);
    const expected = `${Math.round(cell.verified_success_rate * cell.n)}/${cell.n}`;
    if (modes[i] !== expected) {
      fail(`${fixture}/${mode}: report says ${modes[i]}, artifact says ${expected}`);
    }
  });
}
const verifiedTotal =
  matrix.cells.reduce((sum, c) => sum + c.verified_success_rate * c.n, 0);
if (!verifiedSection.includes(`${verifiedTotal}/${matrix.cells.length * matrix.samples_per_cell}`)) {
  fail(`verified-success section lacks the ${verifiedTotal}/${matrix.cells.length * matrix.samples_per_cell} total`);
}

// ---- full-cell metric pairs: first evidence and task wall -----------------
for (const cell of matrix.cells.filter((c) => c.mode === "full")) {
  for (const [metric, column] of [
    ["completion_ms", 1],
    ["first_edit_ms", 2],
    ["first_evidence_ms", 3],
    ["task_wall_ms", 4],
  ]) {
    const m = cell.metrics[metric];
    const expected = `${m.p50}/${m.p95}`;
    const cells = row(fullTable, cell.fixture);
    if (cells[column] !== expected) {
      fail(
        `${cell.fixture} ${metric}: report column says ${cells[column]}, artifact says ${expected}`
      );
    }
  }
}

// ---- twelve serial/reference control p50s ---------------------------------
for (const fixture of [...new Set(matrix.cells.map((c) => c.fixture))]) {
  const cells = row(controlsTable, fixture);
  for (const [mode, column] of [["serial", 1], ["reference", 2]]) {
    const cell = matrix.cells.find((c) => c.fixture === fixture && c.mode === mode);
    const expected = `${cell.metrics.completion_ms.p50} / ${cell.metrics.first_edit_ms.p50}`;
    if (cells[column] !== expected) {
      fail(
        `${fixture} ${mode} p50 (completion / TTFR): report says ${cells[column]}, artifact says ${expected}`
      );
    }
  }
}

// ---- model_ms p50 range across every cell ---------------------------------
const p50s = matrix.cells.map((c) => c.metrics.model_ms.p50);
const lo = Math.floor(Math.min(...p50s) * 1000) / 1000;
const hi = Math.floor(Math.max(...p50s) * 1000) / 1000;
const range = `${lo.toFixed(3)}–${hi.toFixed(3)}`;
if (!modelSection.includes(range)) {
  fail(`model section lacks the measured p50 range ${range}`);
}

// ---- legs (both pairs, scoped to the critical-path section) ---------------
const criticalSection = section("## Critical-path breakdown", "## Model, Jev and tool calls");
for (const leg of matrix.legs) {
  const pair = `${leg.p50_us}/${leg.p95_us}`;
  if (!criticalSection.includes(pair)) {
    fail(`leg ${leg.leg} p50/p95 ${pair} missing from the critical-path section`);
  }
}

console.log("m14 reconcile ok");
