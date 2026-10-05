#!/usr/bin/env node
// M14 progress checker (gate G9).
//
// PROGRESS.md must carry a dated Milestone 14 completed-gates entry, the
// current milestone must have advanced past it, and the README status
// line must claim Milestone 14 or later. The docs-freshness test separately
// requires the latest completed milestone and its successor.
"use strict";

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function fail(message) {
  console.error(message);
  process.exit(1);
}

function read(name) {
  const file = path.join(root, name);
  if (!fs.existsSync(file)) fail(`missing ${name}`);
  return fs.readFileSync(file, "utf8");
}

const progress = read("PROGRESS.md");
const readme = read("README.md");

const gatesSection = progress.split("## Completed gates")[1];
if (!gatesSection) fail("PROGRESS.md has no Completed gates section");
if (!/- \d{4}-\d{2}-\d{2} Milestone 14\b/.test(gatesSection)) {
  fail("PROGRESS.md lacks a dated Milestone 14 completed-gates entry");
}

const currentSection = progress.split("## Current milestone")[1];
if (!currentSection) fail("PROGRESS.md has no Current milestone section");
const currentNumber = currentSection.match(/Milestone (\d+)/);
if (!currentNumber || Number(currentNumber[1]) < 15) {
  fail(`Current milestone should advance past Milestone 14, got ${currentNumber?.[1]}`);
}

const statusLine = readme.split("\n").find((line) => line.includes("Status ("));
if (!statusLine) fail("README.md needs a Status line");
const statusNumber = statusLine.match(/Milestone (\d+)/);
if (!statusNumber || Number(statusNumber[1]) < 14) {
  fail(`README status lags M14: ${statusLine.trim()}`);
}

console.log("progress ok");
