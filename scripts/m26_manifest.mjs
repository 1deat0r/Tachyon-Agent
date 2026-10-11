import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {root, sha256, requireTrue, filesUnder} from './heldout_manifest.mjs';
export {root, sha256, requireTrue};

export const fixtures = ['ceiling-division', 'signed-midpoint', 'atomic-transfer'];
export const manifestPath = path.join(root, 'docs/milestones/M26_MANIFEST.json');

// M26 is an evaluation on held-out fixtures (no live coverage to date).
// No production change is proposed, so there is no adopt/reject rule —
// only the pre-registered reporting rules in M26_PLAN.md.
export function frozenFiles() {
  const retained = [
    'docs/milestones/M26_PLAN.md',
    'docs/milestones/M24_PLAN.md',
    'docs/milestones/M26_PLAN.md',
    'scripts/m26_manifest.mjs',
    'scripts/m26_run.mjs',
    'crates/tachyon-core/src/runtime.rs',
    'crates/tachyon-core/src/driver.rs',
    'crates/tachyon-core/src/driver/model.rs',
    'crates/tachyon-core/examples/bench_matrix.rs',
  ];
  const listed = [];
  for (const fixture of fixtures) {
    listed.push(...filesUnder(path.join(root, 'fixtures', fixture)));
    listed.push(...filesUnder(path.join(root, 'fixtures/solutions', fixture)));
  }
  return [...listed, ...retained]
    .map((p) => path.relative(root, p).replaceAll('\\', '/'))
    .sort();
}

export function plannedOrder() {
  const rows = [];
  for (const fixture of fixtures) {
    for (let sample = 1; sample <= 3; sample++) {
      rows.push({fixture, mode: 'full', sample, proposal_variant: 'm23-framing'});
    }
  }
  return rows;
}

export function makeManifest() {
  const files = {};
  for (const rel of frozenFiles()) {
    files[rel] = sha256(fs.readFileSync(path.join(root, rel)));
  }
  return {schema: 1, model: 'meta/muse-spark-1.3-contributor', planned_order: plannedOrder(), files};
}

export function assertFrozen(m) {
  const current = makeManifest();
  requireTrue(m.schema === 1, 'schema changed');
  requireTrue(JSON.stringify(m.planned_order) === JSON.stringify(current.planned_order), 'planned order changed');
  requireTrue(JSON.stringify(m.files) === JSON.stringify(current.files), 'frozen input hash mismatch');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.includes('--freeze')) {
      fs.writeFileSync(manifestPath, JSON.stringify(makeManifest(), null, 2) + '\n', {flag: 'wx'});
    }
    assertFrozen(JSON.parse(fs.readFileSync(manifestPath, 'utf8')));
    console.log('m26 manifest valid');
  } catch (e) {
    console.error(e.message);
    process.exitCode = 1;
  }
}
