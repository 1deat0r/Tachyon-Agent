import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {root, sha256, requireTrue, filesUnder} from './heldout_manifest.mjs';
export {root, sha256, requireTrue};

export const manifestPath = path.join(root, 'docs/milestones/M23B_MANIFEST.json');

export const rows = [];
for (const sample of [1, 2, 3]) {
  rows.push({fixture: 'reservation-ledger', mode: 'full', sample, proposal_variant: 'm23-framing'});
  rows.push({fixture: 'reservation-ledger', mode: 'serial', sample, proposal_variant: 'm23-framing'});
}

export function plannedOrder() { return rows; }

export function frozenFiles() {
  const retained = [
    'docs/milestones/M23B_PLAN.md',
    'docs/milestones/M23_PLAN.md',
    'docs/milestones/M23_DECISION_REPORT.md',
    'scripts/m23b_manifest.mjs',
    'scripts/m23b_run.mjs',
    'crates/tachyon-core/src/driver.rs',
    'crates/tachyon-core/src/driver/model.rs',
    'crates/tachyon-core/src/runtime.rs',
    'crates/tachyon-core/examples/bench_matrix.rs',
  ];
  return [
    ...filesUnder(path.join(root, 'fixtures', 'reservation-ledger')),
    ...filesUnder(path.join(root, 'fixtures/solutions', 'reservation-ledger')),
    ...retained,
  ]
    .map((p) => path.relative(root, p).replaceAll('\\', '/'))
    .sort();
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
  requireTrue(m.model === current.model, 'model changed');
  requireTrue(JSON.stringify(m.planned_order) === JSON.stringify(current.planned_order), 'row order changed');
  requireTrue(JSON.stringify(m.files) === JSON.stringify(current.files), 'frozen input hash mismatch');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.includes('--freeze')) {
      fs.writeFileSync(manifestPath, JSON.stringify(makeManifest(), null, 2) + '\n', {flag: 'wx'});
    }
    assertFrozen(JSON.parse(fs.readFileSync(manifestPath, 'utf8')));
    console.log('m23b manifest valid');
  } catch (e) {
    console.error(e.message);
    process.exitCode = 1;
  }
}
