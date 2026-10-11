import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {root, sha256, requireTrue, filesUnder} from './heldout_manifest.mjs';
export {root, sha256, requireTrue};

export const fixture = 'reservation-ledger';
export const manifestPath = path.join(root, 'docs/milestones/M23_MANIFEST.json');

// M23 replays the four retained M20 failure classes through the production
// driver (no live calls) plus the default-off flag proof. Any prompt, retry,
// or deadline candidate must keep these green.
export const replay = [
  {cell: `${fixture}/full/3`, failure_class: 'partial_repair'},
  {cell: `${fixture}/serial/3`, failure_class: 'transport_error'},
  {cell: `${fixture}/full/4`, failure_class: 'model_stage_deadline'},
  {cell: `${fixture}/serial/4`, failure_class: 'model_stage_deadline'},
];

export function frozenFiles() {
  const retained = [
    'docs/milestones/STATEFUL_SAMPLES.jsonl',
    'docs/milestones/M21_MANIFEST.json',
    'docs/milestones/M21_PLAN.md',
    'docs/milestones/M23_PLAN.md',
    'docs/milestones/M23_TRIAL_MANIFEST.md',
    'scripts/m23_manifest.mjs',
    'scripts/m23_run.mjs',
    'crates/tachyon-core/src/runtime.rs',
    'crates/tachyon-core/src/driver.rs',
    'crates/tachyon-core/src/driver/model.rs',
    'crates/tachyon-core/examples/bench_matrix.rs',
  ];
  return [
    ...filesUnder(path.join(root, 'fixtures', fixture)),
    ...filesUnder(path.join(root, 'fixtures/solutions', fixture)),
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
  return {schema: 1, replay, files};
}

export function assertFrozen(m) {
  const current = makeManifest();
  requireTrue(m.schema === 1, 'schema changed');
  requireTrue(JSON.stringify(m.replay) === JSON.stringify(current.replay), 'replay map changed');
  requireTrue(JSON.stringify(m.files) === JSON.stringify(current.files), 'frozen input hash mismatch');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.includes('--freeze')) {
      fs.writeFileSync(manifestPath, JSON.stringify(makeManifest(), null, 2) + '\n', {flag: 'wx'});
    }
    assertFrozen(JSON.parse(fs.readFileSync(manifestPath, 'utf8')));
    console.log('m23 manifest valid');
  } catch (e) {
    console.error(e.message);
    process.exitCode = 1;
  }
}
