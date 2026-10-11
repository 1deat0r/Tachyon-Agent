import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {root, sha256, requireTrue, filesUnder} from './heldout_manifest.mjs';
export {root, sha256, requireTrue};

export const fixture = 'reservation-ledger';
export const manifestPath = path.join(root, 'docs/milestones/M21_MANIFEST.json');

// The four retained M20 failure cells this milestone replays
// deterministically through the production driver (no live calls).
export const replay = [
  {cell: `${fixture}/full/3`, failure_class: 'partial_repair', evidence: 'docs/milestones/STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.reserve.rs.txt'},
  {cell: `${fixture}/serial/3`, failure_class: 'transport_error', evidence: 'docs/milestones/STATEFUL_SAMPLES.jsonl'},
  {cell: `${fixture}/full/4`, failure_class: 'model_stage_deadline', evidence: 'docs/milestones/STATEFUL_SAMPLES.jsonl'},
  {cell: `${fixture}/serial/4`, failure_class: 'model_stage_deadline', evidence: 'docs/milestones/STATEFUL_SAMPLES.jsonl'}
];

export function frozenFiles() {
  const retained = [
    'docs/milestones/STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.reserve.rs.txt',
    'docs/milestones/STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.release.rs.txt',
    'docs/milestones/STATEFUL_FAILED_RESERVATION_LEDGER_FULL_3.log.txt',
    'docs/milestones/STATEFUL_SAMPLES.jsonl',
    'docs/milestones/M21_PLAN.md',
    'scripts/m21_manifest.mjs',
    'crates/tachyon-core/src/driver.rs',
    'crates/tachyon-core/src/driver/model.rs',
    'crates/tachyon-core/examples/bench_matrix.rs'
  ];
  return [
    ...filesUnder(path.join(root, 'fixtures', fixture)),
    ...filesUnder(path.join(root, 'fixtures/solutions', fixture)),
    ...retained
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
    console.log('m21 manifest valid');
  } catch (e) {
    console.error(e.message);
    process.exitCode = 1;
  }
}
