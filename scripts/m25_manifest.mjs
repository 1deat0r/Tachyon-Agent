import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {root, sha256, requireTrue, filesUnder} from './heldout_manifest.mjs';
export {root, sha256, requireTrue};

export const manifestPath = path.join(root, 'docs/milestones/M25_MANIFEST.json');

// M25: first live-model ACP prompt turns (evaluation, no production
// change, no adopt/reject — reporting rules in M25_PLAN.md).
export function frozenFiles() {
  const retained = [
    'docs/milestones/M25_PLAN.md',
    'crates/tachyon-acp/tests/session_prompt_live_model.rs',
    'crates/tachyon-acp/tests/common/mod.rs',
    'scripts/m25_manifest.mjs',
    'scripts/m25_run.mjs',
    'crates/tachyon-acp/src/server.rs',
    'crates/tachyon-acp/src/turn.rs',
  ];
  return [
    ...filesUnder(path.join(root, 'fixtures', 'duplicate-range')),
    ...filesUnder(path.join(root, 'fixtures/solutions', 'duplicate-range')),
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
  return {schema: 1, model: 'meta/muse-spark-1.3-contributor', rows: 3, files};
}

export function assertFrozen(m) {
  const current = makeManifest();
  requireTrue(m.schema === 1, 'schema changed');
  requireTrue(m.rows === 3 && JSON.stringify(m.rows) === JSON.stringify(current.rows), 'row count changed');
  requireTrue(JSON.stringify(m.files) === JSON.stringify(current.files), 'frozen input hash mismatch');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.includes('--freeze')) {
      fs.writeFileSync(manifestPath, JSON.stringify(makeManifest(), null, 2) + '\n', {flag: 'wx'});
    }
    assertFrozen(JSON.parse(fs.readFileSync(manifestPath, 'utf8')));
    console.log('m25 manifest valid');
  } catch (e) {
    console.error(e.message);
    process.exitCode = 1;
  }
}
