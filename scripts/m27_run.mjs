import fs from 'node:fs';
import path from 'node:path';
import {spawn, execFileSync} from 'node:child_process';
import {root, manifestPath, assertFrozen, sha256, requireTrue} from './m27_manifest.mjs';
const output = path.resolve(process.argv[2] ?? path.join(root, 'target/m27/raw.jsonl'));
const statePath = output.replace(/\.jsonl$/, '') + '.state.json';
const metaPath = output.replace(/\.jsonl$/, '') + '.meta.json';
let child;
let interrupted = false;
const state = {complete: false, recorded_samples: 0};
function save() { fs.writeFileSync(statePath + '.tmp', JSON.stringify(state, null, 2) + '\n'); fs.renameSync(statePath + '.tmp', statePath); }
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { interrupted = true; child?.kill(signal); save(); process.exitCode = 1; });
try {
  requireTrue(!fs.existsSync(output) && !fs.existsSync(statePath) && !fs.existsSync(metaPath), 'refusing to overwrite batch');
  const manifestText = fs.readFileSync(manifestPath, 'utf8'), manifest = JSON.parse(manifestText);
  assertFrozen(manifest);
  const commit = execFileSync('git', ['rev-parse', 'HEAD'], {cwd: root, encoding: 'utf8'}).trim();
  for (const file of [...Object.keys(manifest.files), 'docs/milestones/M27_MANIFEST.json']) {
    const committed = execFileSync('git', ['show', `${commit}:${file}`], {cwd: root, maxBuffer: 8 * 1024 * 1024, stdio: ['ignore', 'pipe', 'ignore']});
    requireTrue(sha256(committed) === sha256(fs.readFileSync(path.join(root, file))), 'uncommitted frozen input');
  }
  fs.mkdirSync(path.dirname(output), {recursive: true}); save();
  // Build the release test binary first so its hash pins the measured code.
  execFileSync('cargo', ['test', '--release', '-p', 'tachyon-acp', '--test', 'session_prompt_live_reslink', '--locked', '--no-run'], {cwd: root, stdio: 'inherit'});
  const deps = fs.readdirSync(path.join(root, 'target/release/deps'))
    .filter((n) => n.startsWith('session_prompt_live_reslink-') && !n.endsWith('.d'))
    .map((n) => path.join(root, 'target/release/deps', n))
    .filter((p) => fs.statSync(p).isFile());
  requireTrue(deps.length > 0, 'test binary not built');
  deps.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
  const binaryHash = sha256(fs.readFileSync(deps[0]));
  fs.writeFileSync(metaPath, JSON.stringify({schema: 1, source_commit: commit, binary_sha256: binaryHash, manifest_sha256: sha256(manifestText), protocol: 'm27-acp-live-turn', model: 'meta/muse-spark-1.3-contributor'}, null, 2) + '\n', {flag: 'wx'});
  fs.writeFileSync(output, '', {flag: 'wx'});
  const command = 'set -a; . "$1"; set +a; export TACHYON_ACP_LIVE=1; export TACHYON_M27_OUTPUT="$2"; export TACHYON_LIVE_BASE_URL=http://127.0.0.1:14000; export TACHYON_LIVE_MODEL=meta/muse-spark-1.3-contributor; export TACHYON_LIVE_API_KEY_ENV=TACHYON_LIVE_API_KEY_UNSET; exec cargo test --release -p tachyon-acp --test session_prompt_live_reslink --locked -- --ignored --test-threads=1';
  const result = await new Promise((resolve, reject) => {
    child = spawn('bash', ['-c', command, 'm27', path.join(process.env.HOME, '.config/tachyon/env'), output], {cwd: root, stdio: ['inherit', 'ignore', 'inherit']});
    child.on('error', (e) => reject(e));
    child.on('close', (code, signal) => resolve({code, signal}));
  });
  requireTrue(result.signal === null && result.code === 0, 'live test binary failed');
  const rows = fs.readFileSync(output, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
  requireTrue(rows.length === 3, `expected 3 rows, got ${rows.length}`);
  for (const row of rows) requireTrue(Array.isArray(row.model_attempts), 'incomplete sample');
  state.recorded_samples = rows.length; state.complete = true; save();
  for (const row of rows) console.log(`${row.fixture}/${row.mode}/s${row.sample}: ${row.outcome}; calls=${row.model_calls}`);
  console.log(`m27 batch complete: ${state.recorded_samples} samples`);
} catch (e) { console.error(e.message); process.exitCode = 1; }
