import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {root,sha256,requireTrue,filesUnder,inputFiles,protocol} from './heldout_manifest.mjs';
export {root,sha256,requireTrue,protocol};
export const ids=['canonical-frame','normalized-registry'];
export const manifestPath=path.join(root,'docs/milestones/MULTIFILE_MANIFEST.json');
export function plannedOrder(){const rows=[];for(let sample=1;sample<=5;sample++){const rotated=ids.map((_,i)=>ids[(i+sample-1)%ids.length]);for(const fixture of rotated)for(const mode of sample%2?['full','serial']:['serial','full'])rows.push({fixture,mode,sample});}return rows;}
export function makeManifest(){const files=[...new Set([...inputFiles(),...ids.flatMap(id=>[...filesUnder(path.join(root,'fixtures',id)),...filesUnder(path.join(root,'fixtures/solutions',id))]).map(p=>path.relative(root,p)),...['docs/milestones/MULTIFILE_PLAN.md','scripts/multifile_manifest.mjs','scripts/multifile_oracles.mjs','scripts/multifile_archive.mjs','scripts/multifile_archive.test.mjs','scripts/multifile_check.mjs','scripts/multifile_check.test.mjs','scripts/multifile_run.mjs']])].sort();return{schema:1,protocol,planned_order:plannedOrder(),files:Object.fromEntries(files.map(p=>[p,sha256(fs.readFileSync(path.join(root,p)))]))};}
export function assertFrozen(m){requireTrue(JSON.stringify(m)===JSON.stringify(makeManifest()),'multifile frozen inputs changed');}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){if(process.argv.includes('--freeze'))fs.writeFileSync(manifestPath,JSON.stringify(makeManifest(),null,2)+'\n',{flag:'wx'});assertFrozen(JSON.parse(fs.readFileSync(manifestPath,'utf8')));console.log('multifile manifest valid');}
