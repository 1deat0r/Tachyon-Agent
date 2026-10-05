import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {root,sha256,requireTrue,filesUnder,inputFiles,protocol} from './heldout_manifest.mjs';
export {root,sha256,requireTrue,protocol};
export const ids=['duplicate-range','signed-midpoint','ceiling-division'];
export const variants=['baseline','boundary-guidance'];
export const manifestPath=path.join(root,'docs/milestones/CANDIDATE_MANIFEST.json');
export function plannedOrder(){return ids.flatMap(fixture=>Array.from({length:5},(_,i)=>(i%2?[...variants].reverse():variants).map(proposal_variant=>({fixture,mode:'full',sample:i+1,proposal_variant}))).flat());}
export function makeManifest(){
 const files=[...new Set([...inputFiles(),...ids.slice(1).flatMap(id=>[...filesUnder(path.join(root,'fixtures',id)),...filesUnder(path.join(root,'fixtures/solutions',id))]).map(p=>path.relative(root,p)),...['docs/milestones/CANDIDATE_PLAN.md','scripts/candidate_manifest.mjs','scripts/candidate_oracles.mjs','scripts/candidate_check.mjs','scripts/candidate_check.test.mjs','scripts/candidate_run.mjs']])].sort();
 return {schema:1,protocol,planned_order:plannedOrder(),files:Object.fromEntries(files.map(p=>[p,sha256(fs.readFileSync(path.join(root,p)))]))};
}
export function assertFrozen(m){requireTrue(JSON.stringify(m)===JSON.stringify(makeManifest()),'candidate frozen inputs changed');}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 if(process.argv.includes('--freeze'))fs.writeFileSync(manifestPath,JSON.stringify(makeManifest(),null,2)+'\n',{flag:'wx'});
 assertFrozen(JSON.parse(fs.readFileSync(manifestPath,'utf8')));console.log('candidate manifest valid');
}
