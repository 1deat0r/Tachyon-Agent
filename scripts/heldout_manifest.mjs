import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
export const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
export const ids=['utf8-boundary','atomic-transfer','duplicate-range'];
export const protocol={samples_per_cell:5,model:'mimo-v2.6-flash',endpoint:'https://api.xiaomimimo.com',temperature:0,output_budget_tokens:4096,model_stage_deadline_ms:120000,deadline_tolerance_ms:1000,max_attempts:2};
export const manifestPath=path.join(root,'docs/milestones/HELDOUT_MANIFEST.json');
export const sha256=(bytes)=>crypto.createHash('sha256').update(bytes).digest('hex');
export const requireTrue=(ok,message)=>{if(!ok)throw new Error(message);};
export function filesUnder(dir) {
 const files=[];
 for(const item of fs.readdirSync(dir,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))) {
  if(item.name==='target')continue;
  const p=path.join(dir,item.name);
  requireTrue(!item.isSymbolicLink(),'symlink in frozen inputs');
  if(item.isDirectory())files.push(...filesUnder(p));else if(item.isFile())files.push(p);
 }
 return files;
}
export function inputFiles() {
 return [...ids.flatMap(id=>[...filesUnder(path.join(root,'fixtures',id)),...filesUnder(path.join(root,'fixtures/solutions',id))]),
  ...['docs/milestones/HELDOUT_PLAN.md','scripts/heldout_manifest.mjs','scripts/heldout_oracles.mjs','scripts/heldout_bench_failure.mjs','scripts/heldout_check.mjs','scripts/heldout_check.test.mjs','scripts/heldout_run.mjs','scripts/live_check.mjs','crates/tachyon-core/src/driver.rs','crates/tachyon-core/src/driver/model.rs','crates/tachyon-core/examples/bench_matrix.rs','crates/tachyon-models/src/decision.rs','crates/tachyon-models/src/provider.rs','crates/tachyon-models/src/openai_compat.rs'].map(p=>path.join(root,p))].map(p=>path.relative(root,p).replaceAll('\\','/')).sort();
}
export function makeManifest() {
 return {schema:1,protocol,tasks:ids.map(id=>({id,descriptor_sha256:sha256(fs.readFileSync(path.join(root,'fixtures',id,'bench.json')))})),files:Object.fromEntries(inputFiles().map(p=>[p,sha256(fs.readFileSync(path.join(root,p)))]))};
}
export function assertFrozen(m) {
 const current=makeManifest();
 requireTrue(m.schema===1&&JSON.stringify(m.protocol)===JSON.stringify(protocol),'protocol changed');
 requireTrue(JSON.stringify(m.tasks)===JSON.stringify(current.tasks),'task descriptors changed');
 requireTrue(JSON.stringify(m.files)===JSON.stringify(current.files),'frozen input hash mismatch');
}
export function plannedOrder() {
 const order=[];
 for(let sample=1;sample<=protocol.samples_per_cell;sample++) {
  const rotated=ids.map((_,i)=>ids[(i+sample-1)%ids.length]);
  for(const fixture of rotated)for(const mode of sample%2?['full','serial']:['serial','full'])order.push({fixture,mode,sample});
 }
 return order;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
 try {
  if(process.argv.includes('--freeze'))fs.writeFileSync(manifestPath,JSON.stringify(makeManifest(),null,2)+'\n',{flag:'wx'});
  assertFrozen(JSON.parse(fs.readFileSync(manifestPath,'utf8')));console.log('heldout manifest valid');
 }catch(e){console.error(e.message);process.exitCode=1;}
}
