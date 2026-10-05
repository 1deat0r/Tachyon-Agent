import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {spawnSync} from 'node:child_process';
import {root,ids,sha256,requireTrue} from './stateful_manifest.mjs';
import {filesUnder} from './heldout_manifest.mjs';
const snapshot=dir=>Object.fromEntries(filesUnder(dir).map(p=>[path.relative(dir,p),sha256(fs.readFileSync(p))]));
function changes(a,b){const x=snapshot(a),y=snapshot(b);return [...new Set([...Object.keys(x),...Object.keys(y)])].filter(p=>x[p]!==y[p]).sort();}
function assertAuthorized(a,b,allowed){requireTrue(JSON.stringify(changes(a,b))===JSON.stringify(allowed),'protected or incomplete change set');}
const visibleTests={
 'reservation-ledger':{'release.rs':'visible_release_is_atomic','reserve.rs':'visible_reserve_is_atomic'},
 'indexed-catalog':{'insert.rs':'visible_insert_rejects_owned_name','remove.rs':'visible_remove_clears_reverse_index','rename.rs':'visible_rename_preserves_state_on_collision'},
};
for(const id of ids){
 const fixture=path.join(root,'fixtures',id),before=snapshot(fixture),d=JSON.parse(fs.readFileSync(path.join(fixture,'bench.json'),'utf8'));
 const scratch=fs.mkdtempSync(path.join(os.tmpdir(),'tachyon-stateful-oracle-')),ws=path.join(scratch,'ws');
 const cargo=args=>{const r=spawnSync('cargo',['test','--offline','--locked',...args],{cwd:ws,env:{...process.env,CARGO_BUILD_JOBS:'2'},encoding:'utf8',timeout:180000,maxBuffer:4*1024*1024});if(r.error)throw r.error;requireTrue(r.signal===null&&[0,101].includes(r.status),'cargo process failure');return{status:r.status,text:r.stdout+r.stderr};};
 try{
  fs.cpSync(fixture,ws,{recursive:true,filter:p=>path.basename(p)!=='target'});
  requireTrue(!d.evidence_paths.includes('subject/tests/holdout.rs'),'hidden oracle in evidence');
  requireTrue(cargo(['--no-run']).status===0,'broken fixture must compile');
  const broken=cargo(['-p',id,'--test','visible']);requireTrue(broken.status===101,'broken fixture must fail behavior');
  for(const name of Object.values(visibleTests[id]))requireTrue(broken.text.includes(`${name} ... FAILED`),'broken defect not demonstrated');
  const originals=Object.fromEntries(d.change_paths.map(p=>[p,fs.readFileSync(path.join(ws,p),'utf8')]));
  const solutions=Object.fromEntries(d.change_paths.map(p=>[p,fs.readFileSync(path.join(root,'fixtures/solutions',id,p),'utf8')]));
  const restore=texts=>{for(const [p,text] of Object.entries(texts))fs.writeFileSync(path.join(ws,p),text);};
  const completeMask=(1<<d.change_paths.length)-1;
  for(let mask=1;mask<completeMask;mask++){
   restore(originals);d.change_paths.forEach((p,i)=>{if(mask&(1<<i))fs.writeFileSync(path.join(ws,p),solutions[p]);});
   requireTrue(cargo(['--no-run']).status===0,'partial repair must compile');
   const r=cargo(['-p',id,'--test','visible']);requireTrue(r.status===101,'partial repair escaped acceptance');
   d.change_paths.forEach((p,i)=>{if(!(mask&(1<<i)))requireTrue(r.text.includes(`${visibleTests[id][path.basename(p)]} ... FAILED`),'partial repair failed for unrelated reason');});
  }
  restore(solutions);requireTrue(cargo([]).status===0,'complete solution fails full acceptance');assertAuthorized(fixture,ws,d.change_paths);
  const apiPath=d.change_paths[0],method=id==='reservation-ledger'?'release':'insert';
  const api=solutions[apiPath].replace(`pub fn ${method}(`,`pub fn missing_${method}(`);requireTrue(api!==solutions[apiPath],'vacuous API mutant');fs.writeFileSync(path.join(ws,apiPath),api);requireTrue(cargo(['--no-run']).status===101,'API drift escaped');restore(solutions);
  if(id==='reservation-ledger'){
   for(const [p,text] of Object.entries(solutions)){
    const mutant=text.replace('*totals.entry(*sku).or_insert(0) += u64::from(*quantity);','totals.insert(*sku, u64::from(*quantity));');
    requireTrue(mutant!==text,'vacuous duplicate batch mutant');fs.writeFileSync(path.join(ws,p),mutant);
   }
  }else{
   const p='subject/src/rename.rs',text=solutions[p];
   const mutant=text.replace('self.owners.remove(&previous);','let _ = previous;');requireTrue(mutant!==text,'vacuous stale index mutant');fs.writeFileSync(path.join(ws,p),mutant);
  }
  requireTrue(cargo(['-p',id,'--test','visible']).status===0,'hidden mutant must pass visible tests');
  const hidden=cargo(['-p',id,'--test','holdout']);const test=id==='reservation-ledger'?'sequences_match_independent_state_oracle':'sequences_match_independent_index_oracle';requireTrue(hidden.status===101&&hidden.text.includes(`${test} ... FAILED`),'independent sequence oracle escaped');
  restore(solutions);fs.appendFileSync(path.join(ws,'subject/src/lib.rs'),'\n// protected drift\n');let rejected=false;try{assertAuthorized(fixture,ws,d.change_paths);}catch{rejected=true;}requireTrue(rejected,'protected drift escaped');
  requireTrue(JSON.stringify(snapshot(fixture))===JSON.stringify(before),'repository fixture changed');
  console.log(`${id}: broken-first, complete solution, ${completeMask-1} partial repairs, API, hidden sequence mutant and protection passed`);
 }finally{fs.rmSync(scratch,{recursive:true,force:true});}
}
console.log('stateful oracles passed');
