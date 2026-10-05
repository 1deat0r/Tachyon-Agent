import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {root,ids,filesUnder,sha256,requireTrue} from './heldout_manifest.mjs';
export function changedPaths(before,after) {
 const snapshot=(dir)=>Object.fromEntries(filesUnder(dir).map(p=>[path.relative(dir,p),sha256(fs.readFileSync(p))]));
 const a=snapshot(before),b=snapshot(after);
 return [...new Set([...Object.keys(a),...Object.keys(b)])].filter(p=>a[p]!==b[p]).sort();
}
export function assertChanges(before,after,allowed) {
 requireTrue(JSON.stringify(changedPaths(before,after))===JSON.stringify([...allowed].sort()),'protected-file or change-set violation');
}
function cargo(ws,target,args) {
 const r=spawnSync('cargo',['test','--offline','--locked',...args],{cwd:ws,env:{...process.env,CARGO_TARGET_DIR:target,CARGO_BUILD_JOBS:'2'},encoding:'utf8',timeout:180000,maxBuffer:4*1024*1024});
 if(r.error)throw r.error;
 requireTrue(r.signal===null&&Number.isInteger(r.status),'cargo did not finish');
 return {ok:r.status===0,text:r.stdout+r.stderr};
}
const apiMutants={
 'utf8-boundary':s=>s.replace('pub fn label(&self)','pub fn missing_label(&self)'),
 'atomic-transfer':s=>s.replace('pub fn balances(&self)','pub fn missing_balances(&self)'),
 'duplicate-range':s=>s.replace('pub fn format_label','pub fn missing_label'),
};
const edgeMutants={
 'utf8-boundary':s=>s.replace('limit.min(self.text.len())','if limit == 0 { self.text.len() } else { limit.min(self.text.len()) }'),
 'atomic-transfer':s=>s.replace('let source =','if amount == 0 { return Err(TransferError::Overflow); }\n        let source ='),
 'duplicate-range':s=>s.replace('let start =','if values.is_empty() { return 0..1; }\n    let start ='),
};
for(const id of ids) {
 const source=path.join(root,'fixtures',id),scratch=fs.mkdtempSync(path.join(os.tmpdir(),'tachyon-heldout-oracle-')),ws=path.join(scratch,'ws'),target=path.join(scratch,'target');
 try {
  fs.cpSync(source,ws,{recursive:true,filter:p=>path.basename(p)!=='target'});
  requireTrue(cargo(ws,target,['--no-run']).ok,`${id}: broken fixture must compile`);
  const descriptor=JSON.parse(fs.readFileSync(path.join(source,'bench.json'),'utf8'));
  const broken=cargo(ws,target,['-p',descriptor.requested_checks[0],'--test','visible']);
  requireTrue(!broken.ok&&/visible_.*FAILED/.test(broken.text),`${id}: behavioral broken-first control failed`);
  requireTrue(!descriptor.evidence_paths.includes('subject/tests/holdout.rs'),`${id}: hidden oracle leaked into evidence`);
  const rel='subject/src/implementation.rs',file=path.join(ws,rel);
  const fixed=fs.readFileSync(path.join(root,'fixtures/solutions',id,rel),'utf8');fs.writeFileSync(file,fixed);
  requireTrue(cargo(ws,target,[]).ok,`${id}: known solution fails`);
  assertChanges(source,ws,descriptor.change_paths);
  const api=apiMutants[id](fixed);requireTrue(api!==fixed,`${id}: vacuous API mutant`);fs.writeFileSync(file,api);
  requireTrue(!cargo(ws,target,['--no-run']).ok,`${id}: API corruption escaped`);
  const edge=edgeMutants[id](fixed);requireTrue(edge!==fixed,`${id}: vacuous edge mutant`);fs.writeFileSync(file,edge);
  requireTrue(cargo(ws,target,['-p',JSON.parse(fs.readFileSync(path.join(source,'bench.json'),'utf8')).requested_checks[0],'--test','visible']).ok,`${id}: edge mutant must pass visible regression`);
  requireTrue(!cargo(ws,target,['-p',descriptor.requested_checks[0],'--test','holdout']).ok,`${id}: hidden edge mutant escaped`);
  fs.writeFileSync(file,fixed);fs.appendFileSync(path.join(ws,'client/src/lib.rs'),'\n// harmless protected drift\n');
  let rejected=false;try{assertChanges(source,ws,descriptor.change_paths);}catch{rejected=true;}
  requireTrue(rejected,`${id}: protected-file drift escaped`);
  console.log(`${id}: compile, broken-first, solution, API, hidden-edge and protected-file controls passed`);
 }finally{fs.rmSync(scratch,{recursive:true,force:true});}
}
console.log('heldout oracles passed');
