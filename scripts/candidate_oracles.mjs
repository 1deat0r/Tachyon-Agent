import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {spawnSync,execFileSync} from 'node:child_process';
import {root,sha256,requireTrue} from './candidate_manifest.mjs';
// Historical frozen provenance is checked against the source used for M16.
const historical=JSON.parse(fs.readFileSync(path.join(root,'docs/milestones/HELDOUT_MANIFEST.json'),'utf8'));
const meta=JSON.parse(fs.readFileSync(path.join(root,'docs/milestones/HELDOUT_BASELINE_META.json'),'utf8'));
for(const [file,hash] of Object.entries(historical.files))requireTrue(sha256(execFileSync('git',['show',`${meta.source_commit}:${file}`],{cwd:root,maxBuffer:4*1024*1024}))===hash,'historical M16 provenance changed');
for(const id of ['signed-midpoint','ceiling-division']){
 const scratch=fs.mkdtempSync(path.join(os.tmpdir(),'tachyon-candidate-oracle-')),ws=path.join(scratch,'ws');
 const cargo=args=>{const r=spawnSync('cargo',['test','--offline','--locked',...args],{cwd:ws,env:{...process.env,CARGO_BUILD_JOBS:'2'},encoding:'utf8',timeout:180000,maxBuffer:4*1024*1024});if(r.error)throw r.error;requireTrue(r.signal===null&&[0,101].includes(r.status),'oracle process failure');return {status:r.status,text:r.stdout+r.stderr};};
 try{
  fs.cpSync(path.join(root,'fixtures',id),ws,{recursive:true,filter:p=>path.basename(p)!=='target'});
  requireTrue(cargo(['--no-run']).status===0,'broken fixture must compile');
  const broken=cargo(['-p',id,'--test','visible']);requireTrue(broken.status===101&&broken.text.includes('visible_regression ... FAILED'),'broken-first control');
  const rel='subject/src/implementation.rs',solution=fs.readFileSync(path.join(root,'fixtures/solutions',id,rel),'utf8');fs.writeFileSync(path.join(ws,rel),solution);
  requireTrue(cargo([]).status===0,'known solution fails acceptance');
  const api=solution.replace('pub fn format_label','pub fn missing_label');requireTrue(api!==solution,'vacuous API mutant');fs.writeFileSync(path.join(ws,rel),api);requireTrue(cargo(['--no-run']).status===101,'API drift escaped');
  fs.writeFileSync(path.join(ws,rel),solution);
  const edge=id==='signed-midpoint'?solution.replace('(a & b) + ((a ^ b) >> 1)','((a as i128 + b as i128) / 2) as i64'):solution.replace('if denominator == 0 {','if numerator == 0 { return Some(1); }\n    if denominator == 0 {');
  requireTrue(edge!==solution,'vacuous boundary mutant');fs.writeFileSync(path.join(ws,rel),edge);
  requireTrue(cargo(['-p',id,'--test','visible']).status===0,'hidden mutant must pass visible');
  const failure=cargo(['-p',id,'--test','holdout']);requireTrue(failure.status===101&&failure.text.includes('boundaries_match_wide_arithmetic ... FAILED'),'hidden boundary escaped');
  console.log(`${id}: broken-first, solution, API and hidden-boundary controls passed`);
 }finally{fs.rmSync(scratch,{recursive:true,force:true});}
}
console.log('candidate oracles passed');
