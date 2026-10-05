import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {spawnSync} from 'node:child_process';
import {root,ids,sha256,requireTrue} from './multifile_manifest.mjs';
import {filesUnder} from './heldout_manifest.mjs';
function snapshot(dir){return Object.fromEntries(filesUnder(dir).map(p=>[path.relative(dir,p),sha256(fs.readFileSync(p))]));}
function assertAuthorized(a,b,allowed){requireTrue(JSON.stringify(changes(a,b))===JSON.stringify(allowed),'protected or incomplete change set');}
function changes(a,b){const x=snapshot(a),y=snapshot(b);return [...new Set([...Object.keys(x),...Object.keys(y)])].filter(p=>x[p]!==y[p]).sort();}
for(const id of ids){
 const fixture=path.join(root,'fixtures',id),before=snapshot(fixture),descriptor=JSON.parse(fs.readFileSync(path.join(fixture,'bench.json'),'utf8'));
 const scratch=fs.mkdtempSync(path.join(os.tmpdir(),'tachyon-multifile-oracle-')),ws=path.join(scratch,'ws');
 const cargo=args=>{const r=spawnSync('cargo',['test','--offline','--locked',...args],{cwd:ws,env:{...process.env,CARGO_BUILD_JOBS:'2'},encoding:'utf8',timeout:180000,maxBuffer:4*1024*1024});if(r.error)throw r.error;requireTrue(r.signal===null&&[0,101].includes(r.status),'cargo process failure');return{status:r.status,text:r.stdout+r.stderr};};
 try{
  fs.cpSync(fixture,ws,{recursive:true,filter:p=>path.basename(p)!=='target'});
  requireTrue(!descriptor.evidence_paths.includes('subject/tests/holdout.rs'),'hidden oracle in evidence');
  requireTrue(cargo(['--no-run']).status===0,'broken fixture must compile');
  const broken=cargo(['-p',id,'--test','visible']);requireTrue(broken.status===101&&broken.text.includes('visible_'),'broken-first behavioral control');
  const originals=Object.fromEntries(descriptor.change_paths.map(p=>[p,fs.readFileSync(path.join(ws,p),'utf8')]));
  const solutions=Object.fromEntries(descriptor.change_paths.map(p=>[p,fs.readFileSync(path.join(root,'fixtures/solutions',id,p),'utf8')]));
  for(const file of descriptor.change_paths){
   for(const [p,text] of Object.entries(originals))fs.writeFileSync(path.join(ws,p),text);
   fs.writeFileSync(path.join(ws,file),solutions[file]);requireTrue(cargo(['--no-run']).status===0,'partial repair must compile');
   const r=cargo(['-p',id,'--test','visible']);
   const expected=id==='canonical-frame'?(file.endsWith('encoder.rs')?'visible_decoder_golden':'visible_encoder_golden'):(file.endsWith('writer.rs')?'visible_reader_normalizes':'visible_writer_duplicate');
   requireTrue(r.status===101&&r.text.includes(`${expected} ... FAILED`),'partial repair escaped or failed for unrelated reason');
  }
  for(const [p,text] of Object.entries(solutions))fs.writeFileSync(path.join(ws,p),text);
  requireTrue(cargo([]).status===0,'known complete solution fails');
  assertAuthorized(fixture,ws,descriptor.change_paths);
  const apiPath=id==='canonical-frame'?'subject/src/encoder.rs':'subject/src/writer.rs';
  const api=solutions[apiPath].replace(id==='canonical-frame'?'pub fn encode(':'pub fn register(',id==='canonical-frame'?'pub fn missing_encode(':'pub fn missing_register(');requireTrue(api!==solutions[apiPath],'vacuous API mutant');fs.writeFileSync(path.join(ws,apiPath),api);requireTrue(cargo(['--no-run']).status===101,'API drift escaped');
  for(const [p,text] of Object.entries(solutions))fs.writeFileSync(path.join(ws,p),text);
  if(id==='canonical-frame'){
   for(const [p,text] of Object.entries(originals))fs.writeFileSync(path.join(ws,p),text);
   fs.writeFileSync(path.join(ws,'subject/tests/roundtrip_control.rs'),'use canonical_frame::{encode,decode};\n#[test] fn roundtrip_only_is_insufficient(){for n in [0,1,255,256]{let p=vec![42;n];assert_eq!(decode(&encode(&p).unwrap()).unwrap(),p);}}\n');
   requireTrue(cargo(['-p',id,'--test','roundtrip_control']).status===0,'mutually consistent wire mutant must roundtrip');
   const r=cargo(['-p',id,'--test','holdout']);requireTrue(r.status===101&&r.text.includes('lengths_and_errors_match_independent_wire_oracle ... FAILED'),'golden wire oracle escaped');
   fs.unlinkSync(path.join(ws,'subject/tests/roundtrip_control.rs'));
  }else{
   for(const [p,text] of Object.entries(solutions))fs.writeFileSync(path.join(ws,p),text.replace('to_ascii_lowercase','to_lowercase'));
   requireTrue(cargo(['-p',id,'--test','visible']).status===0,'Unicode-case mutant must pass visible');
   const r=cargo(['-p',id,'--test','holdout']);requireTrue(r.status===101&&r.text.includes('canonical_names_errors_and_state_match_literal_oracle ... FAILED'),'literal Unicode oracle escaped');
  }
  for(const [p,text] of Object.entries(solutions))fs.writeFileSync(path.join(ws,p),text);
  fs.appendFileSync(path.join(ws,'subject/src/lib.rs'),'\n// protected drift\n');let rejected=false;try{assertAuthorized(fixture,ws,descriptor.change_paths);}catch{rejected=true;}requireTrue(rejected,'protected drift escaped authorization control');
  requireTrue(JSON.stringify(snapshot(fixture))===JSON.stringify(before),'repository fixture changed');
  console.log(`${id}: broken-first, complete solution, both partial repairs, API, independent hidden and protected-drift controls passed`);
 }finally{fs.rmSync(scratch,{recursive:true,force:true});}
}
console.log('multifile oracles passed');
