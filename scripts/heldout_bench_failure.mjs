// Exercise the actual benchmark error schema without a live provider call.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {root,requireTrue} from './heldout_manifest.mjs';
const solution=path.join(root,'fixtures/solutions/utf8-boundary/subject/src/implementation.rs');
const original=fs.readFileSync(solution,'utf8');
const mutant=original.replace('pub fn label(&self)','pub fn missing_label(&self)');
requireTrue(mutant!==original,'vacuous benchmark failure control');
let scratch;
try {
 fs.writeFileSync(solution,mutant);
 const r=spawnSync(path.join(root,'target/release/examples/bench_matrix'),['utf8-boundary','full','1'],{cwd:root,env:{...process.env,TACHYON_BENCH_LIVE:'0',CARGO_BUILD_JOBS:'2'},encoding:'utf8',stdio:['ignore','pipe','ignore'],timeout:180000,maxBuffer:4*1024*1024});
 if(r.error)throw r.error;
 requireTrue(r.status===1&&r.signal===null,'benchmark failure control did not fail normally');
 let row;try{row=JSON.parse(r.stdout.trim());}catch{throw new Error('benchmark failure control produced invalid JSON');}
 scratch=row.scratch;
 requireTrue(row.outcome==='error'&&row.error_code==='verification_failed'&&!row.verified,'bad patch claimed success');
 requireTrue(typeof row.task_id==='string'&&row.task_id.length>0,'failed benchmark row lacks task identity');
 requireTrue(row.failure_durable&&row.recovery==='recovered_failed'&&row.protected_unchanged,'failed recovery or protection control');
 requireTrue(row.model_calls===1&&row.model_attempts[0].error===null,'control must use one valid scripted proposal');
 requireTrue(JSON.stringify(row.observed_changes)===JSON.stringify(['subject/src/implementation.rs']),'control changed protected paths');
 console.log('heldout benchmark failed-row control passed');
}finally {
 requireTrue(fs.readFileSync(solution,'utf8')===mutant,'concurrent solution edit; refusing to overwrite it');
 fs.writeFileSync(solution,original);
 if(scratch&&path.dirname(scratch)===os.tmpdir()&&path.basename(scratch).startsWith('tachyon-m14-utf8-boundary-'))fs.rmSync(scratch,{recursive:true,force:true});
}
