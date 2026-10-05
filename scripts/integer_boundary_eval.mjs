// Deterministic development eval. No provider calls or repository mutation.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {root,filesUnder,sha256,requireTrue} from './heldout_manifest.mjs';

const fixture=path.join(root,'fixtures/duplicate-range');
const relativeSource='subject/src/implementation.rs';
const retainedPath='docs/milestones/HELDOUT_FAILED_DUPLICATE_RANGE_SERIAL_3.rs.txt';
const retained=fs.readFileSync(path.join(root,retainedPath),'utf8');
const solution=fs.readFileSync(path.join(root,'fixtures/solutions/duplicate-range',relativeSource),'utf8');
const oracle=`use sorted_range::equal_range;
#[test]
fn integer_boundaries_match_independent_linear_counts() {
    fn visit(prefix: &mut Vec<i64>, start: usize, alphabet: &[i64], count: &mut usize) {
        for needle in [i64::MIN, i64::MIN + 1, -2, -1, 0, 1, 2, i64::MAX - 1, i64::MAX] {
            let expected_start = prefix.iter().filter(|value| **value < needle).count();
            let expected_count = prefix.iter().filter(|value| **value == needle).count();
            assert_eq!(equal_range(prefix, needle), expected_start..expected_start + expected_count,
                "values={prefix:?}, needle={needle}");
            *count += 1;
        }
        if prefix.len() < 4 {
            for index in start..alphabet.len() {
                prefix.push(alphabet[index]);
                visit(prefix, index, alphabet, count);
                prefix.pop();
            }
        }
    }
    let mut count = 0;
    visit(&mut Vec::new(), 0, &[i64::MIN, -1, 0, 1, i64::MAX], &mut count);
    assert_eq!(count, 1134);
}
`;
const variants=[
 {id:'known-safe',source:solution,pass:true},
 {id:'retained-overflow',source:retained,pass:false},
 {id:'wrapping-add',source:retained.replace('needle + 1','needle.wrapping_add(1)'),pass:false},
 {id:'saturating-add',source:retained.replace('needle + 1','needle.saturating_add(1)'),pass:false},
];
requireTrue(variants.every(v=>v.id==='retained-overflow'||v.source!==retained),'vacuous variant');
const snapshot=()=>Object.fromEntries(filesUnder(fixture).map(p=>[path.relative(fixture,p),sha256(fs.readFileSync(p))]));
const before=snapshot();
const scratch=fs.mkdtempSync(path.join(os.tmpdir(),'tachyon-integer-eval-'));
const ws=path.join(scratch,'ws');
const results=[];
function cargo(profile,args) {
 const command=['test','--offline','--locked',...(profile==='release'?['--release']:[]),...args];
 const r=spawnSync('cargo',command,{cwd:ws,env:{...process.env,CARGO_TARGET_DIR:path.join(scratch,'target'),CARGO_BUILD_JOBS:'2'},encoding:'utf8',timeout:180000,maxBuffer:4*1024*1024});
 if(r.error)throw r.error;
 requireTrue(r.signal===null&&[0,101].includes(r.status),'cargo setup, signal, or unexpected exit');
 return {status:r.status,text:r.stdout+r.stderr};
}
try {
 fs.cpSync(fixture,ws,{recursive:true,filter:p=>path.basename(p)!=='target'});
 fs.writeFileSync(path.join(ws,'subject/tests/development_boundary.rs'),oracle);
 for(const variant of variants) {
  fs.writeFileSync(path.join(ws,relativeSource),variant.source);
  for(const profile of ['debug','release']) {
   requireTrue(cargo(profile,['--no-run']).status===0,`${variant.id}/${profile}: must compile`);
   requireTrue(cargo(profile,['-p','sorted-range','--test','visible']).status===0,`${variant.id}/${profile}: visible regression failed`);
   const boundary=cargo(profile,['-p','sorted-range','--test','development_boundary']);
   let diagnostic='passed';
   if(variant.pass) {
    requireTrue(boundary.status===0,`${variant.id}/${profile}: safe solution fails boundary oracle`);
    requireTrue(cargo(profile,[]).status===0,`${variant.id}/${profile}: safe solution fails workspace/API checks`);
   }else {
    diagnostic=variant.id==='retained-overflow'&&profile==='debug'?'attempt to add with overflow':'assertion `left == right` failed';
    requireTrue(boundary.status===101&&boundary.text.includes('integer_boundaries_match_independent_linear_counts ... FAILED')&&boundary.text.includes(diagnostic),`${variant.id}/${profile}: boundary control escaped or failed for wrong reason`);
   }
   results.push({variant:variant.id,profile,source_sha256:sha256(variant.source),expected_pass:variant.pass,exit:boundary.status,diagnostic,control_passed:true});
   console.log(`${variant.id}/${profile}: ${variant.pass?'verified':'rejected'} by boundary oracle`);
  }
 }
 requireTrue(JSON.stringify(snapshot())===JSON.stringify(before),'repository fixture changed during eval');
 const report={schema:1,scope:'deterministic development oracle controls; no model-quality score',model_calls:0,fixture:'duplicate-range',sorted_sequences:126,queries_per_sequence:9,cases_per_profile:1134,retained_source:retainedPath,retained_sha256:sha256(retained),oracle_sha256:sha256(oracle),results};
 const output=path.join(root,'target/integer-boundary/results.json');fs.mkdirSync(path.dirname(output),{recursive:true});fs.writeFileSync(output,JSON.stringify(report,null,2)+'\n');
 console.log('integer boundary development eval passed');
}finally {fs.rmSync(scratch,{recursive:true,force:true});}
