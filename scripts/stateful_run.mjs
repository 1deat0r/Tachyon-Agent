import fs from 'node:fs';
import path from 'node:path';
import {archiveFailure} from './multifile_archive.mjs';
import {spawn,execFileSync} from 'node:child_process';
import {root,protocol,manifestPath,assertFrozen,plannedOrder,sha256,requireTrue} from './stateful_manifest.mjs';
const output=path.resolve(process.argv[2]??path.join(root,'target/stateful/raw.jsonl'));
const statePath=output.replace(/\.jsonl$/,'')+'.state.json';
const metaPath=output.replace(/\.jsonl$/,'')+'.meta.json';
let child;
let interrupted=false;
const state={complete:false,recorded_samples:0,in_flight:null};
function save(){fs.writeFileSync(statePath+'.tmp',JSON.stringify(state,null,2)+'\n');fs.renameSync(statePath+'.tmp',statePath);}
for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>{interrupted=true;state.interrupted=true;child?.kill(signal);save();process.exitCode=1;});
try {
 requireTrue(!fs.existsSync(output)&&!fs.existsSync(statePath)&&!fs.existsSync(metaPath),'refusing to overwrite batch');
 const manifestText=fs.readFileSync(manifestPath,'utf8'),manifest=JSON.parse(manifestText);assertFrozen(manifest);
 const binary=path.join(root,'target/release/examples/bench_matrix'),binaryHash=sha256(fs.readFileSync(binary));
 const commit=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim();
 for(const file of [...Object.keys(manifest.files),'docs/milestones/STATEFUL_MANIFEST.json']) {
  const committed=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,maxBuffer:4*1024*1024,stdio:['ignore','pipe','ignore']});
  requireTrue(sha256(committed)===sha256(fs.readFileSync(path.join(root,file))),'uncommitted frozen input');
 }
 fs.mkdirSync(path.dirname(output),{recursive:true});save();
 fs.writeFileSync(metaPath,JSON.stringify({schema:1,source_commit:commit,binary_sha256:binaryHash,manifest_sha256:sha256(manifestText),protocol,planned_order:plannedOrder()},null,2)+'\n',{flag:'wx'});
 const fd=fs.openSync(output,'wx');
 try {
  for(const cell of plannedOrder()) {
   requireTrue(!interrupted,'batch interrupted');
   assertFrozen(manifest);requireTrue(sha256(fs.readFileSync(binary))===binaryHash,'binary changed during run');
   state.in_flight={...cell,started_at:new Date().toISOString()};save();
   const command='set -a; . "$1"; set +a; export TACHYON_BENCH_LIVE=1; export TACHYON_BENCH_VARIANT=baseline; export TACHYON_LIVE_BASE_URL=https://api.xiaomimimo.com; export TACHYON_LIVE_MODEL=mimo-v2.6-flash; exec "$2" "$3" "$4" "$5"';
   const result=await new Promise((resolve,reject)=>{
    child=spawn('bash',['-c',command,'heldout',path.join(process.env.HOME,'.config/tachyon/env'),binary,cell.fixture,cell.mode,String(cell.sample)],{cwd:root,stdio:['ignore','pipe','ignore']});
    let stdout='';const timer=setTimeout(()=>child.kill('SIGKILL'),310000);
    child.stdout.on('data',data=>{stdout+=data; if(stdout.length>4*1024*1024)child.kill('SIGKILL');});
    child.on('error',e=>{clearTimeout(timer);reject(e);});child.on('close',(code,signal)=>{clearTimeout(timer);resolve({stdout,code,signal});});
   });
   requireTrue(result.signal===null&&[0,1].includes(result.code),'child interrupted or setup failed');
   let row;try{row=JSON.parse(result.stdout.trim());}catch{throw new Error('invalid JSON benchmark sample');}requireTrue(Array.isArray(row.model_attempts)&&row.proposal_variant==='baseline','incomplete or nonbaseline sample');const scratch=row.scratch;
   delete row.scratch;
   fs.writeSync(fd,JSON.stringify(row)+'\n');fs.fsyncSync(fd);
   state.recorded_samples++;state.in_flight=null;save();console.log(`${cell.fixture}/${cell.mode}/${cell.sample}: ${row.outcome}; calls=${row.model_calls}`);
   // Accounting is durable before optional artifact retention can fail.
   if(row.outcome==='error'&&scratch){
    const descriptor=JSON.parse(fs.readFileSync(path.join(root,'fixtures',cell.fixture,'bench.json'),'utf8'));
    archiveFailure({...row,scratch},cell,descriptor,path.join(root,'target/stateful/failures'));
   }
  }
  requireTrue(!interrupted,'batch interrupted');state.complete=true;save();
 }finally{fs.closeSync(fd);}
}catch(e){console.error(e.message);process.exitCode=1;}
