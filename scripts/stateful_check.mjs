import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {aggregate} from './live_check.mjs';
import {root,ids,protocol,manifestPath,assertFrozen,plannedOrder,sha256,requireTrue} from './stateful_manifest.mjs';
export function checkStateful(rows,m,meta){
 assertFrozen(m);requireTrue(meta.schema===1&&meta.manifest_sha256===sha256(JSON.stringify(m,null,2)+'\n'),'manifest provenance mismatch');
 requireTrue(/^[a-f0-9]{40}$/.test(meta.source_commit)&&/^[a-f0-9]{64}$/.test(meta.binary_sha256),'missing source/binary provenance');
 requireTrue(JSON.stringify(meta.protocol)===JSON.stringify(protocol)&&JSON.stringify(meta.planned_order)===JSON.stringify(plannedOrder()),'protocol/order mismatch');
 const state=meta.runner_state;requireTrue(state?.complete===true&&!state.interrupted&&state.in_flight===null&&state.recorded_samples===20,'incomplete batch');
 requireTrue(rows.length===20&&JSON.stringify(rows.map(({fixture,mode,sample})=>({fixture,mode,sample})))===JSON.stringify(plannedOrder()),'row order/identity mismatch');
 requireTrue(rows.every(r=>r.proposal_variant==='baseline'&&r.provider==='bench-live'&&r.model===protocol.model&&typeof r.task_id==='string'&&r.task_id.length>0&&Number.isFinite(r.model_ms)&&r.model_ms<=121000)&&new Set(rows.map(r=>r.task_id)).size===20,'variant, model, task or deadline mismatch');
 const tasks={};for(const id of ids){const d=JSON.parse(fs.readFileSync(path.join(root,'fixtures',id,'bench.json'),'utf8'));tasks[id]=aggregate(rows.filter(r=>r.fixture===id),5,d).modes;}
 const cells=Object.values(tasks).flatMap(Object.values),verified=cells.reduce((n,c)=>n+c.verified,0);
 return{schema:1,eval_valid:true,scope:'two new stateful cross-module synthetic tasks, five runs per mode, unchanged production baseline',source_commit:meta.source_commit,binary_sha256:meta.binary_sha256,manifest_sha256:meta.manifest_sha256,tasks,verified,total:20,model_calls:cells.reduce((n,c)=>n+c.model_calls,0),reference_line_met:verified/20>=.95&&cells.every(c=>c.verified>=4),comparison_allowed:false,cost_usd:null,decision:'Retain all failures. Small per-cell samples support no general reliability or speed claim.'};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){const rows=fs.readFileSync(path.join(root,'docs/milestones/STATEFUL_SAMPLES.jsonl'),'utf8').trim().split('\n').map(JSON.parse),m=JSON.parse(fs.readFileSync(manifestPath,'utf8')),meta=JSON.parse(fs.readFileSync(path.join(root,'docs/milestones/STATEFUL_META.json'),'utf8'));const result=checkStateful(rows,m,meta);fs.writeFileSync(path.join(root,'docs/milestones/STATEFUL_MATRIX.json'),JSON.stringify(result,null,2)+'\n');console.log(`stateful baseline valid: ${result.verified}/20 verified; reference line ${result.reference_line_met?'met':'unmet'}`);}
