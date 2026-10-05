import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {aggregate} from './live_check.mjs';
import {root,ids,variants,protocol,manifestPath,assertFrozen,plannedOrder,sha256,requireTrue} from './candidate_manifest.mjs';
export function checkCandidate(rows,m,meta){
 assertFrozen(m);requireTrue(meta.schema===1&&meta.manifest_sha256===sha256(JSON.stringify(m,null,2)+'\n'),'manifest provenance mismatch');
 requireTrue(/^[a-f0-9]{40}$/.test(meta.source_commit)&&/^[a-f0-9]{64}$/.test(meta.binary_sha256),'missing source/binary provenance');
 requireTrue(JSON.stringify(meta.protocol)===JSON.stringify(protocol)&&JSON.stringify(meta.planned_order)===JSON.stringify(plannedOrder()),'protocol/order provenance mismatch');
 const state=meta.runner_state;requireTrue(state?.complete===true&&!state.interrupted&&state.in_flight===null&&state.recorded_samples===30,'incomplete batch');
 requireTrue(rows.length===30&&JSON.stringify(rows.map(({fixture,mode,sample,proposal_variant})=>({fixture,mode,sample,proposal_variant})))===JSON.stringify(plannedOrder()),'wrong row identity/order');
 requireTrue(rows.every(r=>r.provider==='bench-live'&&r.model===protocol.model&&typeof r.task_id==='string'&&r.task_id.length>0&&Number.isFinite(r.model_ms)&&r.model_ms<=121000)&&new Set(rows.map(r=>r.task_id)).size===30,'mixed provider, task or deadline');
 const tasks={};for(const id of ids){tasks[id]={};const descriptor=JSON.parse(fs.readFileSync(path.join(root,'fixtures',id,'bench.json'),'utf8'));for(const variant of variants)tasks[id][variant]=aggregate(rows.filter(r=>r.fixture===id&&r.proposal_variant===variant),5,descriptor,['full']).modes.full;}
 const developmentImproved=tasks['duplicate-range']['boundary-guidance'].verified>tasks['duplicate-range'].baseline.verified;
 const noHeldoutRegression=ids.slice(1).every(id=>tasks[id]['boundary-guidance'].verified>=tasks[id].baseline.verified);
 const candidateTotal=ids.reduce((n,id)=>n+tasks[id]['boundary-guidance'].verified,0);
 const referenceMet=candidateTotal/15>=.95&&ids.every(id=>tasks[id]['boundary-guidance'].verified>=4);
 const eligible=developmentImproved&&noHeldoutRegression&&referenceMet;
 return {schema:1,eval_valid:true,scope:'one development task and two new held-out tasks; five full-mode samples per arm',source_commit:meta.source_commit,binary_sha256:meta.binary_sha256,manifest_sha256:meta.manifest_sha256,tasks,development_improved:developmentImproved,no_heldout_regression:noHeldoutRegression,candidate_reference_met:referenceMet,eligible_for_production_design:eligible,production_adopted:false,comparison_allowed:false,cost_usd:null,model_calls:rows.reduce((n,r)=>n+r.model_calls,0),decision:eligible?'Eligible only for further production context/provenance design; no reliability or speed claim.':'Do not adopt: strict development improvement and held-out nonregression/reference conditions were not all met.'};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const rows=fs.readFileSync(path.join(root,'docs/milestones/CANDIDATE_SAMPLES.jsonl'),'utf8').trim().split('\n').map(JSON.parse),m=JSON.parse(fs.readFileSync(manifestPath,'utf8')),meta=JSON.parse(fs.readFileSync(path.join(root,'docs/milestones/CANDIDATE_META.json'),'utf8'));
 const result=checkCandidate(rows,m,meta);fs.writeFileSync(path.join(root,'docs/milestones/CANDIDATE_MATRIX.json'),JSON.stringify(result,null,2)+'\n');console.log(`candidate experiment valid: ${result.decision}`);
}
