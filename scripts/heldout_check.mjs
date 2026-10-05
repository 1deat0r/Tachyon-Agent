import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {aggregate} from './live_check.mjs';
import {root,ids,protocol,manifestPath,assertFrozen,plannedOrder,sha256,requireTrue} from './heldout_manifest.mjs';
export function checkBaseline(rows,manifest,meta) {
 assertFrozen(manifest);
 requireTrue(meta.schema===1&&meta.manifest_sha256===sha256(JSON.stringify(manifest,null,2)+'\n'),'manifest provenance mismatch');
 requireTrue(JSON.stringify(meta.protocol)===JSON.stringify(protocol),'run protocol mismatch');
 requireTrue(/^[a-f0-9]{40}$/.test(meta.source_commit)&&/^[a-f0-9]{64}$/.test(meta.binary_sha256),'missing source/binary provenance');
 const state=meta.runner_state;
 requireTrue(state?.complete===true&&state.interrupted!==true&&state.in_flight===null&&state.recorded_samples===30,'incomplete runner state');
 const expected=plannedOrder();
 requireTrue(JSON.stringify(meta.planned_order)===JSON.stringify(expected),'run order provenance mismatch');
 requireTrue(rows.every(r=>Number.isFinite(r.model_ms)&&r.model_ms<=protocol.model_stage_deadline_ms+protocol.deadline_tolerance_ms),'model deadline exceeded');
 requireTrue(rows.length===expected.length,'incomplete sample corpus');
 requireTrue(JSON.stringify(rows.map(({fixture,mode,sample})=>({fixture,mode,sample})))===JSON.stringify(expected),'sample order or identity changed');
 requireTrue(rows.every(r=>r.model===protocol.model&&r.provider==='bench-live'),'pinned provider/model changed');
 requireTrue(rows.every(r=>typeof r.task_id==='string'&&r.task_id.length>0)&&new Set(rows.map(r=>r.task_id)).size===rows.length,'task reused or absent');
 const tasks={};
 for(const id of ids) {
  const descriptor=JSON.parse(fs.readFileSync(path.join(root,'fixtures',id,'bench.json'),'utf8'));
  tasks[id]=aggregate(rows.filter(r=>r.fixture===id),protocol.samples_per_cell,descriptor).modes;
 }
 const allCells=Object.values(tasks).flatMap(t=>Object.values(t));
 const verified=allCells.reduce((n,c)=>n+c.verified,0);
 const referenceMet=verified/rows.length>=0.95&&allCells.every(c=>c.verified>=4);
 return {schema:1,generated:new Date().toISOString(),eval_valid:true,scope:'three synthetic held-out bounded tasks, five runs per mode; no prompt changes',source_commit:meta.source_commit,binary_sha256:meta.binary_sha256,manifest_sha256:meta.manifest_sha256,protocol,tasks,verified,total:rows.length,model_calls:allCells.reduce((n,c)=>n+c.model_calls,0),reference_line_met:referenceMet,comparison_allowed:false,cost_usd:null,cost_note:'No verified price supplied; interrupted calls may also have unreconciled billing.',decision:referenceMet?'Exploratory reference line met. Retain failures; use new held-out tasks for later claims.':'Reference line unmet. Retain this baseline; nominate development evals from failures without weakening oracles.'};
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
 try {
  const rows=fs.readFileSync(process.argv[2]??path.join(root,'docs/milestones/HELDOUT_BASELINE_SAMPLES.jsonl'),'utf8').trim().split('\n').map(JSON.parse);
  const manifest=JSON.parse(fs.readFileSync(manifestPath,'utf8'));
  const meta=JSON.parse(fs.readFileSync(path.join(root,'docs/milestones/HELDOUT_BASELINE_META.json'),'utf8'));
  const result=checkBaseline(rows,manifest,meta);
  fs.writeFileSync(path.join(root,'docs/milestones/HELDOUT_BASELINE_MATRIX.json'),JSON.stringify(result,null,2)+'\n');
  console.log(`heldout baseline valid: ${result.verified}/${result.total} verified; reference line ${result.reference_line_met?'met':'unmet'}`);
 }catch(e){console.error(e.message);process.exitCode=1;}
}
