import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import {requireTrue} from './heldout_manifest.mjs';
// Retain authorized source artifacts, never a raw provider response.
export function archiveFailure(row,cell,descriptor,destinationRoot){
 requireTrue(path.dirname(row.scratch)===os.tmpdir()&&path.basename(row.scratch).startsWith(`tachyon-m14-${cell.fixture}-`),'unexpected failure workspace');
 requireTrue(typeof row.task_id==='string'&&/^[a-zA-Z0-9-]+$/.test(row.task_id),'unsafe failure task identity');
 const ws=path.join(row.scratch,'ws');
 for(const p of [row.scratch,ws]){const st=fs.lstatSync(p);requireTrue(st.isDirectory()&&!st.isSymbolicLink(),'linked failure workspace');}
 const canonicalRoot=fs.realpathSync(ws),destination=path.join(destinationRoot,row.task_id);
 for(const rel of descriptor.change_paths){
  requireTrue(!path.isAbsolute(rel)&&rel.length>0&&!rel.split('/').some(p=>p==='..'||p==='.'||p===''),'unsafe declared failure path');
  let ancestor=ws;
  for(const part of rel.split('/').slice(0,-1)){ancestor=path.join(ancestor,part);const st=fs.lstatSync(ancestor);requireTrue(st.isDirectory()&&!st.isSymbolicLink(),'linked failure source ancestor');}
  const source=path.join(ws,rel),canonical=fs.realpathSync(source);
  requireTrue(canonical.startsWith(canonicalRoot+path.sep),'failure source escaped workspace');
  const st=fs.lstatSync(source);requireTrue(st.isFile()&&!st.isSymbolicLink(),'failure source is not regular');
  const fd=fs.openSync(canonical,fs.constants.O_RDONLY|(fs.constants.O_NOFOLLOW??0));
  try{
   const opened=fs.fstatSync(fd);requireTrue(opened.isFile()&&opened.dev===st.dev&&opened.ino===st.ino,'failure source identity changed');
   fs.mkdirSync(path.dirname(path.join(destination,rel)),{recursive:true});fs.writeFileSync(path.join(destination,rel),fs.readFileSync(fd));
  }finally{fs.closeSync(fd);}
 }
}
