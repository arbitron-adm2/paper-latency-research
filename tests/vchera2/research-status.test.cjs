'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {normalise,readResearchStatus,readJSON}=require('../../apps/vchera2/research-status.cjs');
const g=()=>({schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',financial_mutations:0,live_permission:false,updated_at_ms:10000,runId:'g',status:'ALLOW_SHADOW',reason_codes:[],runner:{final:false},heuristic_timing:{entry_ms:3000,exit_ms:10000},counts:{pressure:3}});
const s=()=>({schemaVersion:1,mode:'READ_ONLY_RESEARCH_NO_ORDERS',financialMutations:0,realSignatures:0,generatedAtMs:10000,runId:'s',status:'RUNNING',input:{lastSequence:42},assets:{BTC:{inputCounts:{book:10},simulated:{net_pnl_scenario:'2.5'}},ETH:{mode:'observe-only',readiness:'UNCONFIGURED'}},acceptance:{completeWarmup:true}});
test('read-only timing pass and simulated pnl never become LIVE permission',()=>{
 const guard=normalise('guard',g(),10000),shadow=normalise('shadow',s(),10000);
 assert.equal(guard.active,true);assert.equal(guard.livePermission,false);assert.equal(guard.estimateClass,'HEURISTIC_NOT_PERSONAL_LATENCY');assert.equal(guard.economics,'NOT_MODELLED');
 assert.equal(shadow.eth.mode,'observe-only');assert.equal(shadow.queueModelIdentified,false);assert.equal(shadow.cashPayoutVerified,false);assert.equal(shadow.livePermission,false);
});
test('completed bounded status is not active and expired running data is stale',()=>{
 const done=g();done.runner.final=true;done.status='COMPLETED';const d=normalise('guard',done,10000);assert.equal(d.active,false);assert.equal(d.completed,true);
 const stale=normalise('shadow',s(),14000);assert.equal(stale.active,false);assert.equal(stale.stale,true);
});
test('inconsistent permission/signatures, future clock or missing explicit zero mutation count rejected',()=>{
 for(const patch of[{live_permission:true},{financial_mutations:1},{updated_at_ms:20000}])assert.throws(()=>normalise('guard',{...g(),...patch},10000));
 assert.throws(()=>normalise('shadow',{...s(),realSignatures:1},10000));assert.throws(()=>normalise('guard',{...g(),financial_mutations:undefined},10000));
});
test('missing manifest is explicitly not started, not a ready guard',()=>{
 const r=readResearchStatus(10000,{manifest:'/does-not-exist/status.json',roots:['/does-not-exist']});assert.equal(r.guard.available,false);assert.equal(r.shadow.active,false);assert.equal(r.livePermission,false);
});
test('status reader is limited to allowed JSON, rejects symlinks and private runtime paths',()=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'research-status-'));
 try{const target=path.join(root,'valid.json');fs.writeFileSync(target,JSON.stringify(g()));assert.equal(readJSON(target,[root]).runId,'g');
 const link=path.join(root,'link.json');fs.symlinkSync(target,link);assert.throws(()=>readJSON(link,[root]));assert.throws(()=>readJSON(target,[path.join(root,'other')]));
 fs.mkdirSync(path.join(root,'.runtime'));const secret=path.join(root,'.runtime','x.json');fs.writeFileSync(secret,'{}');assert.throws(()=>readJSON(secret,[root]));
 }finally{fs.rmSync(root,{recursive:true,force:true});}
});

test('persistent shadow uses its current embedded guard instead of a completed old experiment',()=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'embedded-guard-'));
 try{const shadow=s();shadow.service={persistent:true};shadow.timingGuard={...g(),runId:'current:timing',status:'SUPPRESSED',timing_status:'SUPPRESSED',reason_codes:['ENTRY_DELAY_BUDGET']};
 const old={...g(),runId:'old',runner:{final:true},status:'COMPLETED'};fs.writeFileSync(path.join(root,'shadow.json'),JSON.stringify(shadow));fs.writeFileSync(path.join(root,'guard.json'),JSON.stringify(old));fs.writeFileSync(path.join(root,'manifest.json'),JSON.stringify({schemaVersion:1,shadow:{file:path.join(root,'shadow.json')},guard:{file:path.join(root,'guard.json')}}));
 const r=readResearchStatus(10000,{manifest:path.join(root,'manifest.json'),roots:[root]});assert.equal(r.guard.active,true);assert.equal(r.guard.runId,'current:timing');assert.equal(r.guard.completed,false);assert.equal(r.guard.parentRunId,'s');assert.equal(r.guard.timingStatus,'SUPPRESSED');
 shadow.status='STOPPED';fs.writeFileSync(path.join(root,'shadow.json'),JSON.stringify(shadow));const stopped=readResearchStatus(10000,{manifest:path.join(root,'manifest.json'),roots:[root]});assert.equal(stopped.guard.active,false);assert.equal(stopped.shadow.active,false);assert.equal(stopped.shadow.completed,false);
 }finally{fs.rmSync(root,{recursive:true,force:true});}
});
