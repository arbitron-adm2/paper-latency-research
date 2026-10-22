'use strict';
const fs=require('node:fs'),path=require('node:path');
const ROOT=path.resolve(__dirname,'../..');
const DEFAULT_ROOTS=[path.join(ROOT,'reports'),path.resolve(ROOT,'../_tmp/paper-shadow-acceptance-20261020')];
function readJSON(file,roots,max=2*1024*1024){
 const p=path.resolve(file);
 if(path.extname(p)!=='.json'||p.includes(path.sep+'.runtime'+path.sep)||fs.lstatSync(p).isSymbolicLink())throw new Error('UNSAFE_STATUS_PATH');
 const real=fs.realpathSync(p);
 if(!roots.some(root=>real.startsWith(path.resolve(root)+path.sep))||fs.statSync(real).size>max)throw new Error('UNSAFE_STATUS_PATH');
 return JSON.parse(fs.readFileSync(real,'utf8'));
}
function unavailable(kind,reason='NOT_STARTED'){return{kind,status:reason,available:false,active:false,livePermission:false};}
function normalise(kind,raw,now){
 const mutations=kind==='shadow'?raw.financialMutations:raw.financial_mutations;
 if(raw.schemaVersion!==1||raw.mode!=='READ_ONLY_RESEARCH_NO_ORDERS'||mutations!==0)throw new Error('INVALID_RESEARCH_STATUS');
 if(raw.live_permission===true||(raw.realSignatures!==undefined&&raw.realSignatures!==0))throw new Error('CONTRADICTORY_RESEARCH_PERMISSION');
 const at=kind==='shadow'?raw.generatedAtMs:raw.updated_at_ms;
 if(!Number.isFinite(at)||at>now+1000)throw new Error('INVALID_RESEARCH_CLOCK');
 const complete=kind==='shadow'?['COMPLETE','ERROR','SMOKE_ONLY'].includes(raw.status):raw.runner?.final===true;
 const ageMs=Math.max(0,now-at);
 const stopped=raw.status==='STOPPED'||raw.observerServiceActive===false;
 const base={kind,available:true,runId:raw.runId,status:raw.status,ageMs,atMs:at,active:!complete&&!stopped&&ageMs<=3000,completed:complete,stale:!complete&&!stopped&&ageMs>3000,livePermission:false};
 if(kind==='guard')return{...base,timingStatus:raw.timing_status||raw.status,reasons:(raw.reason_codes||[]).slice(0,16),counts:raw.counts,
  entryEstimateMs:raw.heuristic_timing?.entry_ms??null,exitEstimateMs:raw.heuristic_timing?.exit_ms??null,
  estimateClass:'HEURISTIC_NOT_PERSONAL_LATENCY',economics:'NOT_MODELLED',calibration:raw.measured_catalog?.calibration?.status||'INCONCLUSIVE',quarantined:!!raw.continuity?.sticky_quarantine};
 const btc=raw.assets?.BTC,eth=raw.assets?.ETH;
 return{...base,smokeOnly:!!raw.smokeOnly,warmupComplete:!!raw.acceptance?.completeWarmup,input:raw.input?.continuity||null,lastSequence:raw.input?.lastSequence??null,
  btcInputCounts:btc?.inputCounts||null,signalCounts:btc?.signalAndGateCounts||null,simulated:btc?.simulated||null,
  eth:{mode:eth?.mode||'observe-only',readiness:eth?.readiness||'UNCONFIGURED',counts:eth?.counts||null},
  queueModelIdentified:false,cashPayoutVerified:false,performanceVerdict:'INCONCLUSIVE',failure:raw.failure||null};
}
function readResearchStatus(now=Date.now(),options={}){
 const roots=options.roots||DEFAULT_ROOTS,manifest=options.manifest||path.join(ROOT,'reports/research-live-status.json');
 let config;try{config=readJSON(manifest,roots,32768);if(config.schemaVersion!==1)throw new Error('INVALID_MANIFEST');}catch{return{mode:'READ_ONLY_RESEARCH_NO_ORDERS',shadow:unavailable('shadow'),guard:unavailable('guard'),livePermission:false};}
 const result={mode:'READ_ONLY_RESEARCH_NO_ORDERS',livePermission:false};
 let currentShadow;
 try{currentShadow=readJSON(config.shadow.file,roots);result.shadow=normalise('shadow',currentShadow,now);}catch{result.shadow=unavailable('shadow','NOT_AVAILABLE_OR_INVALID');}
 try{
  if(currentShadow?.service?.persistent&&currentShadow.timingGuard){
   const active=['RUNNING','STARTING','RESUMING'].includes(currentShadow.status);
   result.guard={...normalise('guard',{...currentShadow.timingGuard,status:active?'RUNNING':'STOPPED',timing_status:currentShadow.timingGuard.timing_status??currentShadow.timingGuard.status,updated_at_ms:currentShadow.generatedAtMs,observerServiceActive:active},now),source:'RUNNING_SHADOW_EMBEDDED_GUARD',parentRunId:currentShadow.runId};
  }else result.guard=normalise('guard',readJSON(config.guard.file,roots),now);
 }catch{result.guard=unavailable('guard','NOT_AVAILABLE_OR_INVALID');}
 if(currentShadow?.comparison)result.comparison=currentShadow.comparison;
 return result;
}
module.exports={readResearchStatus,normalise,readJSON};
