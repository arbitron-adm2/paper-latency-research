'use strict';
const C=require('./state-codec.cjs');
const FIELDS=['now','seq','ledgerSequence','balance','reserved','locked','realised','fees','funding','bestEquity','drawdown','exposureMs','capitalMs','missingDepthTicks','samples','counts','tickStats','lastCandidate','lastPrice','lastEntryPolicyReason','assetCoverage','inputHealthy','integrityWarmUntil','accountingQuarantined','entryBlockReasons','suspendedTimers','priceEpoch'];
function captureEngine(e){
  const out={schemaVersion:1,rngState:e.rng.state,intents:new Map(),positions:e.positions,timers:e.timers,orders:e.orders};
  for(const k of FIELDS)out[k]=e[k];
  const values=[...e.intents.entries()],recent=new Set(values.slice(-100).map(([id])=>id));
  for(const [id,r] of values)if(recent.has(id)||['submitted','queued','outcome_unknown'].includes(r.state))out.intents.set(id,r);
  return C.clone(out);
}
function restoreEngine(e,s,{preserveClock=false}={}){
  if(!s||s.schemaVersion!==1||!['balance','reserved','locked','realised','fees','funding'].every(k=>typeof s[k]==='bigint')||s.reserved<0n||s.locked<0n||!(s.intents instanceof Map)||!(s.positions instanceof Map)||!Number.isInteger(s.rngState))throw new Error('INVALID_FINANCIAL_CHECKPOINT');
  const t=e.now,seq=e.ledgerSequence??0,x=C.clone(s);
  for(const k of FIELDS)if(Object.hasOwn(x,k))e[k]=x[k];
  if(preserveClock){e.now=t;e.ledgerSequence=Math.max(seq,e.ledgerSequence??0);}
  e.rng.state=x.rngState;e.intents=x.intents;e.positions=x.positions;e.timers=x.timers;e.orders=x.orders;
  for(const [id,r] of e.intents){if(r.kind==='entry'){r.q.intentId=id;if(r.entryLimit)Object.defineProperty(r,'entryLimit',{value:Object.freeze(r.entryLimit),enumerable:true,writable:false,configurable:false});}}
}
function hasRisk(e){return e.reserved>0n||e.locked>0n||e.positions.size>0||[...e.intents.values()].some(r=>['submitted','queued','outcome_unknown'].includes(r.state));}
function censor(e,reason){
  let required=0n;
  for(const r of e.intents.values())if(['submitted','queued','outcome_unknown'].includes(r.state)){
    r.state='outcome_unknown';r.censoredReason=reason;if(r.kind==='entry')required+=r.margin;
  }
  if(required>e.reserved)e.reserved=required; // conservative write-ahead submit, never a free release
  for(const p of e.positions.values()){p.censored=true;p.censoredReason=reason;}
  e.suspendedTimers=[...(e.suspendedTimers??[]),...e.timers];e.timers=[];
  e.accountingQuarantined=true;e.entryBlockReasons=[...new Set([...(e.entryBlockReasons??[]),reason])];
}
function resumeCensored(e,now){
  // Non-stop SIM: undo censor() instead of freezing. Suspended timers fire at the first fresh quote;
  // entries without a pending timer are released; exits are requeued; reserved/locked are rebuilt from live records.
  const out={timers:(e.suspendedTimers??[]).length,entriesReleased:0,entriesRequeued:0,exitsRequeued:0,positions:0};
  for(const t of e.suspendedTimers??[])e.timers.push(t);e.suspendedTimers=[];
  for(const r of e.intents.values()){
    if(!['submitted','queued','outcome_unknown'].includes(r.state))continue;
    if(r.executionPriceMissing&&!e.scenario?.nonstop){r.state='outcome_unknown';out.unresolvedPricesRetained=(out.unresolvedPricesRetained??0)+1;continue;}
    // Non-stop: a missing execution-time price never holds the slot. Entry -> released below; exit -> requeued and fills at the first later fresh quote, wait counted from the original miss.
    if(r.executionPriceMissing&&r.state==='outcome_unknown'){r.priceMissResumedAt=now;if(r.kind==='exit')r.firstPriceMissAt??=r.executionAtMs??now;out.priceMissingResumed=(out.priceMissingResumed??0)+1;}
    const pending=e.timers.some(t=>t.id===r.id&&t.kind===r.kind);
    if(r.kind==='entry'){
      if(pending){r.state='queued';out.entriesRequeued++;}
      else{r.state='expired';r.releasedBy='NONSTOP_RESUME';e.counts.entryExpired++;try{e.bridge?.api?.terminal?.(r.q);}catch{}out.entriesReleased++;}
    }else if(r.kind==='exit'){
      r.state='queued';if(!pending)e.timers.push({at:now,kind:'exit',id:r.id,sequence:e.seq++});out.exitsRequeued++;
    }
  }
  for(const p of e.positions.values())if(p.censored){p.censored=false;p.resumedAt=now;out.positions++;}
  e.timers.sort((a,b)=>a.at-b.at||a.sequence-b.sequence);
  e.reserved=[...e.intents.values()].filter(r=>r.kind==='entry'&&['submitted','queued','outcome_unknown'].includes(r.state)).reduce((s,r)=>s+r.margin,0n);
  e.locked=[...e.positions.values()].reduce((s,p)=>s+p.margin,0n);
  e.accountingQuarantined=false;e.entryBlockReasons=[];
  return out;
}
function boundRuntime(e){
  // Financial records are already durable; only ephemeral buffers/terminal caches retire.
  e.ledger.length=0;e.orders.length=0;
  for(const xs of Object.values(e.samples))if(xs.length>512)xs.splice(0,xs.length-512);
  if(e.intents.size>512){const keep=new Set([...e.intents.keys()].slice(-100));for(const [id,r] of e.intents)if(!keep.has(id)&&!['submitted','queued','outcome_unknown'].includes(r.state))e.intents.delete(id);}
}
module.exports={captureEngine,restoreEngine,hasRisk,censor,resumeCensored,boundRuntime};
