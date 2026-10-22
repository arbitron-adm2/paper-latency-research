'use strict';
// Synthetic regression for the SIM execution preset "vlad" (Vlad paper-mode execution). NOT performance evidence.
const test=require('node:test'),assert=require('node:assert/strict');
const {DatabaseSync}=require('node:sqlite');
const {StrategySuite}=require('../../apps/papertrade-shadow/suite.cjs');
const {buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs');
const {validateDefinition}=require('../../apps/papertrade-shadow/strategy-registry.cjs');
const START=Date.parse('2026-10-20T08:20:00Z');
let sequence=0;
function make(guard={status:'SUPPRESSED',reason_codes:['BLOCK_COVERAGE_GAP']}){const db=new DatabaseSync(':memory:');const suite=new StrategySuite({history:{db,transaction:fn=>fn()},base:buildProfile(),guard:()=>guard,storageOk:()=>true,now:START});return{db,suite};}
function market(symbol,venue,at,bid,ask){const payload={bid:String(bid),ask:String(ask),frameKind:'quote',instrumentId:symbol==='BTC'?0:1,priceScale:symbol==='BTC'?'10':'100',sourceTimestampKnown:true};return{schemaVersion:1,sequence:++sequence,kind:'market',symbol,venue,recv_ms:at,source_ms:at,payload,payload_sha256:require('node:crypto').createHash('sha256').update(JSON.stringify(payload)).digest('hex'),provenance:'synthetic'};}
function add(x,extra={}){return x.suite.addStrategy({requestId:'preset-test-'+(++sequence)+'-aaaaaaaaaaaa',template:'btc-main',label:'preset test',timing:'observe',signals:'both',execution:{preset:'vlad'},...extra},START);}
function ready(m,px=81000){const e=m.engine;Object.assign(e.scenario,{entryMs:[4000],cancelMs:[10]});e.integrityWarmUntil=START;e.inputHealthy=true;for(const v of['hl','bf'])m.bridge.market(market('BTC',v,START,px,px+.1));}
const q={strategy:'book',side:'L',asset:'BTC',margin:2100,ctx:{sig:.8,dev:0,spread:0,baseline:0,books:[]}};
test('preset replaces 5 s validity, latency pool, cancel rule and exit buckets only for its own model',()=>{const x=make();try{
  const r=add(x),m=x.suite.models.find(v=>v.id===r.model.id),e=m.engine;
  assert.equal(e.config.entryDeadline.signalTtlMs,3600000);assert.equal(e.scenario.cancelMinLeadMs,1500);assert.equal(e.scenario.congestion,0);
  assert.ok(e.scenario.entryMs.length>=8);assert.equal(e.scenario.exitBuckets.at(-1).maxHoldMs,null);
  const control=x.suite.models.find(v=>v.id==='main_btc_observe').engine;
  assert.equal(control.config.entryDeadline.signalTtlMs,5000);assert.equal(control.scenario.cancelMinLeadMs,undefined);assert.equal(control.scenario.exitBuckets,undefined);
  ready(m);assert.equal(m.entryGate().allowed,true,'R3 is not applied to the Vlad preset');
  assert.equal(x.suite.models.find(v=>v.id==='main_btc_observe').entryGate().allowed,false,'control keeps R3 coverage block');
}finally{x.db.close();}});
test('cancel succeeds with >=1.5 s lead; failed cancel fills at fresh execution-time HL price, with no stale fallback',()=>{const x=make();try{
  const id=add(x).model.id,m=x.suite.models.find(v=>v.id===id),e=m.engine;ready(m);
  const a=e.submit({...q});assert.ok(a);m.advance(START+1000,false);e.cancel(a,'signal gone');m.advance(START+1100,false);
  assert.equal(e.intents.get(a).state,'cancelled');assert.equal(e.counts.cancelled,1);
  const b=e.submit({...q});assert.ok(b);m.advance(START+4200,false);e.cancel(b,'signal gone');m.bridge.market(market('BTC','hl',START+5000,81020,81020.1));m.advance(START+5200,false);
  assert.equal(e.counts.cancelConflict,1);assert.equal(e.intents.get(b).state,'executed');assert.equal(e.counts.lateFill,1);assert.equal(e.positions.size,1);
  assert.equal([...e.positions.values()][0].entry,810201n,'late fill uses current price after the move');
  e.now=START+20000;e.inputHealthy=false;assert.equal(e.quote(),null);assert.equal(e.fallbackQuote(),null,'stale fallback disabled in all presets');
}finally{x.db.close();}});
test('exit delay follows hold-time buckets; non-preset path unchanged',()=>{const x=make();try{
  const id=add(x).model.id,m=x.suite.models.find(v=>v.id===id),e=m.engine;e.now=START+300000;
  e.scenario.exitBuckets=[{maxHoldMs:10000,medianMs:9900},{maxHoldMs:null,medianMs:1400}];
  assert.equal(e.exitDelay({openedAt:START+295000}),9900);assert.equal(e.exitDelay({openedAt:START}),1400);
}finally{x.db.close();}});
test('registry and source checks refuse unknown presets, fixed rails and keys absent from the source bot',()=>{const x=make();try{
  const base={requestId:'preset-registry-test-aaaaaaa',template:'btc-main',label:'x',timing:'observe',signals:'both'};
  assert.throws(()=>validateDefinition({...base,execution:{preset:'fast'}}),/EXECUTION_PRESET/);
  assert.throws(()=>validateDefinition({...base,execution:{preset:'vlad',entryTtlMs:1}}),/EXECUTION_PRESET/);
  assert.throws(()=>validateDefinition({...base,parameters:{margin:1}}),/PARAMETER/);
  assert.throws(()=>validateDefinition({...base,parameters:{maxFeedAgeMs:9999}}),/PARAMETER/);
  assert.throws(()=>add(x,{parameters:{notARealSourceKey:1}}),/PARAMETER_NOT_IN_SOURCE/);
  assert.equal(validateDefinition({...base,parameters:{stopUsd:4,cushBoth:false}}).parameters.stopUsd,4);
}finally{x.db.close();}});
test('feed gap with exposure keeps both preset and control models running (non-stop SIM)',()=>{const x=make();try{
  const id=add(x).model.id,m=x.suite.models.find(v=>v.id===id),e=m.engine;ready(m);assert.ok(e.submit({...q}));
  const c=x.suite.models.find(v=>v.id==='main_btc_observe');Object.assign(c.engine.scenario,{entryMs:[4000],cancelMs:[10],congestion:0,volatility:0,cancelConflictProbability:0});c.engine.integrityWarmUntil=START;c.engine.inputHealthy=true;for(const v of['hl','bf'])c.bridge.market(market('BTC',v,START,81000,81000.1));x.setGuard?.({status:'ALLOW_SHADOW',reason_codes:[]});
  c.engine.submit({...q});
  x.suite.invalidate(START+500,'history_gap');
  assert.equal(m.status==='QUARANTINED',false);assert.equal(m.gaps,1);assert.ok(e.ledger.some(r=>r.kind==='input_gap_continued'));
  assert.notEqual(c.status,'QUARANTINED');assert.equal(c.gaps,1);assert.ok(!c.engine.accountingQuarantined);
  m.advance(START+4100,false);assert.equal(e.counts.filled,0,'gap cannot invent a stale-price fill');assert.equal(e.reserved,0n,'non-stop: entry without execution price releases the reserve');assert.ok(e.ledger.some(r=>r.kind==='entry_price_unavailable_released'));
}finally{x.db.close();}});
test('continuous preset: restart with exposure is continued, not frozen',()=>{const x=make();try{
  const id=add(x).model.id,m=x.suite.models.find(v=>v.id===id),e=m.engine;ready(m);assert.ok(e.submit({...q}));m.bridge.market(market('BTC','hl',START+3900,81000,81000.1));m.advance(START+4100,false);assert.equal(e.positions.size,1);m.save(START+4200);
  const again=new StrategySuite({history:{db:x.db,transaction:fn=>fn()},base:buildProfile(),guard:()=>({status:'ALLOW_SHADOW',reason_codes:[]}),storageOk:()=>true,now:START+9000});
  const r=again.models.find(v=>v.id===id);assert.notEqual(r.status,'QUARANTINED');assert.equal(!!r.frozen,false);assert.equal(r.engine.positions.size,1);
  assert.ok(r.engine.ledger.some(v=>v.kind==='restart_with_exposure_continued'));
}finally{x.db.close();}});
test('archived models keep saved state but stop consuming the feed and free a model slot',()=>{const x=make();try{
  x.db.prepare("INSERT INTO sim_strategy_archived VALUES('main_btc_strict',?,'cpu budget')").run(START);
  const again=new StrategySuite({history:{db:x.db,transaction:fn=>fn()},base:buildProfile(),guard:()=>({status:'ALLOW_SHADOW',reason_codes:[]}),storageOk:()=>true,now:START+1000});
  const m=again.models.find(v=>v.id==='main_btc_strict');assert.equal(m.status,'ARCHIVED');assert.equal(m.summary().archived,true);
  const before=m.marketEvents;again.ingest(market('BTC','hl',START+1100,81000,81000.1),START+1100,true);assert.equal(m.marketEvents,before);
}finally{x.db.close();}});
