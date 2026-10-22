'use strict';
// Synthetic regression for the SIM execution preset "live" (generated 46-value example, not calibrated) and backtest warm-up/seed. NOT performance evidence.
const test=require('node:test'),assert=require('node:assert/strict');
const {DatabaseSync}=require('node:sqlite');
const {StrategySuite}=require('../../apps/papertrade-shadow/suite.cjs');
const {buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs');
const {validateDefinition}=require('../../apps/papertrade-shadow/strategy-registry.cjs');
const {Replay}=require('../../apps/strategy-lab/replay.cjs');
const START=Date.parse('2026-10-20T08:20:00Z');
let sequence=0;
function make(){const db=new DatabaseSync(':memory:');const suite=new StrategySuite({history:{db,transaction:fn=>fn()},base:buildProfile(),guard:()=>({status:'ALLOW_SHADOW',reason_codes:[]}),storageOk:()=>true,now:START});return{db,suite};}
function market(symbol,venue,at,bid,ask){const payload={bid:String(bid),ask:String(ask),frameKind:'quote',instrumentId:0,priceScale:'10',sourceTimestampKnown:true};return{schemaVersion:1,sequence:++sequence,kind:'market',symbol,venue,recv_ms:at,source_ms:at,payload,payload_sha256:require('node:crypto').createHash('sha256').update(JSON.stringify(payload)).digest('hex'),provenance:'synthetic'};}
function add(x,preset='live'){return x.suite.addStrategy({requestId:'live-preset-'+(++sequence)+'-aaaaaaaaaaaa',template:'btc-main',label:'live preset test',timing:'observe',signals:'both',execution:{preset}},START);}
function ready(m,px=81000){const e=m.engine;Object.assign(e.scenario,{entryMs:[4000],cancelMs:[10]});e.integrityWarmUntil=START;e.inputHealthy=true;for(const v of['hl','bf'])m.bridge.market(market('BTC',v,START,px,px+.1));}
const q={strategy:'book',side:'L',asset:'BTC',margin:2100,ctx:{sig:.8,dev:0,spread:0,baseline:0,books:[]}};
test('live preset: 20 s entry validity, assumed cancel success, synthetic exit pool for holds <30 s, Vlad buckets beyond',()=>{const x=make();try{
  const r=add(x),m=x.suite.models.find(v=>v.id===r.model.id),e=m.engine;
  assert.equal(r.model.execution.preset,'live');assert.equal(e.config.entryDeadline.signalTtlMs,20000);
  assert.equal(e.scenario.cancelMinLeadMs,1500);assert.equal(e.scenario.cancelSuccessProbability,0.13);
  assert.equal(e.scenario.exitBuckets[0].values.length,46);assert.equal(e.scenario.exitBuckets[2].values,undefined);
  const vladId=add(x,'vlad').model.id,vlad=x.suite.models.find(v=>v.id===vladId).engine;assert.equal(vlad.scenario.cancelSuccessProbability,undefined);
}finally{x.db.close();}});
test('live preset cancel: lead is necessary, success probability decides; short lead always conflicts',()=>{const x=make();try{
  const id=add(x).model.id,m=x.suite.models.find(v=>v.id===id),e=m.engine;ready(m);
  e.scenario.cancelSuccessProbability=0;
  const a=e.submit({...q});assert.ok(a);m.advance(START+1000,false);e.cancel(a,'signal gone');m.advance(START+1100,false);
  assert.equal(e.counts.cancelConflict,1,'enough lead but probability 0 -> conflict');
  m.bridge.market(market('BTC','hl',START+3900,81000,81000.1));m.advance(START+4200,false);assert.equal(e.positions.size,1);
}finally{x.db.close();}
  const y=make();try{const id=add(y).model.id,m=y.suite.models.find(v=>v.id===id),e=m.engine;ready(m);e.scenario.cancelSuccessProbability=1;
    const a=e.submit({...q});m.advance(START+1000,false);e.cancel(a,'signal gone');m.advance(START+1100,false);assert.equal(e.intents.get(a).state,'cancelled');
    const b=e.submit({...q});m.advance(START+3900,false);e.cancel(b,'signal gone');m.advance(START+4200,false);assert.equal(e.counts.cancelConflict,1,'lead < 1500 ms conflicts even with probability 1');
  }finally{y.db.close();}
});
test('registry accepts only vlad and live presets',()=>{
  const base={requestId:'live-preset-registry-aaaaaaaaaaaa',template:'btc-y2',label:'x',timing:'observe',signals:'both'};
  assert.equal(validateDefinition({...base,execution:{preset:'live'}}).execution.preset,'live');
  assert.throws(()=>validateDefinition({...base,execution:{preset:'other'}}),/INVALID_EXECUTION_PRESET/);
});
test('backtest warm-up blocks entries until the window and seeds Y2 trade memory from the live journal',()=>{
  const seed=[{opened_at:START-3600000,closed_at:START-3590000,side:'S',entry:81000,exit:80990,qty:0.0247,margin:20,leverage:100,reason:'test',gross_pnl:0.25,est_pnl:0.09,status:'closed',strategy:'spread',live:1,sig:5}];
  const r=new Replay({start:START,end:START+3600000,split:START+1800000,profiles:[{id:'y2_live_seed',asset:'BTC',family:'y2',timing:'observe',label:'seed',execution:{preset:'live'}}],warmupMs:2400000,seedRows:seed});
  try{
    assert.equal(r.start,START-2400000);assert.equal(r.windowStart,START);
    const m=r.models[0];assert.ok(m.engine.integrityWarmUntil>=START);assert.equal(r.seeded[m.id],1);
    assert.equal(r.db.prepare('SELECT COUNT(*) n FROM "'+m.bridge.tableName+'" WHERE live=1').get().n,1);
    assert.equal(m.entryGate().allowed,false);
  }finally{r.close();}
});
