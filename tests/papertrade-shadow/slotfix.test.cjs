'use strict';
// Regression 21.10: since causal execution prices (11:26 MSK) a missing execution-time HL quote left comparison
// accounts in outcome_unknown forever (ONE_OPEN_OR_PENDING_SLOT_LIMIT); the stuck exposure then tripped the Y2 day
// high-water guard and the model stopped deciding without any record. Synthetic, NOT performance evidence.
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {DatabaseSync}=require('node:sqlite');
const C=require('../../apps/papertrade-shadow/state-codec.cjs');
const {StrategySuite,canFreezeUnresolvedModels,canPreserveLegacyPriceAccounts,canContinueComparisonAccounts}=require('../../apps/papertrade-shadow/suite.cjs');
const {buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs');
const {PersistentObserver}=require('../../apps/papertrade-shadow/persistent-session.cjs');
const {hasRisk}=require('../../apps/papertrade-shadow/engine-state.cjs');
const {ROOT}=require('../../apps/papertrade-shadow/strategy.cjs');
const START=Date.parse('2026-10-21T10:03:00Z'),UNIT=10n**18n;
let sequence=0;
const allow={status:'ALLOW_SHADOW',reason_codes:[]};
function suite(db,now=START){return new StrategySuite({history:{db,transaction:fn=>fn()},base:buildProfile(),guard:()=>allow,storageOk:()=>true,now});}
function market(symbol,venue,at,bid,ask){const payload={bid:String(bid),ask:String(ask),frameKind:'quote',instrumentId:symbol==='BTC'?0:1,priceScale:symbol==='BTC'?'10':'100',sourceTimestampKnown:true};return{schemaVersion:1,sequence:++sequence,kind:'market',symbol,venue,recv_ms:at,source_ms:at,payload,payload_sha256:require('node:crypto').createHash('sha256').update(JSON.stringify(payload)).digest('hex'),provenance:'synthetic'};}
function quote(m,at,px=83000){for(const v of['hl','bf'])m.bridge.market(market('BTC',v,at,px,px+1));}
const q={strategy:'spread',side:'S',asset:'BTC',margin:2100,ctx:{sig:4,dev:4,spread:0,baseline:0,books:[]}};
function ready(m,at=START){const e=m.engine;Object.assign(e.scenario,{entryMs:[2000],exitMs:[100],cancelMs:[10],congestion:0,volatility:0,cancelConflictProbability:0});e.integrityWarmUntil=at;e.inputHealthy=true;return e;}
function open(m,at=START){const e=ready(m,at);quote(m,at);assert.ok(e.submit({...q}));quote(m,at+1900);m.advance(at+5000,false);assert.equal(e.positions.size,1);return [...e.positions.values()][0];} // last quote +1900: stale by the exit time +5100
// Reproduce what the 11:26 code saved: no execution-time quote -> outcome_unknown + executionPriceMissing.
function legacyMiss(m,fn){const e=m.engine;e.scenario.nonstop=false;try{fn();}finally{e.scenario.nonstop=true;}}

test('exit without execution-time quote (stop at 13:03) holds the slot only until the first fresh quote, then frees it',()=>{const db=new DatabaseSync(':memory:');try{
  const m=suite(db).models.find(v=>v.id==='main_btc_observe'),e=m.engine,p=open(m);
  e.close({positionId:p.positionId,closing:{why:'стоп'}});m.advance(START+6000,false);
  const r=e.intents.get('shadow-close-'+p.positionId);
  assert.equal(r.state,'queued');assert.equal(hasRisk(e),true,'position is really still open while it waits');assert.equal(m.entryGate().allowed,false);
  assert.equal(e.ledger.filter(x=>x.kind==='execution_price_unavailable').length,1);assert.match(e.ledger.find(x=>x.kind==='execution_price_unavailable').risk,/^nonstop SIM/);
  quote(m,START+6200,83010);m.advance(START+7000,false);
  assert.equal(e.positions.size,0);assert.equal(e.locked,0n);assert.equal(hasRisk(e),false);assert.equal(m.entryGate().allowed,true);
  const trade=C.parse(db.prepare('SELECT payload_json FROM sim_strategy_trades WHERE model_id=?').get(m.id).payload_json);
  assert.equal(trade.closedAt,START+6600);assert.equal(trade.exitPriceWait.firstMissAtMs,START+5100);assert.equal(trade.exitPriceWait.waitMs,1500);
  assert.equal(m.summary().priceUnresolved,0);
}finally{db.close();}});

test('entry without execution-time quote (13:08) releases the reserve at once; no local expiry keeps the slot',()=>{const db=new DatabaseSync(':memory:');try{
  const m=suite(db).models.find(v=>v.id==='main_btc_observe'),e=ready(m);quote(m,START);const id=e.submit({...q});assert.ok(id);
  m.advance(START+2100,false);
  assert.equal(e.intents.get(id).state,'expired');assert.equal(e.reserved,0n);assert.equal(e.counts.filled,0);assert.equal(e.realised,0n);
  assert.equal(hasRisk(e),false);assert.equal(m.entryGate().allowed,true);
  assert.deepEqual(e.ledger.filter(x=>['execution_price_unavailable','entry_price_unavailable_released'].includes(x.kind)).map(x=>x.kind),['execution_price_unavailable','entry_price_unavailable_released']);
}finally{db.close();}});

test('accounts already stuck by the 11:26 code resume on restart: entry released, exit closes at first fresh quote, wait counted from the original miss',()=>{const db=new DatabaseSync(':memory:');try{
  const s=suite(db),a=s.models.find(v=>v.id==='main_btc_observe'),b=s.models.find(v=>v.id==='main_btc_strict');
  const p=open(a);a.engine.close({positionId:p.positionId,closing:{why:'стоп'}});legacyMiss(a,()=>a.advance(START+6000,false));
  const eb=ready(b);quote(b,START);const id=eb.submit({...q});legacyMiss(b,()=>b.advance(START+2100,false));
  for(const [m,kind] of [[a,'exit'],[b,'entry']]){const r=[...m.engine.intents.values()].find(x=>x.kind===kind&&x.state==='outcome_unknown');assert.ok(r&&r.executionPriceMissing,'legacy stuck '+kind);m.save(START+6000);}
  assert.equal(db.prepare('SELECT COUNT(*) n FROM sim_strategy_accounts WHERE unresolved=1').get().n,2);
  const s2=suite(db,START+60000),a2=s2.models.find(v=>v.id===a.id),b2=s2.models.find(v=>v.id===b.id);
  assert.equal(b2.resumed.priceMissingResumed,1);assert.equal(b2.resumed.entriesReleased,1);assert.equal(b2.engine.reserved,0n);assert.equal(b2.engine.intents.get(id).state,'expired');assert.equal(hasRisk(b2.engine),false);
  assert.equal(a2.resumed.priceMissingResumed,1);assert.equal(a2.resumed.exitsRequeued,1);assert.equal(a2.engine.locked,2100n*UNIT);
  a2.engine.inputHealthy=true;a2.advance(START+60100,false);assert.equal(a2.engine.positions.size,1,'no quote yet: waits, never fills in the past');
  quote(a2,START+60200,83020);a2.advance(START+61000,false);
  assert.equal(a2.engine.positions.size,0);assert.equal(a2.engine.locked,0n);assert.equal(hasRisk(a2.engine),false);
  const fill=a2.engine.ledger.find(x=>x.kind==='close_fill');assert.equal(fill.exitPriceWait.firstMissAtMs,START+5100);assert.equal(fill.exitPriceWait.waitMs,fill.closedAt-(START+5100));assert.ok(fill.closedAt>=START+60200);
  a2.save(START+61000);b2.save(START+61000);assert.equal(db.prepare('SELECT COUNT(*) n FROM sim_strategy_accounts WHERE unresolved=1').get().n,0);
}finally{db.close();}});

test('source LIVE switched off by its own day guard is recorded and shown, also after restart; never silent',()=>{const db=new DatabaseSync(':memory:');try{
  const m=suite(db).models.find(v=>v.id==='y2_timing_observe'),api=m.bridge.api;m.engine.integrityWarmUntil=START;
  m.advance(START+200);assert.equal(m.status,'RUNNING');assert.equal(m.sourceLive,true);
  api.restoreFlatPolicy({cfg:{...api.cfg,live:false},guard:{...api.signalState().guard,tripped:true,dayKey:START}});   // state persisted by the silent models
  m.advance(START+400);
  const off=m.engine.ledger.filter(x=>x.kind==='source_live_disabled');assert.equal(off.length,1);assert.equal(off[0].guardTripped,true);
  assert.equal(m.status,'SOURCE_LIVE_OFF');assert.equal(m.summary().sourceLive,false);assert.equal(m.summary().sourceOff.reason,'SOURCE_DAY_HIGH_WATER_GUARD');
  m.advance(START+600);assert.equal(m.engine.ledger.filter(x=>x.kind==='source_live_disabled').length,1,'one record per transition');
  m.save(START+600);const r=suite(db,START+5000).models.find(v=>v.id===m.id);r.engine.integrityWarmUntil=START+5000;r.advance(START+5200);
  assert.equal(r.status,'SOURCE_LIVE_OFF');assert.equal(db.prepare("SELECT COUNT(*) n FROM sim_strategy_events WHERE model_id=? AND kind='source_live_disabled'").get(m.id).n,2);
  r.bridge.api.cfg.live=true;r.advance(START+5400);assert.equal(r.status,'RUNNING');assert.ok(r.engine.ledger.some(x=>x.kind==='source_live_enabled'));
}finally{db.close();}});

test('flat upgrade gate: unresolved non-stop comparison accounts carry forward when consistent; corrupt or inconsistent ones still block',()=>{const db=new DatabaseSync(':memory:');try{
  const s=suite(db),a=s.models.find(v=>v.id==='main_btc_observe'),p=open(a);a.engine.close({positionId:p.positionId,closing:{why:'стоп'}});legacyMiss(a,()=>a.advance(START+6000,false));a.save(START+6000);
  assert.equal(canFreezeUnresolvedModels(db),false);assert.equal(canPreserveLegacyPriceAccounts(db),false);assert.equal(canContinueComparisonAccounts(db),true);
  const row=db.prepare('SELECT snapshot_json,snapshot_hash FROM sim_strategy_accounts WHERE model_id=?').get(a.id),put=(json,hash)=>db.prepare('UPDATE sim_strategy_accounts SET snapshot_json=?,snapshot_hash=? WHERE model_id=?').run(json,hash,a.id);
  put(row.snapshot_json,'bad');assert.equal(canContinueComparisonAccounts(db),false);
  for(const mutate of [x=>{x.engine.locked=0n;},x=>{x.engine.accountingQuarantined=true;}]){const x=C.parse(row.snapshot_json);mutate(x);const blob=C.stringify(x);put(blob,C.hash(blob));assert.equal(canContinueComparisonAccounts(db),false);}
  put(row.snapshot_json,row.snapshot_hash);assert.equal(canContinueComparisonAccounts(db),true);
}finally{db.close();}});

test('upgrade-flat-profile path succeeds with a stuck comparison account and preserves its capital',()=>{
  const parent=fs.mkdtempSync(path.join(ROOT,'reports/slotfix-upgrade-')),dir=path.join(parent,'state');let o;
  try{
    o=new PersistentObserver({stateDir:dir,clock:()=>START,comparisonEnabled:true});
    const m=o.suite.models.find(v=>v.id==='main_btc_observe');m.engine.entryGate=()=>({allowed:true});const p=open(m);m.engine.close({positionId:p.positionId,closing:{why:'стоп'}});legacyMiss(m,()=>m.advance(START+6000,false));m.save(START+6000);
    const balance=m.engine.balance;o.stop('UNIT_OLD_CLEAN_STOP',START+7000);
    const db=new DatabaseSync(path.join(dir,'shadow-history.sqlite'));
    for(const row of db.prepare('SELECT slot,payload_json FROM sim_checkpoints').all()){const state=C.parse(row.payload_json);state.modelHash='previous-reviewed-model';const blob=C.stringify(state);db.prepare('UPDATE sim_checkpoints SET payload_json=?,sha256=?,model_hash=? WHERE slot=?').run(blob,C.hash(blob),'previous-reviewed-model',row.slot);}
    assert.equal(canFreezeUnresolvedModels(db),false);assert.equal(canPreserveLegacyPriceAccounts(db),false);db.close();
    o=new PersistentObserver({stateDir:dir,clock:()=>START+60000,flatUpgrade:true,comparisonEnabled:true});assert.equal(o.flatUpgradeApplied,true);
    const r=o.suite.models.find(v=>v.id==='main_btc_observe');assert.equal(r.engine.balance,balance);assert.equal(r.engine.locked,2100n*UNIT);assert.equal(r.resumed.priceMissingResumed,1);
    assert.equal(r.engine.intents.get('shadow-close-'+p.positionId).state,'queued');
  }finally{if(o&&!o.history.closed)o.stop('UNIT_END',o.session.now);fs.rmSync(parent,{recursive:true,force:true});}
});

test('explicit one-shot re-arm: waits for the source guard day reset, applies once, never re-arms a later trip',()=>{const db=new DatabaseSync(':memory:');try{
  const DAY=86400000,m=suite(db).models.find(v=>v.id==='y2_timing_observe'),api=m.bridge.api;m.engine.integrityWarmUntil=START;
  m.advance(START+200);const D0=api.signalState().guard.dayKey;assert.ok(D0>0);
  api.restoreFlatPolicy({cfg:{...api.cfg,live:false},guard:{...api.signalState().guard,tripped:true,dayKey:D0}});
  m.advance(START+400);assert.equal(m.status,'SOURCE_LIVE_OFF');assert.equal(m.sourceOff.dayKey,D0);
  m.save(START+400);db.prepare('INSERT INTO sim_source_rearm VALUES(?,?,?,?,NULL)').run(m.id,D0,START+500,'test: user re-arm');
  const r=suite(db,START+600).models.find(v=>v.id===m.id),a=r.bridge.api;r.engine.integrityWarmUntil=START+600;
  r.advance(START+800);assert.equal(r.status,'SOURCE_LIVE_OFF','same day: source guard still tripped');assert.equal(a.cfg.live,false);
  r.advance(START+DAY);assert.equal(a.cfg.live,true);assert.equal(a.signalState().guard.tripped,false);
  assert.equal(r.engine.ledger.filter(x=>x.kind==='source_live_rearmed').length,1);assert.ok(db.prepare('SELECT applied_ms FROM sim_source_rearm WHERE model_id=?').get(m.id).applied_ms>0);
  r.advance(START+DAY+200);assert.equal(r.status,'RUNNING');assert.ok(r.engine.ledger.some(x=>x.kind==='source_live_enabled'));
  const D1=a.signalState().guard.dayKey;a.restoreFlatPolicy({cfg:{...a.cfg,live:false},guard:{...a.signalState().guard,tripped:true,dayKey:D1}});
  r.advance(START+DAY+400);assert.equal(r.status,'SOURCE_LIVE_OFF');r.advance(START+2*DAY);assert.equal(a.cfg.live,false,'consumed request never re-arms a later trip');
  assert.equal(r.engine.ledger.filter(x=>x.kind==='source_live_rearmed').length,1);
}finally{db.close();}});
