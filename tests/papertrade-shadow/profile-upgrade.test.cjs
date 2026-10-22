'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {DatabaseSync}=require('node:sqlite');
const {PersistentObserver}=require('../../apps/papertrade-shadow/persistent-session.cjs');
const C=require('../../apps/papertrade-shadow/state-codec.cjs');
const ROOT=path.resolve(__dirname,'../..'),T=Date.parse('2026-10-20T08:20:00Z'),UNIT=10n**18n;
test('explicit clean flat profile upgrade preserves account, cash, loss ledger and original run',()=>{
 const parent=fs.mkdtempSync(path.join(ROOT,'reports/upgrade-test-')),dir=path.join(parent,'state');let o;
 try{o=new PersistentObserver({stateDir:dir,clock:()=>T});o.session.engine.balance=2250n*UNIT;o.session.engine.realised=-50n*UNIT;o.session.engine.note('UNIT_SYNTHETIC_LOSS',{});const id=o.runId;o.stop('UNIT_OLD_CLEAN_STOP',T);
 const db=new DatabaseSync(path.join(dir,'shadow-history.sqlite'));for(const row of db.prepare('SELECT slot,payload_json FROM sim_checkpoints').all()){const state=C.parse(row.payload_json);state.modelHash='previous-reviewed-model';const blob=C.stringify(state);db.prepare('UPDATE sim_checkpoints SET payload_json=?,sha256=?,model_hash=? WHERE slot=?').run(blob,C.hash(blob),'previous-reviewed-model',row.slot);}const events=db.prepare('SELECT COUNT(*) n FROM sim_events').get().n;db.close();
 o=new PersistentObserver({stateDir:dir,clock:()=>T+2000,flatUpgrade:true});assert.equal(o.runId,id);assert.equal(o.session.engine.balance,2250n*UNIT);assert.equal(o.session.engine.realised,-50n*UNIT);assert.equal(o.flatUpgradeApplied,true);assert.equal(o.recovery.requiresExplicitRecovery,false);assert.equal(o.session.startMs,T);assert.ok(o.session.engine.integrityWarmUntil>=T+182000);assert.ok(o.history.totals().sim_events>events);assert.equal(o.history.db.prepare("SELECT COUNT(*) n FROM quality_events WHERE kind='EXPLICIT_FLAT_MODEL_UPGRADE'").get().n,1);
 }finally{if(o&&!o.history.closed)o.stop('UNIT_END',o.session.now);fs.rmSync(parent,{recursive:true,force:true});}
});
test('legacy price upgrade preserves an open comparison position, native journal and cash; new series excludes old entry prices',()=>{
 const parent=fs.mkdtempSync(path.join(ROOT,'reports/upgrade-price-test-')),dir=path.join(parent,'state');let o;
 try{
  o=new PersistentObserver({stateDir:dir,clock:()=>T,comparisonEnabled:true});
  const m=o.suite.models.find(m=>m.id==='main_btc_observe'),e=m.engine;e.inputHealthy=true;e.integrityWarmUntil=T;e.entryGate=()=>({allowed:true});
  const id=e.submit({strategy:'spread',side:'L',margin:2100,ctx:{sig:9,dev:-9,spread:0,baseline:9,books:[]}});assert.ok(id);
  assert.ok(e.observeEntry(id,{executedAtMs:T,observedAtMs:T,executionBlockSeconds:Math.floor(T/1000),priceRaw:831695n}));
  for(const model of o.suite.models){delete model.engine.priceEpoch;model.save(T);}
  const runId=o.runId,balance=e.balance,realised=e.realised,positionId=[...e.positions.keys()][0];o.stop('UNIT_LEGACY_CLEAN_STOP',T);
  const db=new DatabaseSync(path.join(dir,'shadow-history.sqlite')),{canPreserveLegacyPriceAccounts}=require('../../apps/papertrade-shadow/suite.cjs');
  for(const account of db.prepare('SELECT model_id,snapshot_json FROM sim_strategy_accounts').all()){const s=C.parse(account.snapshot_json);delete s.native.positions;const blob=C.stringify(s);db.prepare('UPDATE sim_strategy_accounts SET snapshot_json=?,snapshot_hash=? WHERE model_id=?').run(blob,C.hash(blob),account.model_id);}
  assert.equal(canPreserveLegacyPriceAccounts(db),true);
  const prior=db.prepare('SELECT * FROM sim_strategy_accounts WHERE model_id=?').get(m.id);
  db.prepare('UPDATE sim_strategy_accounts SET snapshot_hash=? WHERE model_id=?').run('bad',m.id);assert.equal(canPreserveLegacyPriceAccounts(db),false);
  db.prepare('UPDATE sim_strategy_accounts SET snapshot_hash=? WHERE model_id=?').run(prior.snapshot_hash,m.id);
  const corrupt=C.parse(prior.snapshot_json);corrupt.engine.locked=0n;const bad=C.stringify(corrupt);
  db.prepare('UPDATE sim_strategy_accounts SET snapshot_json=?,snapshot_hash=? WHERE model_id=?').run(bad,C.hash(bad),m.id);assert.equal(canPreserveLegacyPriceAccounts(db),false);
  db.prepare('UPDATE sim_strategy_accounts SET snapshot_json=?,snapshot_hash=? WHERE model_id=?').run(prior.snapshot_json,prior.snapshot_hash,m.id);
  for(const row of db.prepare('SELECT slot,payload_json FROM sim_checkpoints').all()){const state=C.parse(row.payload_json);state.modelHash='legacy-price-model';const blob=C.stringify(state);db.prepare('UPDATE sim_checkpoints SET payload_json=?,sha256=?,model_hash=? WHERE slot=?').run(blob,C.hash(blob),'legacy-price-model',row.slot);}db.close();
  o=new PersistentObserver({stateDir:dir,clock:()=>T+2000,flatUpgrade:true,comparisonEnabled:true});const r=o.suite.models.find(m=>m.id==='main_btc_observe');
  assert.equal(o.runId,runId);assert.ok(o.flatUpgradeApplied);assert.equal(r.engine.balance,balance);assert.equal(r.engine.realised,realised);assert.equal(r.engine.locked,2100n*UNIT);assert.ok(r.engine.positions.has(positionId));assert.equal(r.bridge.api.positions.size,1);
  assert.equal([...r.bridge.api.positions.values()][0].legacyStateRestored,true,'old snapshot reconstructed from exact native position ID');
  const managed=[...r.bridge.api.positions.values()][0];managed.maxFav=7.25;managed.bBest=83200;r.save(T+2100);
  const {StrategySuite}=require('../../apps/papertrade-shadow/suite.cjs'),again=new StrategySuite({history:o.history,base:o.profile,guard:()=>({status:'ALLOW_SHADOW',reason_codes:[]}),storageOk:()=>true,now:T+2200});
  const restored=[...again.models.find(m=>m.id===r.id).bridge.api.positions.values()][0];assert.equal(restored.maxFav,7.25);assert.equal(restored.bBest,83200);assert.equal(o.history.db.prepare('SELECT count(*) n FROM "'+r.bridge.tableName+'" WHERE status=\'open\'').get().n,1);
  assert.equal(r.summary().priceEpoch.netUsdc,'0.000000000000000000');assert.equal(r.summary().priceEpoch.counts.closed,0);assert.equal(r.summary().priceEpoch.counts.filled,0);
  assert.equal(canPreserveLegacyPriceAccounts(o.history.db),false,'one-time migration cannot be reused for a new price epoch');
 }finally{if(o&&!o.history.closed)o.stop('UNIT_END',o.session.now);fs.rmSync(parent,{recursive:true,force:true});}
});
