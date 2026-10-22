'use strict';
// Synthetic regression for non-stop SIM comparison models. NOT performance evidence.
const test=require('node:test'),assert=require('node:assert/strict');
const {DatabaseSync}=require('node:sqlite');
const {StrategySuite}=require('../../apps/papertrade-shadow/suite.cjs');
const {buildProfile}=require('../../apps/papertrade-shadow/persistent-profile.cjs');
const {censor}=require('../../apps/papertrade-shadow/engine-state.cjs');
const START=Date.parse('2026-10-20T08:20:00Z');
let sequence=0;
const allow={status:'ALLOW_SHADOW',reason_codes:[]};
function suite(db,now=START){return new StrategySuite({history:{db,transaction:fn=>fn()},base:buildProfile(),guard:()=>allow,storageOk:()=>true,now});}
function market(symbol,venue,at,bid,ask){const payload={bid:String(bid),ask:String(ask),frameKind:'quote',instrumentId:symbol==='BTC'?0:1,priceScale:symbol==='BTC'?'10':'100',sourceTimestampKnown:true};return{schemaVersion:1,sequence:++sequence,kind:'market',symbol,venue,recv_ms:at,source_ms:at,payload,payload_sha256:require('node:crypto').createHash('sha256').update(JSON.stringify(payload)).digest('hex'),provenance:'synthetic'};}
function quote(m,at,px=81000){for(const v of['hl','bf'])m.bridge.market(market('BTC',v,at,px,px+.1));}
const q={strategy:'book',side:'L',asset:'BTC',margin:2100,ctx:{sig:.8,dev:0,spread:0,baseline:0,books:[]}};
// Control model without execution preset: before this change it was quarantined/frozen.
function opened(db){
  const s=suite(db),m=s.models.find(v=>v.id==='main_btc_observe'),e=m.engine;
  Object.assign(e.scenario,{entryMs:[100],exitMs:[100],cancelMs:[10],congestion:0,volatility:0,cancelConflictProbability:0});
  e.integrityWarmUntil=START;e.inputHealthy=true;quote(m,START);
  assert.ok(e.submit({...q}));m.advance(START+200,false);assert.equal(e.positions.size,1);
  return {s,m,e,p:[...e.positions.values()][0]};
}
test('close without execution-time quote waits with the slot held, then fills at the first later fresh quote, never at a past time',()=>{const db=new DatabaseSync(':memory:');try{
  const {m,e,p}=opened(db);assert.equal(m.continuous,true);
  m.advance(START+30000,false);                       // quote now stale
  e.close({positionId:p.positionId,closing:{why:'test close'}});
  m.advance(START+40000,false);
  const r=e.intents.get('shadow-close-'+p.positionId);
  assert.equal(r.state,'queued');assert.equal(r.executionPriceMissing,undefined);
  assert.equal(e.ledger.filter(x=>x.kind==='execution_price_unavailable').length,1);assert.ok(e.ledger.some(x=>x.kind==='exit_price_wait'));
  assert.equal(e.positions.size,1);assert.equal(e.locked,2100n*10n**18n);assert.equal(m.status==='QUARANTINED',false);
  quote(m,START+40100,81010);m.advance(START+40700,false);
  assert.equal(e.positions.size,0);assert.equal(e.locked,0n);assert.equal(e.counts.closed,1);assert.equal(r.state,'executed');
  const fill=e.ledger.find(x=>x.kind==='close_fill');assert.equal(fill.exit_raw,String(Math.round((81010+81010.1)/2*10)),'later quote, not the stale one');assert.ok(fill.closedAt>=START+40100,'fill time is not before the quote');
  assert.equal(fill.exitPriceWait.firstMissAtMs,START+30100);assert.equal(fill.exitPriceWait.waitMs,fill.closedAt-(START+30100));
  assert.ok(e.ledger.some(x=>x.kind==='exit_filled_after_price_wait'));
}finally{db.close();}});
test('model exception is recorded, pauses new entries 10 s and never freezes the account',()=>{const db=new DatabaseSync(':memory:');try{
  const s=suite(db),m=s.models.find(v=>v.id==='main_btc_observe');m.engine.integrityWarmUntil=START;m.engine.inputHealthy=true;
  const original=m.bridge.market;m.bridge.market=()=>{throw new Error('synthetic failure');};
  s.ingest(market('BTC','hl',START+1000,81000,81000.1),START+1000,true);m.bridge.market=original;
  assert.notEqual(m.status,'QUARANTINED');assert.equal(m.error,null);assert.equal(m.errors,1);assert.ok(!m.engine.accountingQuarantined);
  assert.equal(m.entryGate().reason,'MODEL_ERROR_COOLDOWN');
  m.advance(START+12000,false);assert.notEqual(m.entryGate().reason,'MODEL_ERROR_COOLDOWN');
}finally{db.close();}});
test('previously censored close without a downtime quote is requeued after restart and fills at the first fresh quote',()=>{const db=new DatabaseSync(':memory:');try{
  const {m,e,p}=opened(db);e.close({positionId:p.positionId,closing:{why:'before freeze'}});
  censor(e,'INPUT_GAP_WITH_EXPOSURE');assert.equal(e.timers.length,0);m.save(START+300);
  const s2=suite(db,START+5000),r=s2.models.find(v=>v.id==='main_btc_observe'),e2=r.engine;
  assert.equal(!!r.frozen,false);assert.notEqual(r.status,'QUARANTINED');assert.ok(!e2.accountingQuarantined);
  assert.equal(r.resumed.exitsRequeued,1);assert.equal(r.resumed.positions,1);assert.equal(e2.locked,2100n*10n**18n);
  assert.ok(e2.ledger.some(x=>x.kind==='nonstop_resume'));
  const stored=db.prepare("SELECT count(*) AS n FROM sim_strategy_events WHERE model_id=? AND kind='nonstop_resume'").get('main_btc_observe');assert.equal(stored.n,1,'nonstop_resume persisted to sim_strategy_events');
  for(const v of['hl','bf'])r.ingest(market('BTC',v,START+5100,81020,81020.1),START+5100,true);r.advance(START+5600,false); // real path: ingest marks continuous input healthy
  assert.equal(e2.positions.size,0);assert.equal(e2.locked,0n);assert.equal(e2.counts.closed,1);
  assert.equal([...e2.intents.values()].some(x=>x.state==='outcome_unknown'),false);assert.ok(e2.ledger.some(x=>x.kind==='exit_filled_after_price_wait'));
}finally{db.close();}});
test('restart with open exposure persists restart_with_exposure_continued to sim_strategy_events',()=>{const db=new DatabaseSync(':memory:');try{
  const {m}=opened(db);m.save(START+300);
  const s2=suite(db,START+5000),r=s2.models.find(v=>v.id==='main_btc_observe');
  assert.notEqual(r.status,'QUARANTINED');assert.equal(r.engine.positions.size,1);
  const rows=db.prepare("SELECT kind,sequence FROM sim_strategy_events WHERE model_id=? AND kind IN ('restart_with_exposure_continued','nonstop_resume')").all('main_btc_observe');
  assert.deepEqual(rows.map(x=>x.kind),['restart_with_exposure_continued']);
  const seqs=db.prepare('SELECT sequence FROM sim_strategy_events WHERE model_id=? ORDER BY sequence').all('main_btc_observe').map(x=>x.sequence);
  assert.equal(new Set(seqs).size,seqs.length,'no duplicate sequences');
}finally{db.close();}});
// --- 22:5x additions: raw store survives quota pressure, archived models do not pin raw context, comparison models top up virtually ---
const fs=require('node:fs'),path=require('node:path');
const {History}=require('../../apps/papertrade-shadow/history.cjs');
const {RawRetention}=require('../../apps/papertrade-shadow/raw-retention.cjs');
const {ROOT}=require('../../apps/papertrade-shadow/strategy.cjs');
function tempState(){const parent=fs.mkdtempSync(path.join(ROOT,'reports/nonstop-unit-'));return {parent,dir:path.join(parent,'state')};}
test('comparison model below the fixed margin reserve tops up virtually, keeps trading and records the event; realised PnL unchanged',()=>{const db=new DatabaseSync(':memory:');try{
  const s=suite(db),m=s.models.find(v=>v.id==='main_btc_observe'),e=m.engine;e.integrityWarmUntil=START;e.inputHealthy=true;
  e.balance=2150n*10n**18n;e.realised=-150n*10n**18n;
  const gate=m.entryGate();assert.notEqual(gate.reason,'FIXED_MARGIN_RESERVE');
  assert.equal(e.balance,2300n*10n**18n);assert.equal(e.realised,-150n*10n**18n);assert.equal(m.topUps,1);
  m.save(START+1);const row=db.prepare("SELECT payload_json FROM sim_strategy_events WHERE model_id=? AND kind='sim_virtual_topup'").get('main_btc_observe');assert.ok(row);
  assert.equal(m.summary().virtualTopUps,1);assert.equal(m.summary().virtualTopUpUsdc,'150.000000000000000000');
}finally{db.close();}});
test('raw store: restored snapshot does not inherit disabled; quota pressure keeps the store alive; protection expires after 2x retention',()=>{
  const x=tempState();let h;try{
    h=new History(x.dir,{now:START});h.begin(buildProfile(),START);
    const policy={...buildProfile().rawPolicy,quotaBytes:1},warnings=[];let recovered=0;
    const raw=new RawRetention(h,policy,{onWarning:w=>warnings.push(w),onRecovered:()=>recovered++});
    raw.restore({...raw.checkpoint(),disabled:true,warning:'RAW_IO'});assert.equal(raw.disabled,false);assert.equal(raw.warning,null);
    raw.observe(market('BTC','hl',START,81000,81000.1),START,'boot');raw.trigger('unit',START,{force:true});raw.flush(START);
    assert.equal(raw.disabled,false);assert.ok(warnings.includes('RAW_QUOTA_PROTECTED_EVIDENCE'));
    raw.policy.quotaBytes=10*1024*1024;raw.observe(market('BTC','hl',START+1000,81001,81001.1),START+1000,'boot');raw.trigger('unit2',START+1000,{force:true});raw.flush(START+1000);
    assert.equal(raw.warning,null);assert.equal(recovered,1);
    h.db.prepare('UPDATE raw_segments SET protected=1').run();
    raw.prune(START+1000+2*policy.retentionMs+1);assert.equal(h.db.prepare('SELECT COUNT(*) n FROM raw_segments WHERE deleted_ms IS NULL AND protected=1').get().n,0);
  }finally{h?.close();fs.rmSync(x.parent,{recursive:true,force:true});}
});
test('shared risk ignores archived comparison models with unresolved accounts',()=>{
  const x=tempState();let h;try{
    h=new History(x.dir,{now:START});h.begin(buildProfile(),START);
    new StrategySuite({history:{db:h.db,transaction:fn=>fn()},base:buildProfile(),guard:()=>allow,storageOk:()=>true,now:START});
    assert.equal(h.hasRisk(),false);
    h.db.prepare("UPDATE sim_strategy_accounts SET unresolved=1 WHERE model_id='main_btc_observe'").run();assert.equal(h.hasRisk(),true);
    h.db.prepare("INSERT OR IGNORE INTO sim_strategy_archived(model_id,archived_ms,reason) VALUES('main_btc_observe',?,'unit')").run(START);assert.equal(h.hasRisk(),false);
    const {canFreezeUnresolvedModels}=require('../../apps/papertrade-shadow/suite.cjs');if(canFreezeUnresolvedModels)assert.equal(canFreezeUnresolvedModels(h.db),true,'archived unresolved account must not block a flat upgrade');
  }finally{h?.close();fs.rmSync(x.parent,{recursive:true,force:true});}
});
